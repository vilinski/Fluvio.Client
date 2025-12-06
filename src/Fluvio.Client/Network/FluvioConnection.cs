using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading.Channels;
using Fluvio.Client.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;

namespace Fluvio.Client.Network;

/// <summary>
/// Connection state for automatic reconnection
/// </summary>
internal enum ConnectionState
{
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Failed
}

/// <summary>
/// Manages TCP connection to Fluvio SPU or SC with automatic reconnection support
/// </summary>
internal sealed class FluvioConnection(
    string host,
    int port,
    bool useTls,
    TimeSpan timeout,
    TimeProvider timeProvider,
    ILogger<FluvioConnection>? logger = null,
    IAsyncPolicy? resiliencePolicy = null,
    bool enableAutoReconnect = true,
    int maxReconnectAttempts = 5,
    TimeSpan reconnectDelay = default)
    : IAsyncDisposable
{
    private readonly ILogger<FluvioConnection> _logger = logger ?? NullLoggerFactory.Instance.CreateLogger<FluvioConnection>();
    private readonly TimeSpan _reconnectDelay = reconnectDelay == default ? TimeSpan.FromSeconds(2) : reconnectDelay;
    private readonly TimeProvider _timeProvider = timeProvider;

    private TcpClient? _tcpClient;
    private Stream? _stream;
    private int _nextCorrelationId;
    private readonly ConcurrentDictionary<int, TaskCompletionSource<ReadOnlyMemory<byte>>> _pendingRequests = new();
    private readonly ConcurrentDictionary<int, Channel<ReadOnlyMemory<byte>>> _streamingRequests = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly SemaphoreSlim _reconnectLock = new(1, 1);
    private Task? _readLoopTask;
    private CancellationTokenSource? _readLoopCts;
    private int _state = (int)ConnectionState.Disconnected;  // Use int for Interlocked operations
    private int _reconnectAttempts;

    public bool IsConnected => (ConnectionState)Interlocked.CompareExchange(ref _state, 0, 0) == ConnectionState.Connected
                               && _tcpClient?.Connected == true;

    public DateTimeOffset? LastSuccessfulRequest { get; private set; }

    public TimeProvider TimeProvider => _timeProvider;

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        // Try to transition from Disconnected to Connecting
        var previousState = (ConnectionState)Interlocked.CompareExchange(
            ref _state,
            (int)ConnectionState.Connecting,
            (int)ConnectionState.Disconnected);

        if (previousState == ConnectionState.Connecting || previousState == ConnectionState.Connected)
        {
            _logger.LogDebug("Already connected or connecting (state: {State})", previousState);
            return;
        }

        // If we're in Failed state, allow reconnection attempt
        if (previousState == ConnectionState.Failed)
        {
            Interlocked.Exchange(ref _state, (int)ConnectionState.Connecting);
        }

        _logger.LogInformation("Connecting to Fluvio at {Host}:{Port} (TLS: {UseTls})", host, port, useTls);

        async Task ConnectCore()
        {
            _tcpClient = new TcpClient();

            await _tcpClient.ConnectAsync(host, port, cancellationToken).AsTask().WaitAsync(timeout);
            _logger.LogDebug("TCP connection established to {Host}:{Port}", host, port);

            if (useTls)
            {
                _logger.LogDebug("Establishing TLS connection");
                var sslStream = new SslStream(_tcpClient.GetStream(), false);
                await sslStream.AuthenticateAsClientAsync(host, null, System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13, true);
                _stream = sslStream;
                _logger.LogDebug("TLS connection established");
            }
            else
            {
                _stream = _tcpClient.GetStream();
            }

            // Start background read loop
            _readLoopCts = new CancellationTokenSource();
            _readLoopTask = Task.Run(() => ReadLoopAsync(_readLoopCts.Token), _readLoopCts.Token);
        }

        try
        {
            if (resiliencePolicy != null)
            {
                await resiliencePolicy.ExecuteAsync(ConnectCore);
            }
            else
            {
                await ConnectCore();
            }

            Interlocked.Exchange(ref _state, (int)ConnectionState.Connected);
            Interlocked.Exchange(ref _reconnectAttempts, 0);

            _logger.LogInformation("Successfully connected to {Host}:{Port}", host, port);
        }
        catch (Exception ex)
        {
            Interlocked.Exchange(ref _state, (int)ConnectionState.Failed);
            _logger.LogError(ex, "Failed to connect to {Host}:{Port}", host, port);
            throw;
        }
    }

    public async Task<ReadOnlyMemory<byte>> SendRequestAsync(ApiKey apiKey, short apiVersion, string? clientId, ReadOnlyMemory<byte> requestBody, CancellationToken cancellationToken = default)
    {
        if (_stream == null || _tcpClient == null || !_tcpClient.Connected)
        {
            _logger.LogError("Connection not established");
            throw new InvalidOperationException("Connection not established");
        }

        var correlationId = Interlocked.Increment(ref _nextCorrelationId);

        // Create activity for distributed tracing
        using var activity = Telemetry.FluvioActivitySource.Instance.StartActivity(
            Telemetry.FluvioActivitySource.Operations.Request,
            ActivityKind.Client);

        activity?.SetTag(Telemetry.FluvioActivitySource.Tags.CorrelationId, correlationId);
        activity?.SetTag(Telemetry.FluvioActivitySource.Tags.ApiKey, apiKey);
        activity?.SetTag(Telemetry.FluvioActivitySource.Tags.Endpoint, $"{host}:{port}");
        activity?.SetTag("request.size", requestBody.Length);

        _logger.LogDebug("Sending request: API={ApiKey} Version={ApiVersion} CorrelationId={CorrelationId} Size={Size} TraceId={TraceId}",
            apiKey, apiVersion, correlationId, requestBody.Length, activity?.TraceId);

        var tcs = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[correlationId] = tcs;

        try
        {
            // Build request with header
            using var writer = new FluvioBinaryWriter();
            var header = new RequestHeader
            {
                ApiKey = apiKey,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId
            };

            header.WriteTo(writer);
            writer._stream.Write(requestBody.Span); // Write body directly without length prefix

            var requestData = writer.ToArray();

            // Write size prefix (4 bytes) + request
            await _writeLock.WaitAsync(cancellationToken);
            try
            {
                using var sizeWriter = new FluvioBinaryWriter();
                sizeWriter.WriteInt32(requestData.Length);
                var sizeBytes = sizeWriter.ToArray();

                await _stream.WriteAsync(sizeBytes, cancellationToken);
                await _stream.WriteAsync(requestData, cancellationToken);
                await _stream.FlushAsync(cancellationToken);
                _logger.LogTrace("Request sent: CorrelationId={CorrelationId} TotalSize={TotalSize}",
                    correlationId, sizeBytes.Length + requestData.Length);
            }
            finally
            {
                _writeLock.Release();
            }

            // Wait for response with timeout and cancellation support
            var response = await tcs.Task.WaitAsync(timeout, cancellationToken);
            LastSuccessfulRequest = _timeProvider.GetUtcNow();

            // Mark activity as successful
            activity?.SetTag("response.size", response.Length);
            activity?.SetStatus(ActivityStatusCode.Ok);

            _logger.LogDebug("Received response: CorrelationId={CorrelationId} Size={Size} TraceId={TraceId}",
                correlationId, response.Length, activity?.TraceId);
            return response;
        }
        catch (Exception ex)
        {
            _pendingRequests.TryRemove(correlationId, out _);

            // Mark activity as failed
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

            _logger.LogError(ex, "Request failed: API={ApiKey} CorrelationId={CorrelationId} TraceId={TraceId}",
                apiKey, correlationId, activity?.TraceId);
            throw;
        }
    }

    /// <summary>
    /// Sends a streaming request (like StreamFetch) that returns multiple responses with the same correlation ID.
    /// Returns a Channel that will receive all streaming responses until the stream ends or is cancelled.
    /// </summary>
    public async Task<ChannelReader<ReadOnlyMemory<byte>>> SendStreamingRequestAsync(
        ApiKey apiKey,
        short apiVersion,
        string? clientId,
        ReadOnlyMemory<byte> requestBody,
        CancellationToken cancellationToken = default)
    {
        if (_stream == null || _tcpClient == null || !_tcpClient.Connected)
        {
            _logger.LogError("Connection not established for streaming request");
            throw new InvalidOperationException("Connection not established");
        }

        var correlationId = Interlocked.Increment(ref _nextCorrelationId);
        _logger.LogInformation("Sending streaming request: API={ApiKey} Version={ApiVersion} CorrelationId={CorrelationId}",
            apiKey, apiVersion, correlationId);

        // Create bounded channel for backpressure (capacity 100, matching Rust implementation)
        var channel = Channel.CreateBounded<ReadOnlyMemory<byte>>(new BoundedChannelOptions(100)
        {
            FullMode = BoundedChannelFullMode.Wait
        });

        _streamingRequests[correlationId] = channel;

        try
        {
            // Build request with header
            using var writer = new FluvioBinaryWriter();
            var header = new RequestHeader
            {
                ApiKey = apiKey,
                ApiVersion = apiVersion,
                CorrelationId = correlationId,
                ClientId = clientId
            };

            header.WriteTo(writer);
            writer._stream.Write(requestBody.Span);

            var requestData = writer.ToArray();

            // Write size prefix (4 bytes) + request
            await _writeLock.WaitAsync(cancellationToken);
            try
            {
                using var sizeWriter = new FluvioBinaryWriter();
                sizeWriter.WriteInt32(requestData.Length);
                var sizeBytes = sizeWriter.ToArray();

                await _stream.WriteAsync(sizeBytes, cancellationToken);
                await _stream.WriteAsync(requestData, cancellationToken);
                await _stream.FlushAsync(cancellationToken);
                _logger.LogDebug("Streaming request sent: CorrelationId={CorrelationId}", correlationId);
            }
            finally
            {
                _writeLock.Release();
            }

            return channel.Reader;
        }
        catch (Exception ex)
        {
            _streamingRequests.TryRemove(correlationId, out _);
            channel.Writer.Complete(ex);
            _logger.LogError(ex, "Streaming request failed: API={ApiKey} CorrelationId={CorrelationId}",
                apiKey, correlationId);
            throw;
        }
    }

    /// <summary>
    /// Cancels a streaming request and closes its channel.
    /// </summary>
    public void CancelStreamingRequest(int correlationId)
    {
        if (_streamingRequests.TryRemove(correlationId, out var channel))
        {
            channel.Writer.Complete();
            _logger.LogDebug("Streaming request cancelled: CorrelationId={CorrelationId}", correlationId);
        }
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        var sizeBuffer = new byte[4];
        _logger.LogDebug("Read loop started");

        try
        {
            while (!cancellationToken.IsCancellationRequested && _stream != null)
            {
                // Read size prefix (4 bytes) - ReadExactlyAsync throws EndOfStreamException on EOF
                try
                {
                    await _stream.ReadExactlyAsync(sizeBuffer, cancellationToken);
                }
                catch (EndOfStreamException)
                {
                    // Connection closed cleanly
                    _logger.LogInformation("Connection closed by remote host");
                    return;
                }

                var responseSize = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
                if (responseSize <= 0 || responseSize > 100 * 1024 * 1024) // 100MB max
                {
                    _logger.LogError("Invalid response size: {ResponseSize}", responseSize);
                    throw new InvalidDataException($"Invalid response size: {responseSize}");
                }

                _logger.LogTrace("Reading response: Size={Size}", responseSize);

                // Read response body - ReadExactlyAsync guarantees full read or throws
                var responseBuffer = new byte[responseSize];
                await _stream.ReadExactlyAsync(responseBuffer, cancellationToken);

                // Parse correlation ID from response
                using var reader = new FluvioBinaryReader(responseBuffer);
                var header = ResponseHeader.ReadFrom(reader);

                // Get remaining bytes as response body
                var bodySize = (int)(reader.Length - reader.Position);
                var bodyBuffer = new byte[bodySize];
                Array.Copy(responseBuffer, (int)reader.Position, bodyBuffer, 0, bodySize);

                // Check if this is a streaming request or regular request
                if (_streamingRequests.TryGetValue(header.CorrelationId, out var streamChannel))
                {
                    // Streaming request - write to channel (may block if consumer is slow - backpressure)
                    try
                    {
                        await streamChannel.Writer.WriteAsync(bodyBuffer, cancellationToken);
                        _logger.LogTrace("Streaming response delivered: CorrelationId={CorrelationId} Size={Size}",
                            header.CorrelationId, bodyBuffer.Length);
                    }
                    catch (ChannelClosedException)
                    {
                        // Consumer cancelled - remove from tracking
                        _streamingRequests.TryRemove(header.CorrelationId, out _);
                        _logger.LogDebug("Streaming channel closed: CorrelationId={CorrelationId}", header.CorrelationId);
                    }
                }
                else if (_pendingRequests.TryRemove(header.CorrelationId, out var tcs))
                {
                    // Regular request - complete the Task
                    _logger.LogTrace("Completing request: CorrelationId={CorrelationId}", header.CorrelationId);
                    tcs.TrySetResult(bodyBuffer);
                }
                else
                {
                    _logger.LogWarning("Received response for unknown CorrelationId={CorrelationId}", header.CorrelationId);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Read loop terminated with error. Failing {PendingCount} pending requests and {StreamingCount} streaming requests",
                _pendingRequests.Count, _streamingRequests.Count);

            // Try to transition from Connected to Disconnected
            Interlocked.CompareExchange(
                ref _state,
                (int)ConnectionState.Disconnected,
                (int)ConnectionState.Connected);

            // Complete all pending requests with exception
            foreach (var kvp in _pendingRequests)
            {
                kvp.Value.TrySetException(ex);
            }
            _pendingRequests.Clear();

            // Complete all streaming channels with exception
            foreach (var kvp in _streamingRequests)
            {
                kvp.Value.Writer.Complete(ex);
            }
            _streamingRequests.Clear();

            // Trigger automatic reconnection if enabled
            if (enableAutoReconnect)
            {
                _ = Task.Run(async () => await TryReconnectAsync(CancellationToken.None));
            }
        }
        finally
        {
            _logger.LogDebug("Read loop exited");
        }
    }

    /// <summary>
    /// Attempts to reconnect automatically with exponential backoff
    /// </summary>
    private async Task TryReconnectAsync(CancellationToken cancellationToken)
    {
        await _reconnectLock.WaitAsync(cancellationToken);
        try
        {
            // Try to transition to Reconnecting state
            var currentState = (ConnectionState)Interlocked.CompareExchange(ref _state, 0, 0);
            if (currentState == ConnectionState.Connected || currentState == ConnectionState.Reconnecting)
            {
                _logger.LogDebug("Already connected or reconnecting, skipping reconnection attempt");
                return;
            }

            Interlocked.Exchange(ref _state, (int)ConnectionState.Reconnecting);

            while (true)
            {
                var attemptNumber = Interlocked.Increment(ref _reconnectAttempts);

                if (attemptNumber > maxReconnectAttempts)
                {
                    Interlocked.Exchange(ref _state, (int)ConnectionState.Failed);
                    _logger.LogError("Failed to reconnect after {MaxAttempts} attempts. Connection marked as failed",
                        maxReconnectAttempts);
                    return;
                }

                var delay = _reconnectDelay * Math.Pow(2, attemptNumber - 1); // Exponential backoff

                _logger.LogWarning("Attempting to reconnect (attempt {Attempt}/{MaxAttempts}) to {Host}:{Port} after {Delay}ms",
                    attemptNumber, maxReconnectAttempts, host, port, delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);

                try
                {
                    // Cleanup old connection
                    await CleanupConnectionAsync();

                    // Try to reconnect
                    await ConnectAsync(cancellationToken);

                    _logger.LogInformation("Successfully reconnected to {Host}:{Port} after {Attempts} attempts",
                        host, port, attemptNumber);
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Reconnection attempt {Attempt}/{MaxAttempts} failed",
                        attemptNumber, maxReconnectAttempts);
                }
            }
        }
        finally
        {
            _reconnectLock.Release();
        }
    }

    /// <summary>
    /// Cleans up the current connection without disposing the entire object
    /// </summary>
    private async Task CleanupConnectionAsync()
    {
        if (_readLoopCts != null)
        {
            await _readLoopCts.CancelAsync();
            if (_readLoopTask != null)
            {
                try
                {
                    await _readLoopTask;
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
            _readLoopCts.Dispose();
            _readLoopCts = null;
        }

        _stream?.Dispose();
        _stream = null;
        _tcpClient?.Dispose();
        _tcpClient = null;
    }

    public async ValueTask DisposeAsync()
    {
        _logger.LogDebug("Disposing connection to {Host}:{Port}", host, port);

        Interlocked.Exchange(ref _state, (int)ConnectionState.Disconnected);

        if (_readLoopCts != null)
        {
            await _readLoopCts.CancelAsync();
            if (_readLoopTask != null)
            {
                try
                {
                    await _readLoopTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }
            _readLoopCts.Dispose();
        }

        _stream?.Dispose();
        _tcpClient?.Dispose();
        _writeLock.Dispose();
        _reconnectLock.Dispose();

        _logger.LogInformation("Connection disposed: {Host}:{Port}", host, port);
    }
}
