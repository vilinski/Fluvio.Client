using System.Collections.Concurrent;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading.Channels;
using Fluvio.Client.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;

namespace Fluvio.Client.Network;

/// <summary>
/// Manages TCP connection to Fluvio SPU or SC
/// </summary>
internal sealed class FluvioConnection(
    string host,
    int port,
    bool useTls,
    TimeSpan timeout,
    ILogger<FluvioConnection>? logger = null,
    IAsyncPolicy? resiliencePolicy = null)
    : IAsyncDisposable
{
    private readonly ILogger<FluvioConnection> _logger = logger ?? NullLoggerFactory.Instance.CreateLogger<FluvioConnection>();

    private TcpClient? _tcpClient;
    private Stream? _stream;
    private int _nextCorrelationId;
    private readonly ConcurrentDictionary<int, TaskCompletionSource<ReadOnlyMemory<byte>>> _pendingRequests = new();
    private readonly ConcurrentDictionary<int, Channel<ReadOnlyMemory<byte>>> _streamingRequests = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private Task? _readLoopTask;
    private CancellationTokenSource? _readLoopCts;

    public bool IsConnected => _tcpClient?.Connected ?? false;

    public DateTime? LastSuccessfulRequest { get; private set; }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Connecting to Fluvio at {Host}:{Port} (TLS: {UseTls})", host, port, useTls);

        async Task ConnectCore()
        {
            _tcpClient = new TcpClient();

            using var timeoutCts = new CancellationTokenSource(timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            await _tcpClient.ConnectAsync(host, port, linkedCts.Token);
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

            _logger.LogInformation("Successfully connected to {Host}:{Port}", host, port);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to {Host}:{Port}", host, port);
            throw;
        }
    }

    public async Task<ReadOnlyMemory<byte>> SendRequestAsync(ApiKey apiKey, short apiVersion, string? clientId, ReadOnlyMemory<byte> requestBody, CancellationToken cancellationToken = default)
    {
        if (_stream == null || _tcpClient == null || !_tcpClient.Connected)
        {
            var error = "Connection not established";
            _logger.LogError(error);
            throw new InvalidOperationException(error);
        }

        var correlationId = Interlocked.Increment(ref _nextCorrelationId);
        _logger.LogDebug("Sending request: API={ApiKey} Version={ApiVersion} CorrelationId={CorrelationId} Size={Size}",
            apiKey, apiVersion, correlationId, requestBody.Length);

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

            // Wait for response
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            await using (linkedCts.Token.Register(() => tcs.TrySetCanceled()))
            {
                var response = await tcs.Task;
                LastSuccessfulRequest = DateTime.UtcNow;
                _logger.LogDebug("Received response: CorrelationId={CorrelationId} Size={Size}",
                    correlationId, response.Length);
                return response;
            }
        }
        catch (Exception ex)
        {
            _pendingRequests.TryRemove(correlationId, out _);
            _logger.LogError(ex, "Request failed: API={ApiKey} CorrelationId={CorrelationId}",
                apiKey, correlationId);
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
            var error = "Connection not established";
            _logger.LogError(error);
            throw new InvalidOperationException(error);
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
                // Read size prefix
                var totalRead = 0;
                while (totalRead < 4)
                {
                    var read = await _stream.ReadAsync(sizeBuffer.AsMemory(totalRead, 4 - totalRead), cancellationToken);
                    if (read == 0)
                    {
                        // Connection closed
                        _logger.LogInformation("Connection closed by remote host");
                        return;
                    }
                    totalRead += read;
                }

                var responseSize = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
                if (responseSize <= 0 || responseSize > 100 * 1024 * 1024) // 100MB max
                {
                    var error = $"Invalid response size: {responseSize}";
                    _logger.LogError(error);
                    throw new InvalidDataException(error);
                }

                _logger.LogTrace("Reading response: Size={Size}", responseSize);

                // Read response body
                var responseBuffer = new byte[responseSize];
                totalRead = 0;
                while (totalRead < responseSize)
                {
                    var read = await _stream.ReadAsync(responseBuffer.AsMemory(totalRead, responseSize - totalRead), cancellationToken);
                    if (read == 0)
                    {
                        var error = "Connection closed while reading response";
                        _logger.LogError(error);
                        throw new EndOfStreamException(error);
                    }
                    totalRead += read;
                }

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
        }
        finally
        {
            _logger.LogDebug("Read loop exited");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _logger.LogDebug("Disposing connection to {Host}:{Port}", host, port);

        if (_readLoopCts != null)
        {
            _readLoopCts.Cancel();
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

        _logger.LogInformation("Connection disposed: {Host}:{Port}", host, port);
    }
}
