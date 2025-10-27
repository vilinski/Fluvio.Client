using System.Collections.Concurrent;
using System.Net.Security;
using System.Net.Sockets;
using Fluvio.Client.Protocol;

namespace Fluvio.Client.Network;

/// <summary>
/// Manages TCP connection to Fluvio SPU or SC
/// </summary>
internal sealed class FluvioConnection : IAsyncDisposable
{
    private readonly string _host;
    private readonly int _port;
    private readonly bool _useTls;
    private readonly TimeSpan _timeout;

    private TcpClient? _tcpClient;
    private Stream? _stream;
    private int _nextCorrelationId;
    private readonly ConcurrentDictionary<int, TaskCompletionSource<ReadOnlyMemory<byte>>> _pendingRequests;
    private readonly SemaphoreSlim _writeLock;
    private Task? _readLoopTask;
    private CancellationTokenSource? _readLoopCts;

    public bool IsConnected => _tcpClient?.Connected ?? false;

    public FluvioConnection(string host, int port, bool useTls, TimeSpan timeout)
    {
        _host = host;
        _port = port;
        _useTls = useTls;
        _timeout = timeout;
        _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<ReadOnlyMemory<byte>>>();
        _writeLock = new SemaphoreSlim(1, 1);
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _tcpClient = new TcpClient();

        using var timeoutCts = new CancellationTokenSource(_timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        await _tcpClient.ConnectAsync(_host, _port, linkedCts.Token);

        if (_useTls)
        {
            var sslStream = new SslStream(_tcpClient.GetStream(), false);
            await sslStream.AuthenticateAsClientAsync(_host, null, System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13, true);
            _stream = sslStream;
        }
        else
        {
            _stream = _tcpClient.GetStream();
        }

        // Start background read loop
        _readLoopCts = new CancellationTokenSource();
        _readLoopTask = Task.Run(() => ReadLoopAsync(_readLoopCts.Token), _readLoopCts.Token);
    }

    public async Task<ReadOnlyMemory<byte>> SendRequestAsync(ApiKey apiKey, short apiVersion, string? clientId, ReadOnlyMemory<byte> requestBody, CancellationToken cancellationToken = default)
    {
        if (_stream == null || _tcpClient == null || !_tcpClient.Connected)
        {
            throw new InvalidOperationException("Connection not established");
        }

        var correlationId = Interlocked.Increment(ref _nextCorrelationId);
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
            writer.WriteBytes(requestBody.Span);

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
            }
            finally
            {
                _writeLock.Release();
            }

            // Wait for response
            using var timeoutCts = new CancellationTokenSource(_timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            using (linkedCts.Token.Register(() => tcs.TrySetCanceled()))
            {
                return await tcs.Task;
            }
        }
        catch
        {
            _pendingRequests.TryRemove(correlationId, out _);
            throw;
        }
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        var sizeBuffer = new byte[4];

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
                        return;
                    }
                    totalRead += read;
                }

                var responseSize = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
                if (responseSize <= 0 || responseSize > 100 * 1024 * 1024) // 100MB max
                {
                    throw new InvalidDataException($"Invalid response size: {responseSize}");
                }

                // Read response body
                var responseBuffer = new byte[responseSize];
                totalRead = 0;
                while (totalRead < responseSize)
                {
                    var read = await _stream.ReadAsync(responseBuffer.AsMemory(totalRead, responseSize - totalRead), cancellationToken);
                    if (read == 0)
                    {
                        throw new EndOfStreamException("Connection closed while reading response");
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

                // Complete the pending request
                if (_pendingRequests.TryRemove(header.CorrelationId, out var tcs))
                {
                    tcs.TrySetResult(bodyBuffer);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Complete all pending requests with exception
            foreach (var kvp in _pendingRequests)
            {
                kvp.Value.TrySetException(ex);
            }
            _pendingRequests.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
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
    }
}
