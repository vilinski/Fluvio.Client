using Fluvio.Client.Abstractions;
using Fluvio.Client.Admin;
using Fluvio.Client.Consumer;
using Fluvio.Client.Network;
using Fluvio.Client.Producer;

namespace Fluvio.Client;

/// <summary>
/// Main Fluvio client for connecting to a Fluvio cluster
/// </summary>
public sealed class FluvioClient : IFluvioClient
{
    private readonly FluvioClientOptions _options;
    private FluvioConnection? _connection;
    private bool _isConnected;

    public FluvioClient(FluvioClientOptions? options = null)
    {
        _options = options ?? new FluvioClientOptions();
    }

    /// <summary>
    /// Create a Fluvio client and connect to the cluster
    /// </summary>
    public static async Task<FluvioClient> ConnectAsync(FluvioClientOptions? options = null, CancellationToken cancellationToken = default)
    {
        var client = new FluvioClient(options);
        await client.ConnectAsync(cancellationToken);
        return client;
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_isConnected)
        {
            return;
        }

        // Parse endpoint (format: "host:port")
        var parts = _options.Endpoint.Split(':');
        if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
        {
            throw new ArgumentException($"Invalid endpoint format: {_options.Endpoint}. Expected format: 'host:port'");
        }

        var host = parts[0];

        _connection = new FluvioConnection(host, port, _options.UseTls, _options.ConnectionTimeout);
        await _connection.ConnectAsync(cancellationToken);
        _isConnected = true;
    }

    public IFluvioProducer Producer(ProducerOptions? options = null)
    {
        EnsureConnected();
        return new FluvioProducer(_connection!, options, _options.ClientId);
    }

    public IFluvioConsumer Consumer(ConsumerOptions? options = null)
    {
        EnsureConnected();
        return new FluvioConsumer(_connection!, options, _options.ClientId);
    }

    public IFluvioAdmin Admin()
    {
        EnsureConnected();
        return new FluvioAdmin(_connection!, _options.ClientId);
    }

    private void EnsureConnected()
    {
        if (!_isConnected || _connection == null)
        {
            throw new InvalidOperationException("Client is not connected. Call ConnectAsync first.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection != null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
        _isConnected = false;
    }
}
