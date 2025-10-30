using Fluvio.Client.Abstractions;
using Fluvio.Client.Admin;
using Fluvio.Client.Consumer;
using Fluvio.Client.Network;
using Fluvio.Client.Producer;
using Fluvio.Client.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Fluvio.Client;

/// <summary>
/// Main Fluvio client for connecting to a Fluvio cluster
/// </summary>
public sealed class FluvioClient : IFluvioClient
{
    private readonly FluvioClientOptions _options;
    private readonly ILogger<FluvioClient> _logger;
    private readonly FluvioMetrics? _metrics;
    private FluvioConnection? _spuConnection;  // For Producer/Consumer (port 9010)
    private FluvioConnection? _scConnection;   // For Admin (port 9003)
    private bool _isConnected;

    /// <summary>
    /// Gets the metrics collector for this client, if metrics are enabled.
    /// </summary>
    public FluvioMetrics? Metrics => _metrics;

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioClient"/> class.
    /// </summary>
    /// <param name="options">Client options.</param>
    public FluvioClient(FluvioClientOptions? options = null)
    {
        _options = options ?? new FluvioClientOptions();
        _logger = _options.LoggerFactory?.CreateLogger<FluvioClient>()
                  ?? NullLoggerFactory.Instance.CreateLogger<FluvioClient>();
        _metrics = _options.EnableMetrics ? new FluvioMetrics() : null;
    }

    /// <summary>
    /// Create a Fluvio client and connect to the cluster
    /// </summary>
    /// <summary>
    /// Creates a Fluvio client and connects to the cluster.
    /// </summary>
    /// <param name="options">Client options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A connected <see cref="FluvioClient"/> instance.</returns>
    public static async Task<FluvioClient> ConnectAsync(FluvioClientOptions? options = null, CancellationToken cancellationToken = default)
    {
        var client = new FluvioClient(options);
        await client.ConnectAsync(cancellationToken);
        return client;
    }

    /// <summary>
    /// Connects the client to the Fluvio cluster.
    /// Connects to SPU for Producer/Consumer operations.
    /// If ScEndpoint is configured, also connects to SC for Admin operations.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_isConnected)
        {
            _logger.LogDebug("Client already connected");
            return;
        }

        _logger.LogInformation("Connecting to Fluvio cluster at {Endpoint}", _options.Endpoint);

        // Validate options
        try
        {
            _options.Validate();
        }
        catch (ArgumentException ex)
        {
            _logger.LogError(ex, "Invalid client configuration");
            throw;
        }

        // Parse SPU endpoint (format: "host:port")
        var parts = _options.Endpoint.Split(':');
        if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
        {
            var error = $"Invalid endpoint format: {_options.Endpoint}. Expected format: 'host:port'";
            _logger.LogError(error);
            throw new ArgumentException(error);
        }

        var host = parts[0];

        try
        {
            // Create resilience policies for connection operations
            var connectionLogger = _options.LoggerFactory?.CreateLogger<FluvioConnection>();

            // Build resilience policy: circuit breaker + retry
            Polly.IAsyncPolicy? spuPolicy = null;
            if (_options.EnableCircuitBreaker)
            {
                spuPolicy = Resilience.CircuitBreakerFactory.CreateResilientPolicy(
                    _options.MaxRetries,
                    _options.RetryBaseDelay,
                    _options.CircuitBreakerFailureThreshold,
                    _options.CircuitBreakerDuration,
                    connectionLogger);
                _logger.LogDebug("Circuit breaker enabled for SPU connection");
            }
            else
            {
                spuPolicy = Resilience.RetryPolicyFactory.CreatePolicy(
                    _options.MaxRetries,
                    _options.RetryBaseDelay,
                    connectionLogger);
            }

            _logger.LogDebug("Connecting to SPU at {Host}:{Port}", host, port);
            _metrics?.RecordConnection(_options.Endpoint, "SPU");
            _spuConnection = new FluvioConnection(host, port, _options.UseTls, _options.ConnectionTimeout, connectionLogger, spuPolicy);
            await _spuConnection.ConnectAsync(cancellationToken);
            _metrics?.IncrementActiveConnections(_options.Endpoint);
            _logger.LogInformation("Successfully connected to SPU at {Host}:{Port}", host, port);

            // Eagerly connect to SC if endpoint is configured
            if (!string.IsNullOrEmpty(_options.ScEndpoint))
            {
                var scParts = _options.ScEndpoint.Split(':');
                if (scParts.Length == 2 && int.TryParse(scParts[1], out var scPort))
                {
                    // Use more aggressive policy for admin operations (SC)
                    Polly.IAsyncPolicy? scPolicy = null;
                    if (_options.EnableCircuitBreaker)
                    {
                        var circuitBreaker = Resilience.CircuitBreakerFactory.CreateAdminPolicy(
                            3, // Lower threshold for SC
                            TimeSpan.FromSeconds(60),
                            connectionLogger);
                        var retry = Resilience.RetryPolicyFactory.CreateAdminPolicy(
                            _options.MaxRetries,
                            _options.RetryBaseDelay,
                            connectionLogger);
                        scPolicy = Polly.Policy.WrapAsync(circuitBreaker, retry);
                        _logger.LogDebug("Circuit breaker enabled for SC connection");
                    }
                    else
                    {
                        scPolicy = Resilience.RetryPolicyFactory.CreateAdminPolicy(
                            _options.MaxRetries,
                            _options.RetryBaseDelay,
                            connectionLogger);
                    }

                    _logger.LogDebug("Connecting to SC at {Host}:{Port}", scParts[0], scPort);
                    _metrics?.RecordConnection(_options.ScEndpoint, "SC");
                    _scConnection = new FluvioConnection(scParts[0], scPort, _options.UseTls, _options.ConnectionTimeout, connectionLogger, scPolicy);
                    await _scConnection.ConnectAsync(cancellationToken);
                    _metrics?.IncrementActiveConnections(_options.ScEndpoint);
                    _logger.LogInformation("Successfully connected to SC at {Host}:{Port}", scParts[0], scPort);
                }
            }

            _isConnected = true;
            _logger.LogInformation("Fluvio client connected successfully");
        }
        catch (Exception ex)
        {
            _metrics?.RecordConnectionFailure(_options.Endpoint, "SPU", ex.GetType().Name);
            _logger.LogError(ex, "Failed to connect to Fluvio cluster");
            throw;
        }
    }

    /// <summary>
    /// Gets a producer instance.
    /// </summary>
    /// <param name="options">Producer options.</param>
    /// <returns>A producer instance.</returns>
    public IFluvioProducer Producer(ProducerOptions? options = null)
    {
        EnsureConnected();
        _logger.LogDebug("Creating producer instance");
        return new FluvioProducer(_spuConnection!, options, _options.ClientId);
    }

    /// <summary>
    /// Gets a consumer instance.
    /// </summary>
    /// <param name="options">Consumer options.</param>
    /// <returns>A consumer instance.</returns>
    public IFluvioConsumer Consumer(ConsumerOptions? options = null)
    {
        EnsureConnected();
        _logger.LogDebug("Creating consumer instance");
        return new FluvioConsumer(_spuConnection!, options, _options.ClientId);
    }

    /// <summary>
    /// Gets an admin instance.
    /// Requires SC connection to be established (via ScEndpoint in options during ConnectAsync).
    /// </summary>
    /// <returns>An admin instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when SC connection is not available.</exception>
    public IFluvioAdmin Admin()
    {
        EnsureConnected();

        if (_scConnection == null)
        {
            _logger.LogError("Cannot create Admin instance: SC connection not established");
            throw new InvalidOperationException(
                "SC connection not established. Configure ScEndpoint in FluvioClientOptions to use Admin operations.");
        }

        _logger.LogDebug("Creating admin instance");
        return new FluvioAdmin(_scConnection, _options.ClientId);
    }

    private void EnsureConnected()
    {
        if (!_isConnected || _spuConnection == null)
        {
            throw new InvalidOperationException("Client is not connected. Call ConnectAsync first.");
        }
    }

    /// <summary>
    /// Checks the health of the Fluvio client connections.
    /// Performs a lightweight health check by verifying connection status and optionally testing with a list topics request.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Health check result with connection status and diagnostics.</returns>
    public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Performing health check");

        try
        {
            if (!_isConnected || _spuConnection == null)
            {
                return HealthCheckResult.Unhealthy("Client not connected");
            }

            var spuConnected = _spuConnection.IsConnected;
            var scConnected = _scConnection?.IsConnected;

            if (!spuConnected)
            {
                return HealthCheckResult.Unhealthy("SPU connection lost", false, scConnected);
            }

            // Perform a lightweight request to verify the connection is responsive
            // Use ListTopics as it's a simple read-only operation
            TimeSpan? requestDuration = null;
            if (_scConnection != null && scConnected == true)
            {
                try
                {
                    var startTime = DateTime.UtcNow;
                    var admin = Admin();

                    // Quick timeout for health check (5 seconds)
                    using var healthCheckCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, healthCheckCts.Token);

                    await admin.ListTopicsAsync(linkedCts.Token);
                    requestDuration = DateTime.UtcNow - startTime;

                    _logger.LogInformation("Health check passed: SPU={SpuConnected}, SC={ScConnected}, RequestDuration={Duration}ms",
                        spuConnected, scConnected, requestDuration?.TotalMilliseconds);

                    return HealthCheckResult.Healthy(spuConnected, scConnected, requestDuration);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Health check request failed");
                    return HealthCheckResult.Unhealthy($"Health check request failed: {ex.Message}", spuConnected, scConnected);
                }
            }

            // If no SC connection, just return connection status
            _logger.LogInformation("Health check passed: SPU={SpuConnected}, SC={ScConnected}", spuConnected, scConnected);
            return HealthCheckResult.Healthy(spuConnected, scConnected, null);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Health check failed");
            return HealthCheckResult.Unhealthy($"Health check error: {ex.Message}");
        }
    }

    /// <summary>
    /// Disposes the client and its resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        _logger.LogDebug("Disposing Fluvio client");

        if (_spuConnection != null)
        {
            _metrics?.DecrementActiveConnections(_options.Endpoint);
            await _spuConnection.DisposeAsync();
            _spuConnection = null;
            _logger.LogDebug("SPU connection disposed");
        }

        if (_scConnection != null)
        {
            _metrics?.DecrementActiveConnections(_options.ScEndpoint);
            await _scConnection.DisposeAsync();
            _scConnection = null;
            _logger.LogDebug("SC connection disposed");
        }

        _isConnected = false;
        _metrics?.Dispose();
        _logger.LogInformation("Fluvio client disposed");
    }
}
