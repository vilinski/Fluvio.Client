using System.Text;
using System.Text.Json;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Admin;
using Fluvio.Client.Consumer;
using Fluvio.Client.Producer;
using Fluvio.Client.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Fluvio.Client;

/// <summary>
/// Main Fluvio client for connecting to a Fluvio cluster.
/// Connects via the native Rust FFI layer (see <see cref="Interop"/>), which wraps the
/// official `fluvio` Rust client.
/// </summary>
public sealed class FluvioClient : IFluvioClient
{
    private readonly FluvioClientOptions _options;
    private readonly ILogger<FluvioClient> _logger;
    private readonly FluvioMetrics? _metrics;
    private readonly Interop.RustResource _handle;
    private bool _disposed;

    /// <summary>
    /// Gets the metrics collector for this client, if metrics are enabled.
    /// </summary>
    public FluvioMetrics? Metrics => _metrics;

    private FluvioClient(Interop.RustResource handle, FluvioClientOptions options, ILogger<FluvioClient> logger, FluvioMetrics? metrics)
    {
        _handle = handle;
        _options = options;
        _logger = logger;
        _metrics = metrics;
    }

    /// <summary>
    /// Merge provided options with default endpoints. Profile-based config resolution
    /// (<c>~/.fluvio/config</c>) is delegated to the native Rust client (see spec §7);
    /// this only fills in defaults for values the caller didn't provide.
    /// </summary>
    private static FluvioClientOptions MergeWithConfig(FluvioClientOptions? provided)
    {
        // If everything is provided, no need to apply defaults
        if (provided is { SpuEndpoint: not null, ScEndpoint: not null, UseTls: not null })
            return provided;

        var spuEndpoint = provided?.SpuEndpoint ?? "localhost:9010";
        var scEndpoint = provided?.ScEndpoint ?? "localhost:9003";
        var useTls = provided?.UseTls ?? false;

        if (provided != null)
        {
            return provided with
            {
                SpuEndpoint = spuEndpoint,
                ScEndpoint = scEndpoint,
                UseTls = useTls
            };
        }

        return new FluvioClientOptions
        {
            SpuEndpoint = spuEndpoint,
            ScEndpoint = scEndpoint,
            UseTls = useTls
        };
    }

    /// <summary>
    /// Creates a Fluvio client and connects to the cluster via the native FFI layer.
    /// </summary>
    /// <param name="options">Client options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A connected <see cref="FluvioClient"/> instance.</returns>
    public static async Task<FluvioClient> ConnectAsync(FluvioClientOptions? options = null, CancellationToken cancellationToken = default)
    {
        var mergedOptions = MergeWithConfig(options);
        var logger = mergedOptions.LoggerFactory?.CreateLogger<FluvioClient>()
                     ?? NullLoggerFactory.Instance.CreateLogger<FluvioClient>();
        var metrics = mergedOptions.EnableMetrics ? new FluvioMetrics() : null;

        var endpoint = mergedOptions.ScEndpoint ?? mergedOptions.SpuEndpoint ?? "localhost:9003";
        logger.LogInformation("Connecting to Fluvio cluster at {Endpoint}", endpoint);

        var configJson = FluvioNativeConfig.ToJson(endpoint, mergedOptions.UseTls ?? false);
        var bytes = Encoding.UTF8.GetBytes(configJson);

        try
        {
            // `p` is a "fixed local" and cannot be captured by the lambda passed to
            // CallAsync (CS1764), so its address is captured as a plain nint instead
            // and cast back to a pointer inside the lambda; the fixed block's pin is
            // still in effect because CallAsync invokes the lambda synchronously.
            Task<nint> connectTask;
            unsafe
            {
                fixed (byte* p = bytes)
                {
                    var configAddr = (nint)p;
                    connectTask = Interop.Callbacks.CallAsync(tcb =>
                    {
                        unsafe
                        {
                            Interop.Native.ClientConnect((byte*)configAddr, (nuint)bytes.Length, tcb);
                        }
                    });
                }
            }
            var resultPtr = await connectTask.ConfigureAwait(false);

            var handle = new Interop.RustResource(resultPtr, Interop.Native.ClientDrop);
            logger.LogInformation("Fluvio client connected successfully to {Endpoint}", endpoint);
            metrics?.RecordConnection(endpoint, "cluster");
            metrics?.IncrementActiveConnections(endpoint);
            return new FluvioClient(handle, mergedOptions, logger, metrics);
        }
        catch (Exception ex)
        {
            metrics?.RecordConnectionFailure(endpoint, "cluster", ex.GetType().Name);
            metrics?.Dispose();
            logger.LogError(ex, "Failed to connect to Fluvio cluster");
            throw;
        }
    }

    /// <summary>
    /// Connects the client to the Fluvio cluster.
    /// </summary>
    /// <remarks>
    /// <see cref="FluvioClient"/> instances are always already connected once constructed
    /// (via <see cref="ConnectAsync(FluvioClientOptions?, CancellationToken)"/>), so this is a
    /// no-op that exists to satisfy <see cref="IFluvioClient"/>.
    /// </remarks>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        EnsureConnected();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets a producer instance.
    /// </summary>
    /// <param name="options">Producer options.</param>
    /// <returns>A producer instance.</returns>
    public IFluvioProducer Producer(ProducerOptions? options = null)
    {
        EnsureConnected();
        return new Producer.FluvioProducer(_handle, options);
    }

    /// <summary>
    /// Gets a consumer instance.
    /// </summary>
    /// <param name="options">Consumer options.</param>
    /// <returns>A consumer instance.</returns>
    public IFluvioConsumer Consumer(ConsumerOptions? options = null)
    {
        EnsureConnected();
        return new Consumer.FluvioConsumer(_handle, options, _options.ClientId, _logger);
    }

    /// <summary>
    /// Gets an admin instance.
    /// </summary>
    /// <returns>An admin instance.</returns>
    public IFluvioAdmin Admin()
    {
        EnsureConnected();
        return new Admin.FluvioAdmin(_handle);
    }

    private void EnsureConnected()
    {
        if (_disposed || _handle.IsInvalid)
        {
            throw new InvalidOperationException("Client is not connected. Call ConnectAsync first.");
        }
    }

    /// <summary>
    /// Checks the health of the Fluvio client connection via the native FFI layer.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Health check result with connection status and diagnostics.</returns>
    public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Performing health check");

        if (_disposed || _handle.IsInvalid)
        {
            return HealthCheckResult.Unhealthy("Client not connected");
        }

        try
        {
            var jsonPtr = await _handle.RunAsyncWithIncrement(h =>
                Interop.Callbacks.CallAsync(tcb => Interop.Native.ClientHealthCheck(h, tcb))).ConfigureAwait(false);
            var json = Interop.Native.ReadAndFreeString(jsonPtr);
            var result = FluvioNativeConfig.ParseHealth(json);
            _logger.LogInformation("Health check completed: IsHealthy={IsHealthy}", result.IsHealthy);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Health check request failed");
            return HealthCheckResult.Unhealthy($"Health check request failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Disposes the client and its resources.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _logger.LogDebug("Disposing Fluvio client");
        _disposed = true;
        _handle.Dispose();

        var endpoint = _options.ScEndpoint ?? _options.SpuEndpoint;
        if (endpoint != null)
        {
            _metrics?.DecrementActiveConnections(endpoint);
        }
        _metrics?.Dispose();

        _logger.LogInformation("Fluvio client disposed");
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Builds the JSON payload sent to the native FFI's client-connect entry point, and parses
/// the JSON payload returned by its health-check entry point.
/// </summary>
internal static class FluvioNativeConfig
{
    /// <summary>
    /// Builds the connect-config JSON consumed by the Rust side's <c>ConnectConfig</c> DTO,
    /// which is then translated into a real <c>fluvio::FluvioConfig</c> (whose <c>tls</c> field
    /// is a <c>TlsPolicy</c> enum, not a plain boolean, so it cannot be deserialized directly
    /// from this shape). Built with <see cref="Utf8JsonWriter"/>/<see cref="JsonDocument"/> rather
    /// than reflection-based <see cref="JsonSerializer"/> so this stays trim/AOT compatible.
    /// </summary>
    public static string ToJson(string endpoint, bool useTls)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteString("endpoint", endpoint);
            writer.WriteBoolean("useTls", useTls);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    public static HealthCheckResult ParseHealth(string? json)
    {
        if (string.IsNullOrEmpty(json))
        {
            return HealthCheckResult.Unhealthy("Health check returned no data");
        }

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        var isHealthy = root.TryGetProperty("isHealthy", out var isHealthyProp) && isHealthyProp.GetBoolean();
        TimeSpan? duration = root.TryGetProperty("elapsedMs", out var elapsedProp) && elapsedProp.TryGetInt64(out var ms)
            ? TimeSpan.FromMilliseconds(ms)
            : null;

        return isHealthy
            ? HealthCheckResult.Healthy(spuConnected: true, scConnected: true, duration)
            : HealthCheckResult.Unhealthy("Native health probe failed", spuConnected: false);
    }
}
