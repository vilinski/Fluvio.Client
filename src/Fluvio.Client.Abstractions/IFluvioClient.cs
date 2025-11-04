using Microsoft.Extensions.Logging;

namespace Fluvio.Client.Abstractions;

/// <summary>
/// Main Fluvio client interface
/// </summary>
public interface IFluvioClient : IAsyncDisposable
{
    /// <summary>
    /// Get a producer instance
    /// </summary>
    IFluvioProducer Producer(ProducerOptions? options = null);

    /// <summary>
    /// Get a consumer instance
    /// </summary>
    IFluvioConsumer Consumer(ConsumerOptions? options = null);

    /// <summary>
    /// Get an admin instance
    /// </summary>
    IFluvioAdmin Admin();

    /// <summary>
    /// Connect to the Fluvio cluster
    /// </summary>
    Task ConnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Check the health of the Fluvio client connections
    /// </summary>
    Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Client configuration options.
/// All parameters are optional. If not provided, the client will use:
/// 1. Active profile from ~/.fluvio/config
/// 2. Sensible defaults (localhost:9010 for SPU, localhost:9003 for SC, TLS off)
/// </summary>
public record FluvioClientOptions(
    string? Endpoint = null,             // SPU endpoint (default: from config or localhost:9010)
    string? ScEndpoint = null,           // SC endpoint (default: from config or localhost:9003)
    bool? UseTls = null,                 // TLS enabled (default: from config or false)
    string? ClientId = null,             // Optional client ID
    string? Profile = null,              // Fluvio profile name (default: current_profile from config)
    TimeSpan ConnectionTimeout = default,
    TimeSpan RequestTimeout = default,
    ILoggerFactory? LoggerFactory = null,
    int MaxRetries = 3,
    TimeSpan RetryBaseDelay = default,
    bool EnableCircuitBreaker = true,
    int CircuitBreakerFailureThreshold = 5,
    TimeSpan CircuitBreakerDuration = default,
    bool EnableMetrics = true)
{
    /// <summary>
    /// Gets the connection timeout for the client.
    /// </summary>
    public TimeSpan ConnectionTimeout { get; init; } = ConnectionTimeout == default ? TimeSpan.FromSeconds(30) : ConnectionTimeout;

    /// <summary>
    /// Gets the request timeout for the client.
    /// </summary>
    public TimeSpan RequestTimeout { get; init; } = RequestTimeout == default ? TimeSpan.FromSeconds(60) : RequestTimeout;

    /// <summary>
    /// Gets the SPU endpoint for Producer/Consumer operations.
    /// Will be resolved from config if not specified.
    /// </summary>
    public string? Endpoint { get; init; } = Endpoint;

    /// <summary>
    /// Gets the SC (Stream Controller) endpoint for Admin operations.
    /// Will be resolved from config if not specified.
    /// </summary>
    public string? ScEndpoint { get; init; } = ScEndpoint;

    /// <summary>
    /// Gets whether TLS is enabled.
    /// Will be resolved from config if not specified.
    /// </summary>
    public bool? UseTls { get; init; } = UseTls;

    /// <summary>
    /// Gets the Fluvio profile name to use.
    /// If null, uses current_profile from ~/.fluvio/config.
    /// </summary>
    public string? Profile { get; init; } = Profile;

    /// <summary>
    /// Gets the logger factory for creating loggers (optional).
    /// If null, no logging will be performed.
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; init; } = LoggerFactory;

    /// <summary>
    /// Gets the maximum number of retry attempts for failed operations.
    /// Default is 3 retries.
    /// </summary>
    public int MaxRetries { get; init; } = MaxRetries <= 0 ? 3 : MaxRetries;

    /// <summary>
    /// Gets the base delay for exponential backoff retry strategy.
    /// Default is 100ms.
    /// </summary>
    public TimeSpan RetryBaseDelay { get; init; } = RetryBaseDelay == default ? TimeSpan.FromMilliseconds(100) : RetryBaseDelay;

    /// <summary>
    /// Gets whether circuit breaker pattern is enabled.
    /// When enabled, repeated failures will open the circuit and prevent further attempts until recovery.
    /// Default is true.
    /// </summary>
    public bool EnableCircuitBreaker { get; init; } = EnableCircuitBreaker;

    /// <summary>
    /// Gets the number of consecutive failures before opening the circuit breaker.
    /// Default is 5 failures.
    /// </summary>
    public int CircuitBreakerFailureThreshold { get; init; } = CircuitBreakerFailureThreshold <= 0 ? 5 : CircuitBreakerFailureThreshold;

    /// <summary>
    /// Gets how long the circuit breaker stays open before attempting recovery.
    /// Default is 30 seconds.
    /// </summary>
    public TimeSpan CircuitBreakerDuration { get; init; } = CircuitBreakerDuration == default ? TimeSpan.FromSeconds(30) : CircuitBreakerDuration;

    /// <summary>
    /// Gets whether metrics collection is enabled.
    /// When enabled, the client will emit metrics via System.Diagnostics.Metrics for OpenTelemetry integration.
    /// Default is true.
    /// </summary>
    public bool EnableMetrics { get; init; } = EnableMetrics;
}

/// <summary>
/// Producer configuration options
/// </summary>
public record ProducerOptions(
    int BatchSize = 1000,
    int MaxRequestSize = 1024 * 1024,  // 1 MB default (matches Rust client)
    TimeSpan LingerTime = default,
    TimeSpan Timeout = default,
    DeliveryGuarantee DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
    IReadOnlyList<SmartModuleInvocation>? SmartModules = null)
{
    /// <summary>
    /// Gets the maximum size in bytes for a single produce request.
    /// Default is 1 MB to match Rust client.
    /// </summary>
    public int MaxRequestSize { get; init; } = MaxRequestSize <= 0 ? 1024 * 1024 : MaxRequestSize;

    /// <summary>
    /// Gets the linger time for batching records.
    /// </summary>
    public TimeSpan LingerTime { get; init; } = LingerTime == default ? TimeSpan.FromMilliseconds(100) : LingerTime;

    /// <summary>
    /// Gets the timeout for produce requests.
    /// Reduced from 30s to 5s for faster failure detection.
    /// </summary>
    public TimeSpan Timeout { get; init; } = Timeout == default ? TimeSpan.FromSeconds(5) : Timeout;
}

/// <summary>
/// Consumer configuration options
/// </summary>
public record ConsumerOptions(
    int MaxBytes = 1024 * 1024,
    IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted,
    IReadOnlyList<SmartModuleInvocation>? SmartModules = null);

/// <summary>
/// Delivery guarantee mode
/// </summary>
public enum DeliveryGuarantee
{
    /// <summary>
    /// At most once delivery guarantee.
    /// </summary>
    AtMostOnce,
    /// <summary>
    /// At least once delivery guarantee.
    /// </summary>
    AtLeastOnce
}

/// <summary>
/// Isolation level for consumer reads
/// </summary>
public enum IsolationLevel
{
    /// <summary>
    /// Read uncommitted isolation level.
    /// </summary>
    ReadUncommitted,
    /// <summary>
    /// Read committed isolation level.
    /// </summary>
    ReadCommitted
}

/// <summary>
/// Result of a health check operation
/// </summary>
public record HealthCheckResult(
    bool IsHealthy,
    bool SpuConnected,
    bool? ScConnected,
    TimeSpan? LastSuccessfulRequestDuration,
    string? ErrorMessage,
    DateTime CheckTimestamp)
{
    /// <summary>
    /// Creates a healthy result
    /// </summary>
    public static HealthCheckResult Healthy(bool spuConnected, bool? scConnected, TimeSpan? requestDuration) =>
        new(true, spuConnected, scConnected, requestDuration, null, DateTime.UtcNow);

    /// <summary>
    /// Creates an unhealthy result
    /// </summary>
    public static HealthCheckResult Unhealthy(string errorMessage, bool spuConnected = false, bool? scConnected = null) =>
        new(false, spuConnected, scConnected, null, errorMessage, DateTime.UtcNow);
}
