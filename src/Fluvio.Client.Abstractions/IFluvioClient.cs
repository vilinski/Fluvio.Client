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
/// </summary>
/// <param name="SpuEndpoint">SPU (Streaming Processing Unit) endpoint for data operations. Format: "host:port". If not provided, loaded from ~/.fluvio/config or defaults to "localhost:9010".</param>
/// <param name="ScEndpoint">SC (Stream Controller) endpoint for admin operations. Format: "host:port". If not provided, loaded from ~/.fluvio/config or defaults to "localhost:9003".</param>
/// <param name="UseTls">Whether to use TLS for connections. If not provided, loaded from ~/.fluvio/config or defaults to false.</param>
/// <param name="ClientId">Optional client identifier for logging and debugging.</param>
/// <param name="Profile">Fluvio profile name to load from ~/.fluvio/config. If not provided, uses the current_profile from config.</param>
/// <param name="ConnectionTimeout">Maximum time to wait for connection establishment. Defaults to 30 seconds.</param>
/// <param name="RequestTimeout">Maximum time to wait for request completion. Defaults to 60 seconds.</param>
/// <param name="LoggerFactory">Logger factory for creating loggers. If not provided, logging is disabled.</param>
/// <param name="MaxRetries">Maximum number of retry attempts for failed operations. Defaults to 3.</param>
/// <param name="RetryBaseDelay">Base delay for exponential backoff retry strategy. Defaults to 100ms.</param>
/// <param name="EnableCircuitBreaker">Whether to enable circuit breaker pattern. When enabled, repeated failures will open the circuit. Defaults to true.</param>
/// <param name="CircuitBreakerFailureThreshold">Number of consecutive failures before opening the circuit breaker. Defaults to 5.</param>
/// <param name="CircuitBreakerDuration">How long the circuit breaker stays open before attempting recovery. Defaults to 30 seconds.</param>
/// <param name="EnableMetrics">Whether to enable metrics collection via System.Diagnostics.Metrics for OpenTelemetry integration. Defaults to true.</param>
/// <param name="EnableAutoReconnect">Whether to enable automatic reconnection on connection failure. Defaults to true.</param>
/// <param name="MaxReconnectAttempts">Maximum number of reconnection attempts before marking connection as failed. Defaults to 5.</param>
/// <param name="ReconnectDelay">Base delay between reconnection attempts (uses exponential backoff). Defaults to 2 seconds.</param>
/// <param name="TimeProvider">Time provider for testability. If not provided, uses TimeProvider.System.</param>
/// <remarks>
/// All parameters are optional. Configuration resolution order:
/// <list type="number">
/// <item>Explicitly provided parameters</item>
/// <item>Active profile from ~/.fluvio/config</item>
/// <item>Sensible defaults</item>
/// </list>
/// </remarks>
public record FluvioClientOptions(
    string? SpuEndpoint = null,
    string? ScEndpoint = null,
    bool? UseTls = null,
    string? ClientId = null,
    string? Profile = null,
    TimeSpan ConnectionTimeout = default,
    TimeSpan RequestTimeout = default,
    ILoggerFactory? LoggerFactory = null,
    int MaxRetries = 3,
    TimeSpan RetryBaseDelay = default,
    bool EnableCircuitBreaker = true,
    int CircuitBreakerFailureThreshold = 5,
    TimeSpan CircuitBreakerDuration = default,
    bool EnableMetrics = true,
    bool EnableAutoReconnect = true,
    int MaxReconnectAttempts = 5,
    TimeSpan ReconnectDelay = default,
    TimeProvider? TimeProvider = null)
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
    /// Gets the SPU (Streaming Processing Unit) endpoint for Producer/Consumer operations.
    /// Will be resolved from config if not specified. Default: localhost:9010
    /// </summary>
    public string? SpuEndpoint { get; init; } = SpuEndpoint;

    /// <summary>
    /// Gets the SC (Stream Controller) endpoint for Admin operations.
    /// Will be resolved from config if not specified. Default: localhost:9003
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

    /// <summary>
    /// Gets whether automatic reconnection is enabled.
    /// When enabled, connections will automatically attempt to reconnect on failure.
    /// Default is true.
    /// </summary>
    public bool EnableAutoReconnect { get; init; } = EnableAutoReconnect;

    /// <summary>
    /// Gets the maximum number of reconnection attempts before marking connection as failed.
    /// Default is 5 attempts.
    /// </summary>
    public int MaxReconnectAttempts { get; init; } = MaxReconnectAttempts <= 0 ? 5 : MaxReconnectAttempts;

    /// <summary>
    /// Gets the base delay between reconnection attempts (uses exponential backoff).
    /// Default is 2 seconds.
    /// </summary>
    public TimeSpan ReconnectDelay { get; init; } = ReconnectDelay == default ? TimeSpan.FromSeconds(2) : ReconnectDelay;

    /// <summary>
    /// Gets the time provider for testability.
    /// If null, uses TimeProvider.System.
    /// </summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider ?? TimeProvider.System;
}

/// <summary>
/// Producer configuration options.
/// </summary>
/// <param name="BatchSize">Number of records to batch together before sending. Defaults to 1000.</param>
/// <param name="MaxRequestSize">Maximum size in bytes for a single produce request. Defaults to 1 MB (matches Rust client).</param>
/// <param name="LingerTime">Time to wait for batching records before sending. Defaults to 100ms.</param>
/// <param name="Timeout">Timeout for produce requests. Defaults to 5 seconds for faster failure detection.</param>
/// <param name="DeliveryGuarantee">Delivery guarantee mode (AtMostOnce or AtLeastOnce). Defaults to AtLeastOnce.</param>
/// <param name="SmartModules">Optional list of SmartModule invocations for server-side transformations.</param>
/// <param name="Partitioner">Partitioner for selecting which partition to send records to. Defaults to SiphashRoundRobinPartitioner.</param>
public record ProducerOptions(
    int BatchSize = 1000,
    int MaxRequestSize = 1024 * 1024,
    TimeSpan LingerTime = default,
    TimeSpan Timeout = default,
    DeliveryGuarantee DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
    IReadOnlyList<SmartModuleInvocation>? SmartModules = null,
    IPartitioner? Partitioner = null)
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

    /// <summary>
    /// Gets the partitioner for selecting which partition to send records to.
    /// If null, defaults to SiphashRoundRobinPartitioner (same as Rust client).
    /// </summary>
    public IPartitioner? Partitioner { get; init; } = Partitioner;
}

/// <summary>
/// Consumer configuration options.
/// </summary>
/// <param name="MaxBytes">Maximum number of bytes to fetch in a single request. Defaults to 1 MB.</param>
/// <param name="IsolationLevel">Isolation level for consumer reads (ReadUncommitted or ReadCommitted). Defaults to ReadCommitted.</param>
/// <param name="SmartModules">Optional list of SmartModule invocations for server-side filtering and transformations.</param>
/// <param name="ConsumerGroup">Optional consumer group name for offset tracking. If provided, offsets are stored on the server.</param>
/// <param name="AutoCommit">Whether to automatically commit offsets. Defaults to true.</param>
/// <param name="AutoCommitInterval">Interval for automatic offset commits. Defaults to 5 seconds.</param>
/// <param name="OffsetReset">Strategy for determining where to start consuming when no offset is stored. Defaults to Latest.</param>
public record ConsumerOptions(
    int MaxBytes = 1024 * 1024,
    IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted,
    IReadOnlyList<SmartModuleInvocation>? SmartModules = null,
    string? ConsumerGroup = null,
    bool AutoCommit = true,
    TimeSpan AutoCommitInterval = default,
    OffsetResetStrategy OffsetReset = OffsetResetStrategy.Latest)
{
    /// <summary>
    /// Gets the auto-commit interval for offset commits.
    /// Default is 5 seconds.
    /// </summary>
    public TimeSpan AutoCommitInterval { get; init; } = AutoCommitInterval == default ? TimeSpan.FromSeconds(5) : AutoCommitInterval;
};

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
/// Offset reset strategy - determines where to start consuming when no offset is stored
/// </summary>
public enum OffsetResetStrategy
{
    /// <summary>
    /// Start from the earliest available offset (beginning of topic)
    /// </summary>
    Earliest,
    /// <summary>
    /// Start from the latest offset (end of topic, only new messages)
    /// </summary>
    Latest,
    /// <summary>
    /// Resume from stored offset, or use Earliest if no offset stored
    /// </summary>
    StoredOrEarliest,
    /// <summary>
    /// Resume from stored offset, or use Latest if no offset stored
    /// </summary>
    StoredOrLatest
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
