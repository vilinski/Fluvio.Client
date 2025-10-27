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
}

/// <summary>
/// Client configuration options
/// </summary>
public record FluvioClientOptions(
    string Endpoint = "localhost:9003",
    bool UseTls = false,
    string? ClientId = null,
    TimeSpan ConnectionTimeout = default,
    TimeSpan RequestTimeout = default)
{
    public TimeSpan ConnectionTimeout { get; init; } = ConnectionTimeout == default ? TimeSpan.FromSeconds(30) : ConnectionTimeout;
    public TimeSpan RequestTimeout { get; init; } = RequestTimeout == default ? TimeSpan.FromSeconds(60) : RequestTimeout;
}

/// <summary>
/// Producer configuration options
/// </summary>
public record ProducerOptions(
    int BatchSize = 1000,
    TimeSpan LingerTime = default,
    DeliveryGuarantee DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce)
{
    public TimeSpan LingerTime { get; init; } = LingerTime == default ? TimeSpan.FromMilliseconds(100) : LingerTime;
}

/// <summary>
/// Consumer configuration options
/// </summary>
public record ConsumerOptions(
    int MaxBytes = 1024 * 1024,
    IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted);

/// <summary>
/// Delivery guarantee mode
/// </summary>
public enum DeliveryGuarantee
{
    AtMostOnce,
    AtLeastOnce
}

/// <summary>
/// Isolation level for consumer reads
/// </summary>
public enum IsolationLevel
{
    ReadUncommitted,
    ReadCommitted
}
