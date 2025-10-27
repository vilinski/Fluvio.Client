namespace Fluvio.Client.Abstractions;

/// <summary>
/// Consumer interface for receiving records from Fluvio topics
/// </summary>
public interface IFluvioConsumer : IAsyncDisposable
{
    /// <summary>
    /// Stream records from the specified topic starting at the given offset
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="offset">Starting offset</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Async enumerable of records</returns>
    IAsyncEnumerable<ConsumeRecord> StreamAsync(string topic, int partition = 0, long offset = 0, CancellationToken cancellationToken = default);

    /// <summary>
    /// Fetch a batch of records from the specified topic
    /// </summary>
    Task<IReadOnlyList<ConsumeRecord>> FetchBatchAsync(string topic, int partition = 0, long offset = 0, int maxBytes = 1024 * 1024, CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents a consumed record
/// </summary>
public record ConsumeRecord(
    long Offset,
    ReadOnlyMemory<byte> Value,
    ReadOnlyMemory<byte>? Key = null,
    DateTimeOffset Timestamp = default);
