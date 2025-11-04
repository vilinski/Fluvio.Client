namespace Fluvio.Client.Abstractions;

/// <summary>
/// Consumer interface for receiving records from Fluvio topics
/// </summary>
public interface IFluvioConsumer : IAsyncDisposable
{
    /// <summary>
    /// Stream records from the specified topic starting at the given offset.
    /// If offset is null, uses OffsetResetStrategy from ConsumerOptions to determine starting point.
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="offset">Starting offset (null to use offset reset strategy)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Async enumerable of records</returns>
    IAsyncEnumerable<ConsumeRecord> StreamAsync(string topic, int partition = 0, long? offset = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Fetch a batch of records from the specified topic
    /// </summary>
    Task<IReadOnlyList<ConsumeRecord>> FetchBatchAsync(string topic, int partition = 0, long offset = 0, int maxBytes = 1024 * 1024, CancellationToken cancellationToken = default);

    /// <summary>
    /// Fetches the last committed offset for a consumer from the cluster.
    /// Returns null if no offset has been committed yet.
    /// </summary>
    /// <param name="consumerId">Consumer ID</param>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Last committed offset, or null if none exists</returns>
    Task<long?> FetchLastOffsetAsync(string consumerId, string topic, int partition = 0, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits (updates) the consumer offset for a specific topic/partition.
    /// Note: Requires an active StreamFetch session ID. This is currently not supported
    /// without an active stream. For MVP, consider tracking offsets client-side.
    /// </summary>
    /// <param name="consumerId">Consumer ID</param>
    /// <param name="topic">Topic name</param>
    /// <param name="partition">Partition number</param>
    /// <param name="offset">Offset to commit</param>
    /// <param name="sessionId">Stream session ID from active StreamFetch</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task CommitOffsetAsync(string consumerId, string topic, int partition, long offset, uint sessionId, CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents a consumed record
/// </summary>
public record ConsumeRecord(
    long Offset,
    ReadOnlyMemory<byte> Value,
    ReadOnlyMemory<byte>? Key = null,
    DateTimeOffset Timestamp = default);
