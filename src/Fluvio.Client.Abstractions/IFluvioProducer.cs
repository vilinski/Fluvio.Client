namespace Fluvio.Client.Abstractions;

/// <summary>
/// Producer interface for sending records to Fluvio topics
/// </summary>
public interface IFluvioProducer : IAsyncDisposable
{
    /// <summary>
    /// Send a record to the specified topic
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="value">Record value</param>
    /// <param name="key">Optional record key</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Offset of the produced record</returns>
    Task<long> SendAsync(string topic, ReadOnlyMemory<byte> value, ReadOnlyMemory<byte>? key = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Send a batch of records to the specified topic
    /// </summary>
    Task<IReadOnlyList<long>> SendBatchAsync(string topic, IEnumerable<ProduceRecord> records, CancellationToken cancellationToken = default);

    /// <summary>
    /// Flush any pending records
    /// </summary>
    Task FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the partition count for a topic to enable correct partitioner behavior.
    /// Must be called before producing to multi-partition topics.
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="partitionCount">Number of partitions</param>
    void SetPartitionCount(string topic, int partitionCount);
}

/// <summary>
/// Represents a record to be produced
/// </summary>
public record ProduceRecord(
    ReadOnlyMemory<byte> Value,
    ReadOnlyMemory<byte>? Key = null,
    IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? Headers = null);
