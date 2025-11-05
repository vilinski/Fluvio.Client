namespace Fluvio.Client.Abstractions;

/// <summary>
/// Interface for partition selection strategy when producing records to multi-partition topics.
/// Partitioners determine which partition a record should be sent to based on the record's key and metadata.
/// </summary>
public interface IPartitioner
{
    /// <summary>
    /// Select a partition for the given record.
    /// </summary>
    /// <param name="topic">Topic name</param>
    /// <param name="key">Optional record key</param>
    /// <param name="value">Record value</param>
    /// <param name="config">Partitioner configuration with available partitions</param>
    /// <returns>Partition index (0-based)</returns>
    int SelectPartition(string topic, ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte> value, PartitionerConfig config);
}

/// <summary>
/// Configuration for partitioners containing partition metadata.
/// </summary>
/// <param name="PartitionCount">Total number of partitions in the topic</param>
/// <param name="AvailablePartitions">List of partition indices that are currently available (optional, defaults to all partitions)</param>
public record PartitionerConfig(
    int PartitionCount,
    IReadOnlyList<int>? AvailablePartitions = null)
{
    /// <summary>
    /// Gets the list of available partition indices.
    /// If not specified, all partitions (0 to PartitionCount-1) are considered available.
    /// </summary>
    public IReadOnlyList<int> AvailablePartitions { get; init; } =
        AvailablePartitions ?? Enumerable.Range(0, PartitionCount).ToList();
}
