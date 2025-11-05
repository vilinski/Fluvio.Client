using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Producer;

/// <summary>
/// Partitioner that always selects a specific partition.
/// Matches the Rust Fluvio client's SpecificPartitioner behavior.
///
/// Use this when you want all records to go to a single partition,
/// regardless of keys or other factors.
/// </summary>
internal sealed class SpecificPartitioner : IPartitioner
{
    private readonly int _partition;

    /// <summary>
    /// Initializes a new instance of SpecificPartitioner for the given partition.
    /// </summary>
    /// <param name="partition">The partition index to always use</param>
    public SpecificPartitioner(int partition)
    {
        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partition), "Partition must be non-negative");
        }

        _partition = partition;
    }

    /// <summary>
    /// Always returns the specified partition, regardless of key or value.
    /// </summary>
    public int SelectPartition(
        string topic,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte> value,
        PartitionerConfig config)
    {
        if (config.AvailablePartitions.Count == 0)
        {
            throw new InvalidOperationException("No available partitions");
        }

        // Validate that the specified partition is in the available list
        if (!config.AvailablePartitions.Contains(_partition))
        {
            throw new InvalidOperationException(
                $"Specified partition {_partition} is not available. " +
                $"Available partitions: [{string.Join(", ", config.AvailablePartitions)}]");
        }

        return _partition;
    }
}
