namespace Fluvio.Client.Abstractions;

/// <summary>
/// Admin interface for managing Fluvio cluster resources
/// </summary>
public interface IFluvioAdmin : IAsyncDisposable
{
    /// <summary>
    /// Create a new topic
    /// </summary>
    Task CreateTopicAsync(string name, TopicSpec spec, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete a topic
    /// </summary>
    Task DeleteTopicAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// List all topics
    /// </summary>
    Task<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get metadata for a specific topic
    /// </summary>
    Task<TopicMetadata?> GetTopicAsync(string name, CancellationToken cancellationToken = default);
}

/// <summary>
/// Topic specification for creation
/// </summary>
public record TopicSpec(
    int Partitions = 1,
    int ReplicationFactor = 1,
    bool IgnoreRackAssignment = false,
    Dictionary<string, string>? CustomSettings = null);

/// <summary>
/// Topic metadata
/// </summary>
public record TopicMetadata(
    string Name,
    int Partitions,
    int ReplicationFactor,
    TopicStatus Status,
    IReadOnlyList<PartitionMetadata> PartitionMetadata);

/// <summary>
/// Partition metadata
/// </summary>
public record PartitionMetadata(
    int Id,
    int Leader,
    IReadOnlyList<int> Replicas,
    IReadOnlyList<int> Isr);

/// <summary>
/// Topic status
/// </summary>
public enum TopicStatus
{
    /// <summary>
    /// Topic is online and available.
    /// </summary>
    Online,
    /// <summary>
    /// Topic is offline and unavailable.
    /// </summary>
    Offline,
    /// <summary>
    /// Topic is provisioned but not yet online.
    /// </summary>
    Provisioned
}
