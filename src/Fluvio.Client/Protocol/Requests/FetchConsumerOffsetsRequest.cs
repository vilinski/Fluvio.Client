namespace Fluvio.Client.Protocol.Requests;

/// <summary>
/// Request to fetch stored consumer offsets from the cluster.
/// API Key: 1008 (FetchConsumerOffsets)
/// </summary>
internal sealed class FetchConsumerOffsetsRequest
{
    /// <summary>
    /// Optional filter to narrow down results.
    /// If null, returns all consumer offsets.
    /// </summary>
    public FilterOptions? Filter { get; set; }

    /// <summary>
    /// Writes the request to the binary writer.
    /// </summary>
    public void WriteTo(FluvioBinaryWriter writer)
    {
        // Option<FilterOptions>
        if (Filter == null)
        {
            writer.WriteInt8(0); // None
        }
        else
        {
            writer.WriteInt8(1); // Some
            Filter.WriteTo(writer);
        }
    }
}

/// <summary>
/// Filter options for fetching consumer offsets.
/// Allows filtering by topic/partition and/or consumer ID.
/// </summary>
internal sealed class FilterOptions
{
    /// <summary>
    /// Optional replica ID (topic + partition) to filter by.
    /// </summary>
    public ReplicaKey? ReplicaId { get; set; }

    /// <summary>
    /// Optional consumer ID to filter by.
    /// </summary>
    public string? ConsumerId { get; set; }

    /// <summary>
    /// Writes the filter options to the binary writer.
    /// </summary>
    public void WriteTo(FluvioBinaryWriter writer)
    {
        // Option<ReplicaKey>
        if (ReplicaId == null)
        {
            writer.WriteInt8(0); // None
        }
        else
        {
            writer.WriteInt8(1); // Some
            ReplicaId.WriteTo(writer);
        }

        // Option<String> consumer_id
        if (string.IsNullOrEmpty(ConsumerId))
        {
            writer.WriteInt8(0); // None
        }
        else
        {
            writer.WriteInt8(1); // Some
            writer.WriteString(ConsumerId);
        }
    }
}

/// <summary>
/// Replica key identifying a specific topic partition.
/// </summary>
internal sealed class ReplicaKey
{
    /// <summary>
    /// Topic name.
    /// </summary>
    public required string Topic { get; set; }

    /// <summary>
    /// Partition index.
    /// </summary>
    public int Partition { get; set; }

    /// <summary>
    /// Writes the replica key to the binary writer.
    /// </summary>
    public void WriteTo(FluvioBinaryWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
    }

    /// <summary>
    /// Reads a replica key from the binary reader.
    /// </summary>
    public static ReplicaKey ReadFrom(FluvioBinaryReader reader)
    {
        return new ReplicaKey
        {
            Topic = reader.ReadString() ?? throw new InvalidDataException("Topic cannot be null"),
            Partition = reader.ReadInt32()
        };
    }
}
