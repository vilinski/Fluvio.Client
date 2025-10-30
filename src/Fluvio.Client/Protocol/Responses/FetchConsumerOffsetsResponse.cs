using Fluvio.Client.Protocol.Requests;

namespace Fluvio.Client.Protocol.Responses;

/// <summary>
/// Response containing consumer offset information.
/// API Key: 1008 (FetchConsumerOffsets)
/// </summary>
internal sealed class FetchConsumerOffsetsResponse
{
    /// <summary>
    /// Error code (0 = success).
    /// </summary>
    public ErrorCode ErrorCode { get; set; }

    /// <summary>
    /// List of consumer offsets matching the filter.
    /// </summary>
    public List<ConsumerOffsetInfo> Consumers { get; set; } = new();

    /// <summary>
    /// Reads the response from the binary reader.
    /// </summary>
    public static FetchConsumerOffsetsResponse ReadFrom(FluvioBinaryReader reader)
    {
        var response = new FetchConsumerOffsetsResponse();

        // Read error code
        response.ErrorCode = (ErrorCode)reader.ReadInt16();

        // Read consumers array (Vec<ConsumerOffset>)
        var count = reader.ReadVarInt();
        response.Consumers = new List<ConsumerOffsetInfo>(count);

        for (int i = 0; i < count; i++)
        {
            response.Consumers.Add(ConsumerOffsetInfo.ReadFrom(reader));
        }

        return response;
    }
}

/// <summary>
/// Information about a consumer's offset for a specific topic/partition.
/// </summary>
internal sealed class ConsumerOffsetInfo
{
    /// <summary>
    /// Consumer ID (unique identifier for the consumer).
    /// </summary>
    public required string ConsumerId { get; set; }

    /// <summary>
    /// Replica key (topic + partition).
    /// </summary>
    public required ReplicaKey ReplicaId { get; set; }

    /// <summary>
    /// Last committed offset.
    /// </summary>
    public long Offset { get; set; }

    /// <summary>
    /// Timestamp when offset was last modified (UTC seconds).
    /// </summary>
    public long ModifiedTime { get; set; }

    /// <summary>
    /// Gets the modified time as a DateTimeOffset.
    /// </summary>
    public DateTimeOffset ModifiedTimeUtc => DateTimeOffset.FromUnixTimeSeconds(ModifiedTime);

    /// <summary>
    /// Reads consumer offset info from the binary reader.
    /// </summary>
    public static ConsumerOffsetInfo ReadFrom(FluvioBinaryReader reader)
    {
        return new ConsumerOffsetInfo
        {
            ConsumerId = reader.ReadString() ?? throw new InvalidDataException("ConsumerId cannot be null"),
            ReplicaId = ReplicaKey.ReadFrom(reader),
            Offset = reader.ReadInt64(),
            ModifiedTime = reader.ReadInt64()
        };
    }
}
