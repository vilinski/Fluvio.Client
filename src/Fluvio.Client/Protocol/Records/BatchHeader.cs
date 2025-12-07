namespace Fluvio.Client.Protocol.Records;

/// <summary>
/// Batch header containing metadata for a record batch
/// Source: fluvio-protocol/src/record/batch.rs
/// </summary>
public record BatchHeader
{
    /// <summary>
    /// Partition leader epoch. Default: -1
    /// </summary>
    public int PartitionLeaderEpoch { get; set; }

    /// <summary>
    /// Magic byte indicating the batch format version. Default: 2
    /// </summary>
    public sbyte Magic { get; set; }

    /// <summary>
    /// CRC32C checksum of the batch data. Computed during encoding.
    /// </summary>
    public uint Crc { get; set; }

    /// <summary>
    /// Batch attributes (compression type, etc.). Default: 0
    /// </summary>
    public short Attributes { get; set; }

    /// <summary>
    /// Last offset delta in this batch. Default: -1
    /// </summary>
    public int LastOffsetDelta { get; set; }

    /// <summary>
    /// Timestamp of the first record. Default: -1 (NO_TIMESTAMP)
    /// </summary>
    public long FirstTimestamp { get; set; }

    /// <summary>
    /// Maximum timestamp in the batch. Default: -1 (NO_TIMESTAMP)
    /// </summary>
    public long MaxTimeStamp { get; set; }

    /// <summary>
    /// Producer ID. Default: -1 (none)
    /// </summary>
    public long ProducerId { get; set; }

    /// <summary>
    /// Producer epoch. Default: -1
    /// </summary>
    public short ProducerEpoch { get; set; }

    /// <summary>
    /// First sequence number. Default: -1
    /// </summary>
    public int FirstSequence { get; set; }

    /// <summary>
    /// Creates a BatchHeader with default values matching Rust implementation
    /// </summary>
    public static BatchHeader Default()
    {
        return new BatchHeader
        {
            PartitionLeaderEpoch = -1,
            Magic = 2,
            Crc = 0,
            Attributes = 0,
            LastOffsetDelta = -1,
            FirstTimestamp = -1,  // NO_TIMESTAMP
            MaxTimeStamp = -1,    // NO_TIMESTAMP
            ProducerId = -1,
            ProducerEpoch = -1,
            FirstSequence = -1
        };
    }

    /// <summary>
    /// Checks if this batch has a schema ID
    /// </summary>
    public bool HasSchema()
    {
        // Check the schema flag bit in attributes
        // Based on Rust: const ATTR_SCHEMA_PRESENT: i16 = 0x10
        return (Attributes & 0x10) != 0;
    }
}
