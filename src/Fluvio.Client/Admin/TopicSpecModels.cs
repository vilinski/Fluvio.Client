using Fluvio.Client.Protocol;

namespace Fluvio.Client.Admin;

/// <summary>
/// Replica specification - algebraic data type with three variants
/// Based on fluvio-controlplane-metadata ReplicaSpec
/// </summary>
internal abstract class ReplicaSpec
{
    private ReplicaSpec() { } // Sealed hierarchy

    /// <summary>
    /// Computed replicas - Fluvio automatically assigns partitions
    /// Tag: 1
    /// </summary>
    public sealed class Computed(int partitions, int replicationFactor, bool ignoreRackAssignment = false)
        : ReplicaSpec
    {
        public int Partitions { get; } = partitions;
        public int ReplicationFactor { get; } = replicationFactor;
        public bool IgnoreRackAssignment { get; } = ignoreRackAssignment;

        public override void Encode(FluvioBinaryWriter writer)
        {
            writer.WriteInt8(1); // Tag for Computed variant
            writer.WriteInt32(Partitions);
            writer.WriteInt32(ReplicationFactor);
            writer.WriteBool(IgnoreRackAssignment);
        }

        public override string ToString() => $"Computed(partitions: {Partitions}, replication: {ReplicationFactor})";
    }

    /// <summary>
    /// Assigned replicas - manual partition-to-SPU mapping
    /// Tag: 0
    /// </summary>
    public sealed class Assigned(List<PartitionMap> partitionMaps) : ReplicaSpec
    {
        public List<PartitionMap> PartitionMaps { get; } = partitionMaps;

        public override void Encode(FluvioBinaryWriter writer)
        {
            writer.WriteInt8(0); // Tag for Assigned variant
            writer.WriteInt32(PartitionMaps.Count);
            foreach (var map in PartitionMaps)
            {
                writer.WriteInt32(map.PartitionId);
                writer.WriteInt32(map.Replicas.Count);
                foreach (var replicaId in map.Replicas)
                {
                    writer.WriteInt32(replicaId);
                }
            }
        }

        public override string ToString() => $"Assigned({PartitionMaps.Count} partitions)";
    }

    /// <summary>
    /// Mirror replicas - for topic mirroring (min_version = 14)
    /// Tag: 2
    /// </summary>
    public sealed class Mirror : ReplicaSpec
    {
        // Simplified for now - full MirrorConfig is complex
        public Mirror()
        {
        }

        public override void Encode(FluvioBinaryWriter writer)
        {
            writer.WriteInt8(2); // Tag for Mirror variant
            // Note: Mirror replica support deferred - not commonly used, will implement when needed
            throw new NotImplementedException("Mirror replica encoding not yet implemented");
        }

        public override string ToString() => "Mirror";
    }

    /// <summary>
    /// Encode this ReplicaSpec variant to binary
    /// </summary>
    public abstract void Encode(FluvioBinaryWriter writer);

    /// <summary>
    /// Decode ReplicaSpec from binary
    /// </summary>
    public static ReplicaSpec Decode(FluvioBinaryReader reader)
    {
        var tag = reader.ReadInt8();
        return tag switch
        {
            0 => DecodeAssigned(reader),
            1 => DecodeComputed(reader),
            2 => DecodeMirror(reader),
            _ => throw new InvalidDataException($"Unknown ReplicaSpec tag: {tag}")
        };
    }

    private static Computed DecodeComputed(FluvioBinaryReader reader)
    {
        var partitions = reader.ReadInt32();
        var replicationFactor = reader.ReadInt32();
        var ignoreRackAssignment = reader.ReadBool();
        return new Computed(partitions, replicationFactor, ignoreRackAssignment);
    }

    private static Assigned DecodeAssigned(FluvioBinaryReader reader)
    {
        var count = reader.ReadInt32();
        var maps = new List<PartitionMap>(count);
        for (var i = 0; i < count; i++)
        {
            var partitionId = reader.ReadInt32();
            var replicaCount = reader.ReadInt32();
            var replicas = new List<int>(replicaCount);
            for (var j = 0; j < replicaCount; j++)
            {
                replicas.Add(reader.ReadInt32());
            }
            maps.Add(new PartitionMap(partitionId, replicas));
        }
        return new Assigned(maps);
    }

    private static Mirror DecodeMirror(FluvioBinaryReader reader)
    {
        // Note: Mirror replica support deferred - not commonly used, will implement when needed
        throw new NotImplementedException("Mirror replica decoding not yet implemented");
    }

    /// <summary>
    /// Pattern matching helper - visit the variant
    /// </summary>
    public T Match<T>(
        Func<Computed, T> computed,
        Func<Assigned, T> assigned,
        Func<Mirror, T> mirror)
    {
        return this switch
        {
            Computed c => computed(c),
            Assigned a => assigned(a),
            Mirror m => mirror(m),
            _ => throw new InvalidOperationException("Unknown ReplicaSpec variant")
        };
    }

    /// <summary>
    /// Get partition count regardless of variant
    /// </summary>
    public int GetPartitionCount()
    {
        return Match(
            computed: c => c.Partitions,
            assigned: a => a.PartitionMaps.Count,
            mirror: _ => throw new NotImplementedException("Mirror partition count not implemented")
        );
    }

    /// <summary>
    /// Get replication factor if available
    /// </summary>
    public int? GetReplicationFactor()
    {
        return Match(
            computed: c => (int?)c.ReplicationFactor,
            assigned: a => a.PartitionMaps.Count > 0 ? (int?)a.PartitionMaps[0].Replicas.Count : null,
            mirror: _ => (int?)null
        );
    }
}

/// <summary>
/// Maps a partition ID to its replica SPU IDs
/// </summary>
internal record PartitionMap(int PartitionId, List<int> Replicas);

/// <summary>
/// Compression algorithm enum (min_version = 6)
/// Based on Rust: None=0, Gzip=1, Snappy=2, Lz4=3, Any=4 (default), Zstd=5
/// </summary>
internal enum CompressionAlgorithm : byte
{
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Any = 4,    // Default value in Rust
    Zstd = 5
}

/// <summary>
/// Cleanup policy - Rust enum with data
/// enum CleanupPolicy { Segment(SegmentBasedPolicy) }
/// </summary>
internal class CleanupPolicy(SegmentBasedPolicy segmentPolicy)
{
    public SegmentBasedPolicy SegmentPolicy { get; set; } = segmentPolicy;

    public void Encode(FluvioBinaryWriter writer)
    {
        writer.WriteInt8(0); // Tag for Segment variant
        writer.WriteUInt32(SegmentPolicy.TimeInSeconds);
    }

    public static CleanupPolicy Decode(FluvioBinaryReader reader)
    {
        var tag = reader.ReadInt8();
        if (tag != 0)
        {
            throw new InvalidDataException($"Unknown CleanupPolicy tag: {tag}");
        }
        var timeInSeconds = reader.ReadUInt32();
        return new CleanupPolicy(new SegmentBasedPolicy(timeInSeconds));
    }
}

/// <summary>
/// Segment-based cleanup policy
/// </summary>
internal class SegmentBasedPolicy(uint timeInSeconds)
{
    public uint TimeInSeconds { get; set; } = timeInSeconds;
}

/// <summary>
/// Full TopicSpec with all optional fields
/// Based on fluvio-controlplane-metadata TopicSpec
/// </summary>
internal class TopicSpecFull(ReplicaSpec replicas)
{
    public ReplicaSpec Replicas { get; set; } = replicas;
    public CleanupPolicy? CleanupPolicy { get; set; } // min_version = 3
    public TopicStorageConfig? Storage { get; set; } // min_version = 4
    public CompressionAlgorithm CompressionType { get; set; } = CompressionAlgorithm.None; // min_version = 6
    public DeduplicationConfig? Deduplication { get; set; } // min_version = 12
    public bool System { get; set; } // min_version = 13

    /// <summary>
    /// Create a simple computed topic spec
    /// </summary>
    public static TopicSpecFull CreateComputed(int partitions, int replicationFactor, bool ignoreRackAssignment = false)
    {
        return new TopicSpecFull(new ReplicaSpec.Computed(partitions, replicationFactor, ignoreRackAssignment));
    }

    /// <summary>
    /// Encode TopicSpec to binary (version 25)
    /// </summary>
    public void Encode(FluvioBinaryWriter writer)
    {
        // 1. Replicas (required)
        Replicas.Encode(writer);

        // 2. cleanup_policy: Option (min_version = 3)
        writer.WriteOptionRef(CleanupPolicy, p => p.Encode(writer));

        // 3. storage: Option (min_version = 4)
        writer.WriteOptionRef(Storage, s => s.Encode(writer));

        // 4. compression_type: CompressionAlgorithm (min_version = 6)
        writer.WriteInt8((sbyte)CompressionType);

        // 5. deduplication: Option (min_version = 12)
        writer.WriteOptionRef(Deduplication, d => d.Encode(writer));

        // 6. system: bool (min_version = 13)
        writer.WriteBool(System);
    }

    /// <summary>
    /// Decode TopicSpec from binary (version 25)
    /// </summary>
    public static TopicSpecFull Decode(FluvioBinaryReader reader)
    {

        // 1. Replicas
        var replicas = ReplicaSpec.Decode(reader);

        var spec = new TopicSpecFull(replicas);

        // 2. cleanup_policy: Option
        spec.CleanupPolicy = reader.ReadOptionRef(() => CleanupPolicy.Decode(reader));

        // 3. storage: Option
        spec.Storage = reader.ReadOptionRef(() => TopicStorageConfig.Decode(reader));

        // 4. compression_type
        spec.CompressionType = (CompressionAlgorithm)reader.ReadInt8();

        // 5. deduplication: Option
        spec.Deduplication = reader.ReadOptionRef(() => DeduplicationConfig.Decode(reader));

        // 6. system
        spec.System = reader.ReadBool();

        return spec;
    }
}

/// <summary>
/// Topic storage configuration (min_version = 4)
/// </summary>
internal class TopicStorageConfig
{
    public uint? SegmentSize { get; set; }
    public uint? MaxPartitionSize { get; set; }

    public void Encode(FluvioBinaryWriter writer)
    {
        writer.WriteOption(SegmentSize, writer.WriteUInt32);
        writer.WriteOption(MaxPartitionSize, writer.WriteUInt32);
    }

    public static TopicStorageConfig Decode(FluvioBinaryReader reader)
    {
        return new TopicStorageConfig
        {
            SegmentSize = reader.ReadOption(reader.ReadUInt32),
            MaxPartitionSize = reader.ReadOption(reader.ReadUInt32)
        };
    }
}

/// <summary>
/// Deduplication configuration (min_version = 12)
/// </summary>
internal class DeduplicationConfig
{
    public List<DeduplicationBound> Bounds { get; set; } = [];
    public uint? Filter { get; set; }

    public void Encode(FluvioBinaryWriter writer)
    {
        writer.WriteInt32(Bounds.Count);
        foreach (var bound in Bounds)
        {
            bound.Encode(writer);
        }
        writer.WriteOption(Filter, writer.WriteUInt32);
    }

    public static DeduplicationConfig Decode(FluvioBinaryReader reader)
    {
        var boundCount = reader.ReadInt32();
        var bounds = new List<DeduplicationBound>(boundCount);
        for (var i = 0; i < boundCount; i++)
        {
            bounds.Add(DeduplicationBound.Decode(reader));
        }

        return new DeduplicationConfig
        {
            Bounds = bounds,
            Filter = reader.ReadOption(reader.ReadUInt32)
        };
    }
}

/// <summary>
/// Deduplication bound (simplified)
/// </summary>
internal class DeduplicationBound
{
    public int Count { get; set; }
    public long Age { get; set; }

    public void Encode(FluvioBinaryWriter writer)
    {
        writer.WriteInt32(Count);
        writer.WriteInt64(Age);
    }

    public static DeduplicationBound Decode(FluvioBinaryReader reader)
    {
        return new DeduplicationBound
        {
            Count = reader.ReadInt32(),
            Age = reader.ReadInt64()
        };
    }
}

/// <summary>
/// Topic resolution status enum
/// Based on fluvio-controlplane-metadata TopicResolution
/// </summary>
internal enum TopicResolution : byte
{
    Init = 0,                   // Initializing - starting state
    Pending = 1,                // Has valid config, ready for replica mapping
    InsufficientResources = 2,  // Replica map cannot be created due to lack of capacity
    InvalidConfig = 3,          // Invalid configuration
    Provisioned = 4,            // All partitions have been provisioned
    Deleting = 5                // Process of being deleted
}

/// <summary>
/// Extension methods for TopicResolution
/// </summary>
internal static class TopicResolutionExtensions
{
    public static Fluvio.Client.Abstractions.TopicStatus ToApiStatus(this TopicResolution resolution)
    {
        return resolution switch
        {
            TopicResolution.Init => Fluvio.Client.Abstractions.TopicStatus.Offline,
            TopicResolution.Pending => Fluvio.Client.Abstractions.TopicStatus.Offline,
            TopicResolution.InsufficientResources => Fluvio.Client.Abstractions.TopicStatus.Offline,
            TopicResolution.InvalidConfig => Fluvio.Client.Abstractions.TopicStatus.Offline,
            TopicResolution.Provisioned => Fluvio.Client.Abstractions.TopicStatus.Provisioned,
            TopicResolution.Deleting => Fluvio.Client.Abstractions.TopicStatus.Offline,
            _ => Fluvio.Client.Abstractions.TopicStatus.Offline
        };
    }
}

/// <summary>
/// Topic status model
/// Based on fluvio-controlplane-metadata TopicStatus
/// </summary>
internal class TopicStatusModel
{
    public TopicResolution Resolution { get; set; }
    public Dictionary<int, List<int>> ReplicaMap { get; set; } = new(); // PartitionId -> List<SpuId>
    public Dictionary<int, object> MirrorMap { get; set; } = new(); // min_version = 14, simplified for now
    public string Reason { get; set; } = "";

    public void Encode(FluvioBinaryWriter writer)
    {
        // resolution: TopicResolution (enum with tag)
        writer.WriteInt8((sbyte)Resolution);

        // replica_map: BTreeMap<PartitionId, Vec<SpuId>>
        writer.WriteInt32(ReplicaMap.Count);
        foreach (var (partitionId, replicas) in ReplicaMap)
        {
            writer.WriteInt32(partitionId);
            writer.WriteInt32(replicas.Count);
            foreach (var spuId in replicas)
            {
                writer.WriteInt32(spuId);
            }
        }

        // mirror_map: MirrorMap (min_version = 14) - simplified
        writer.WriteInt32(MirrorMap.Count);
        // Skip complex mirror map for now

        // reason: String
        writer.WriteString(Reason);
    }

    public static TopicStatusModel Decode(FluvioBinaryReader reader)
    {
        var status = new TopicStatusModel();

        // resolution: TopicResolution
        status.Resolution = (TopicResolution)reader.ReadInt8();

        // replica_map: BTreeMap<PartitionId, Vec<SpuId>>
        // IMPORTANT: BTreeMap count is u16 (2 bytes), not i32 (4 bytes)!
        var replicaMapCount = reader.ReadUInt16();

        for (var i = 0; i < replicaMapCount; i++)
        {
            var partitionId = reader.ReadInt32();
            var replicaCount = reader.ReadInt32();

            var replicas = new List<int>(replicaCount);
            for (var j = 0; j < replicaCount; j++)
            {
                replicas.Add(reader.ReadInt32());
            }
            status.ReplicaMap[partitionId] = replicas;
        }

        // mirror_map: MirrorMap (min_version = 14)
        // IMPORTANT: BTreeMap count is u16 (2 bytes), not i32!
        var mirrorMapCount = reader.ReadUInt16();

        if (mirrorMapCount > 0)
        {
            // Mirror maps are rare and complex - skip for MVP
            throw new NotImplementedException($"Mirror map decoding not yet implemented (count: {mirrorMapCount})");
        }

        // reason: String
        status.Reason = reader.ReadString() ?? "";

        return status;
    }
}
