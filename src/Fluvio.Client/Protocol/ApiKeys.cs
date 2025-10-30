namespace Fluvio.Client.Protocol;

/// <summary>
/// Fluvio API keys (based on Kafka-inspired protocol)
/// </summary>
internal enum ApiKey : short
{

    // Producer APIs (SPU)
    Produce = 0,

    // Consumer APIs (SPU)
    Fetch = 1,
    ListOffsets = 2,
    FetchOffsets = 1002,
    StreamFetch = 1003,
    UpdateOffsets = 1005,
    UpdateConsumerOffset = 1006,
    DeleteConsumerOffset = 1007,
    FetchConsumerOffsets = 1008,

    // Metadata APIs
    Metadata = 3,
    ApiVersions = 18,

    // Admin APIs (SC - Stream Controller)
    // Based on fluvio-sc-schema AdminPublicApiKey
    // Note: AdminList = 1003 is for SC, StreamFetch = 1003 is for SPU (different ports)
    AdminCreate = 1001,
    AdminDelete = 1002,
    AdminList = 1003,
    AdminWatch = 1004,
    AdminMirroring = 1005,
    AdminUpdate = 1006,

    // Legacy Admin APIs (deprecated, keeping for reference)
    CreateTopics = 19,
    DeleteTopics = 20,
    DescribeTopics = 75,

    // Custom Fluvio APIs (using high numbers to avoid conflicts)
    FluvioStream = 1000,
}

/// <summary>
/// Error codes returned by Fluvio
/// </summary>
internal enum ErrorCode : short
{
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidMessageSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    TopicAuthorizationFailed = 29,
}
