namespace Fluvio.Client.Protocol;

/// <summary>
/// Fluvio API keys (based on Kafka-inspired protocol)
/// </summary>
internal enum ApiKey : short
{
    // Metadata APIs
    ApiVersions = 18,
    Metadata = 3,

    // Producer APIs
    Produce = 0,

    // Consumer APIs
    Fetch = 1,
    ListOffsets = 2,

    // Admin APIs
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
