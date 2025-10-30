using Fluvio.Client.Abstractions;
using Fluvio.Client.Network;
using Fluvio.Client.Protocol;

namespace Fluvio.Client.Admin;

/// <summary>
/// Fluvio admin implementation for topic management
/// </summary>
internal sealed class FluvioAdmin : IFluvioAdmin
{
    private readonly FluvioConnection _connection;
    private readonly string? _clientId;

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioAdmin"/> class.
    /// </summary>
    /// <param name="connection">The Fluvio connection.</param>
    /// <param name="clientId">Optional client ID.</param>
    public FluvioAdmin(FluvioConnection connection, string? clientId)
    {
        _connection = connection;
        _clientId = clientId;
    }

    /// <summary>
    /// Creates a new topic in the Fluvio cluster.
    /// Uses ObjectApiCreateRequest protocol to communicate with SC (Stream Controller).
    /// </summary>
    /// <param name="name">Topic name.</param>
    /// <param name="spec">Topic specification.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException">Thrown when topic name is invalid</exception>
    public async Task CreateTopicAsync(string name, TopicSpec spec, CancellationToken cancellationToken = default)
    {
        ValidateTopicName(name);
        // Build ObjectApiCreateRequest according to fluvio-sc-schema
        using var writer = new FluvioBinaryWriter();

        // 1. Write type label (String)
        writer.WriteString("Topic");

        // 2. Build CreateRequest<TopicSpec> in a temp buffer
        using var requestWriter = new FluvioBinaryWriter();

        // CommonCreateRequest
        requestWriter.WriteString(name);
        requestWriter.WriteBool(false); // dry_run

        // timeout: Option<u32> (min_version = 7, we use version 25)
        requestWriter.WriteOption<uint>(null, requestWriter.WriteUInt32);

        // TopicSpec - create a computed replica spec from the simple TopicSpec
        var topicSpec = TopicSpecFull.CreateComputed(
            partitions: spec.Partitions,
            replicationFactor: spec.ReplicationFactor,
            ignoreRackAssignment: spec.IgnoreRackAssignment
        );

        // Encode the full TopicSpec using the ADT
        topicSpec.Encode(requestWriter);

        var requestBytes = requestWriter.ToArray();

        // 3. Write buffer length (u32)
        writer.WriteUInt32((uint)requestBytes.Length);

        // 4. Write buffer
        writer._stream.Write(requestBytes);

        var requestBody = writer.ToArray();

        // Send request to SC with AdminCreate API key and COMMON_VERSION
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.AdminCreate,
            25, // COMMON_VERSION
            _clientId,
            requestBody,
            cancellationToken);

        // Parse Status response
        using var reader = new FluvioBinaryReader(responseBytes);

        // Status { name: String, error_code: ErrorCode (i16), error_message: Option<String> }
        var responseName = reader.ReadString(); // topic name
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadOptionalString(); // Option<String>

        if (errorCode != ErrorCode.None)
        {
            throw new FluvioException($"Create topic '{responseName}' failed: {errorCode}" +
                (errorMessage != null ? $" - {errorMessage}" : ""));
        }
    }

    /// <summary>
    /// Deletes a topic from the Fluvio cluster.
    /// Uses ObjectApiDeleteRequest protocol to communicate with SC (Stream Controller).
    /// </summary>
    /// <param name="name">Topic name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException">Thrown when topic name is invalid</exception>
    public async Task DeleteTopicAsync(string name, CancellationToken cancellationToken = default)
    {
        ValidateTopicName(name);
        // Build ObjectApiDeleteRequest according to fluvio-sc-schema
        using var writer = new FluvioBinaryWriter();

        // 1. Write type label (String)
        writer.WriteString("Topic");

        // 2. Build DeleteRequest<TopicSpec> in a temp buffer
        using var requestWriter = new FluvioBinaryWriter();

        // DeleteRequest { key: String, force: bool (min_version=13) }
        requestWriter.WriteString(name); // key (topic name)
        requestWriter.WriteBool(false);  // force (min_version = 13, always false for now)

        var requestBytes = requestWriter.ToArray();

        // 3. Write buffer length (u32)
        writer.WriteUInt32((uint)requestBytes.Length);

        // 4. Write buffer
        writer._stream.Write(requestBytes);

        var requestBody = writer.ToArray();

        // Send request to SC with AdminDelete API key and COMMON_VERSION
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.AdminDelete,
            25, // COMMON_VERSION
            _clientId,
            requestBody,
            cancellationToken);

        // Parse Status response
        using var reader = new FluvioBinaryReader(responseBytes);

        // Status { name: String, error_code: ErrorCode (i16), error_message: Option<String> }
        var topicName = reader.ReadString(); // topic name
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadOptionalString(); // Option<String>

        if (errorCode != ErrorCode.None)
        {
            throw new FluvioException($"Delete topic '{topicName}' failed: {errorCode}" +
                (errorMessage != null ? $" - {errorMessage}" : ""));
        }
    }

    /// <summary>
    /// Lists all topics in the Fluvio cluster.
    /// Uses ObjectApiListRequest protocol to communicate with SC (Stream Controller).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of topic metadata.</returns>
    public async Task<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        // Build ObjectApiListRequest according to fluvio-sc-schema
        using var writer = new FluvioBinaryWriter();

        // 1. Write type label (String)
        writer.WriteString("Topic");

        // 2. Build ListRequest<TopicSpec> in a temp buffer
        using var requestWriter = new FluvioBinaryWriter();

        // ListRequest { name_filters: ListFilters, summary: bool, system: bool }

        // name_filters: Vec<ListFilter> - empty vec means list all
        requestWriter.WriteInt32(0); // filters count = 0 (list all topics)

        // summary: bool (min_version = 10)
        requestWriter.WriteBool(false); // return full metadata, not summary

        // system: bool (min_version = 13)
        requestWriter.WriteBool(false); // don't filter for system topics only

        var requestBytes = requestWriter.ToArray();

        // 3. Write buffer length (u32)
        writer.WriteUInt32((uint)requestBytes.Length);

        // 4. Write buffer
        writer._stream.Write(requestBytes);

        var requestBody = writer.ToArray();

        // Send request to SC with AdminList API key and COMMON_VERSION
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.AdminList,
            25, // COMMON_VERSION
            _clientId,
            requestBody,
            cancellationToken);

        // Parse ObjectApiListResponse
        using var reader = new FluvioBinaryReader(responseBytes);

        // Debug: Log response size

        // 1. Read type label
        var typeLabel = reader.ReadString();
        if (typeLabel != "Topic")
        {
            throw new FluvioException($"Unexpected type label in ListTopics response: {typeLabel}");
        }

        // 2. Read buffer length
        var bufferLength = reader.ReadUInt32();

        // 3. Read ListResponse<TopicSpec> { inner: Vec<Metadata<TopicSpec>> }
        var topicCount = reader.ReadInt32();

        var topics = new List<TopicMetadata>();

        for (int i = 0; i < topicCount; i++)
        {
            // Each Metadata<TopicSpec> contains:
            // - name: String
            // - spec: TopicSpec
            // - status: TopicStatus

            var topicName = reader.ReadString() ?? "";

            // Decode TopicSpec using the ADT
            var spec = TopicSpecFull.Decode(reader);

            // Extract basic info from the spec
            var partitions = spec.Replicas.GetPartitionCount();
            var replicationFactor = spec.Replicas.GetReplicationFactor() ?? 1;

            // Decode TopicStatus
            var status = TopicStatusModel.Decode(reader);

            var topic = new TopicMetadata(
                Name: topicName,
                Partitions: partitions,
                ReplicationFactor: replicationFactor,
                Status: status.Resolution.ToApiStatus(),
                PartitionMetadata: new List<PartitionMetadata>() // Note: Partition metadata extraction deferred - replica_map provides basic info
            );

            topics.Add(topic);
        }

        return topics;
    }

    /// <summary>
    /// Gets metadata for a specific topic.
    /// </summary>
    /// <param name="name">Topic name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Topic metadata or null if not found.</returns>
    public async Task<TopicMetadata?> GetTopicAsync(string name, CancellationToken cancellationToken = default)
    {
        var allTopics = await ListTopicsAsync(cancellationToken);
        return allTopics.FirstOrDefault(t => t.Name == name);
    }

    private TopicMetadata ReadTopicMetadata(FluvioBinaryReader reader)
    {
        var name = reader.ReadString() ?? "";
        var partitionCount = reader.ReadInt32();
        var replicationFactor = reader.ReadInt32();
        var status = (TopicStatus)reader.ReadInt8();

        var partitions = new List<PartitionMetadata>(partitionCount);
        for (int i = 0; i < partitionCount; i++)
        {
            var partitionId = reader.ReadInt32();
            var leader = reader.ReadInt32();

            var replicaCount = reader.ReadInt32();
            var replicas = new List<int>(replicaCount);
            for (int j = 0; j < replicaCount; j++)
            {
                replicas.Add(reader.ReadInt32());
            }

            var isrCount = reader.ReadInt32();
            var isr = new List<int>(isrCount);
            for (int j = 0; j < isrCount; j++)
            {
                isr.Add(reader.ReadInt32());
            }

            partitions.Add(new PartitionMetadata(partitionId, leader, replicas, isr));
        }

        return new TopicMetadata(name, partitionCount, replicationFactor, status, partitions);
    }

    /// <summary>
    /// Validates a topic name according to Fluvio rules.
    /// Topic names must:
    /// - Be 63 characters or less
    /// - Contain only lowercase alphanumeric characters or '-'
    /// - Not start or end with '-'
    /// </summary>
    /// <param name="name">Topic name to validate</param>
    /// <exception cref="ArgumentException">Thrown when topic name is invalid</exception>
    private static void ValidateTopicName(string name)
    {
        const int MaxResourceNameLen = 63;

        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Topic name cannot be null or empty", nameof(name));
        }

        if (name.Length > MaxResourceNameLen)
        {
            throw new ArgumentException(
                $"Invalid topic name: '{name}'. Name exceeds max characters allowed {MaxResourceNameLen}",
                nameof(name));
        }

        if (name.StartsWith('-') || name.EndsWith('-'))
        {
            throw new ArgumentException(
                $"Invalid topic name: '{name}'. Name cannot start or end with '-'",
                nameof(name));
        }

        foreach (char ch in name)
        {
            if (!char.IsAsciiLetterLower(ch) && !char.IsAsciiDigit(ch) && ch != '-')
            {
                throw new ArgumentException(
                    $"Invalid topic name: '{name}'. Name can only contain lowercase alphanumeric characters or '-'",
                    nameof(name));
            }
        }
    }

    /// <summary>
    /// Disposes the admin instance. (No-op, does not own connection.)
    /// </summary>
    public ValueTask DisposeAsync()
    {
        // Admin doesn't own the connection, so nothing to dispose
        return ValueTask.CompletedTask;
    }
}
