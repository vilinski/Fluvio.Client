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

    public FluvioAdmin(FluvioConnection connection, string? clientId)
    {
        _connection = connection;
        _clientId = clientId;
    }

    public async Task CreateTopicAsync(string name, TopicSpec spec, CancellationToken cancellationToken = default)
    {
        // Build create topic request
        using var writer = new FluvioBinaryWriter();

        writer.WriteString(name);
        writer.WriteInt32(spec.Partitions);
        writer.WriteInt32(spec.ReplicationFactor);
        writer.WriteBool(spec.IgnoreRackAssignment);

        // Write custom settings
        var settings = spec.CustomSettings ?? new Dictionary<string, string>();
        writer.WriteInt32(settings.Count);
        foreach (var kvp in settings)
        {
            writer.WriteString(kvp.Key);
            writer.WriteString(kvp.Value);
        }

        var requestBody = writer.ToArray();

        // Send request
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.CreateTopics,
            0, // API version
            _clientId,
            requestBody,
            cancellationToken);

        // Parse response
        using var reader = new FluvioBinaryReader(responseBytes);

        // Read error code
        var errorCode = (ErrorCode)reader.ReadInt16();
        if (errorCode != ErrorCode.None)
        {
            var errorMessage = reader.ReadString() ?? "Unknown error";
            throw new FluvioException($"Create topic failed: {errorCode} - {errorMessage}");
        }
    }

    public async Task DeleteTopicAsync(string name, CancellationToken cancellationToken = default)
    {
        // Build delete topic request
        using var writer = new FluvioBinaryWriter();
        writer.WriteString(name);

        var requestBody = writer.ToArray();

        // Send request
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.DeleteTopics,
            0, // API version
            _clientId,
            requestBody,
            cancellationToken);

        // Parse response
        using var reader = new FluvioBinaryReader(responseBytes);

        // Read error code
        var errorCode = (ErrorCode)reader.ReadInt16();
        if (errorCode != ErrorCode.None)
        {
            var errorMessage = reader.ReadString() ?? "Unknown error";
            throw new FluvioException($"Delete topic failed: {errorCode} - {errorMessage}");
        }
    }

    public async Task<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        // Build metadata request (list all topics)
        using var writer = new FluvioBinaryWriter();
        writer.WriteInt32(-1); // -1 means all topics

        var requestBody = writer.ToArray();

        // Send request
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.Metadata,
            0, // API version
            _clientId,
            requestBody,
            cancellationToken);

        // Parse response
        using var reader = new FluvioBinaryReader(responseBytes);

        // Read error code
        var errorCode = (ErrorCode)reader.ReadInt16();
        if (errorCode != ErrorCode.None)
        {
            var errorMessage = reader.ReadString() ?? "Unknown error";
            throw new FluvioException($"List topics failed: {errorCode} - {errorMessage}");
        }

        // Read topics count
        var topicCount = reader.ReadInt32();
        var topics = new List<TopicMetadata>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topic = ReadTopicMetadata(reader);
            topics.Add(topic);
        }

        return topics;
    }

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

    public ValueTask DisposeAsync()
    {
        // Admin doesn't own the connection, so nothing to dispose
        return ValueTask.CompletedTask;
    }
}
