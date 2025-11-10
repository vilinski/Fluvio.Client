using System.Diagnostics;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Network;
using Fluvio.Client.Protocol;
using Fluvio.Client.Protocol.Records;
using Fluvio.Client.Telemetry;

namespace Fluvio.Client.Producer;

/// <summary>
/// Fluvio producer implementation
/// </summary>
internal sealed class FluvioProducer : IFluvioProducer
{
    private readonly FluvioConnection _spuConnection;
    private readonly FluvioConnection? _scConnection;
    private readonly ProducerOptions _options;
    private readonly string? _clientId;
    private readonly IPartitioner _partitioner;
    private readonly Dictionary<string, PartitionerConfig> _topicPartitionCache = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioProducer"/> class.
    /// </summary>
    /// <param name="spuConnection">The SPU connection for sending records.</param>
    /// <param name="scConnection">The SC connection for fetching metadata (optional).</param>
    /// <param name="options">Producer options.</param>
    /// <param name="clientId">Optional client ID.</param>
    public FluvioProducer(FluvioConnection spuConnection, FluvioConnection? scConnection, ProducerOptions? options, string? clientId)
    {
        _spuConnection = spuConnection;
        _scConnection = scConnection;
        _options = options ?? new ProducerOptions();
        _clientId = clientId;
        _partitioner = _options.Partitioner ?? new SiphashRoundRobinPartitioner();
    }

    /// <summary>
    /// Sends a record to the specified topic.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="value">Record value.</param>
    /// <param name="key">Optional record key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Offset of the produced record.</returns>
    public async Task<long> SendAsync(string topic, ReadOnlyMemory<byte> value, ReadOnlyMemory<byte>? key = null, CancellationToken cancellationToken = default)
    {
        using var activity = FluvioActivitySource.Instance.StartActivity(
            FluvioActivitySource.Operations.Produce,
            ActivityKind.Producer);

        activity?.SetTag(FluvioActivitySource.Tags.MessagingSystem, "fluvio");
        activity?.SetTag(FluvioActivitySource.Tags.MessagingOperation, "publish");
        activity?.SetTag(FluvioActivitySource.Tags.MessagingDestination, topic);
        activity?.SetTag(FluvioActivitySource.Tags.Topic, topic);
        activity?.SetTag(FluvioActivitySource.Tags.RecordCount, 1);

        try
        {
            var offsets = await SendBatchAsync(topic, [new ProduceRecord(value, key)], cancellationToken);
            var offset = offsets[0];

            activity?.SetTag(FluvioActivitySource.Tags.Offset, offset);
            activity?.SetStatus(ActivityStatusCode.Ok);

            return offset;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Sends a batch of records to the specified topic.
    /// Uses partitioner to distribute records across partitions.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="records">Records to produce.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of offsets for produced records.</returns>
    public async Task<IReadOnlyList<long>> SendBatchAsync(string topic, IEnumerable<ProduceRecord> records, CancellationToken cancellationToken = default)
    {
        var recordList = records.ToList();

        // Get partition configuration for this topic (with automatic discovery)
        var partitionConfig = await GetPartitionConfigAsync(topic, cancellationToken);

        // Group records by partition using partitioner
        var partitionedRecords = new Dictionary<int, List<ProduceRecord>>();
        foreach (var record in recordList)
        {
            var partition = _partitioner.SelectPartition(topic, record.Key, record.Value, partitionConfig);
            if (!partitionedRecords.ContainsKey(partition))
            {
                partitionedRecords[partition] = [];
            }
            partitionedRecords[partition].Add(record);
        }

        // Send records to each partition and collect offsets
        var allOffsets = new List<long>(recordList.Count);

        foreach (var (partition, partitionRecords) in partitionedRecords.OrderBy(kv => kv.Key))
        {
            var partitionOffsets = await SendToPartitionAsync(topic, partition, partitionRecords, cancellationToken);
            allOffsets.AddRange(partitionOffsets);
        }

        return allOffsets;
    }

    /// <summary>
    /// Sends records to a specific partition.
    /// </summary>
    private async Task<IReadOnlyList<long>> SendToPartitionAsync(
        string topic,
        int partition,
        List<ProduceRecord> records,
        CancellationToken cancellationToken)
    {
        using var activity = FluvioActivitySource.Instance.StartActivity(
            $"{FluvioActivitySource.Operations.Produce}.partition",
            ActivityKind.Producer);

        activity?.SetTag(FluvioActivitySource.Tags.Topic, topic);
        activity?.SetTag(FluvioActivitySource.Tags.Partition, partition);
        activity?.SetTag(FluvioActivitySource.Tags.RecordCount, records.Count);
        activity?.SetTag(FluvioActivitySource.Tags.SmartModuleCount, _options.SmartModules?.Count ?? 0);

        try
        {
            // Build produce request according to fluvio-spu-schema ProduceRequest structure
            using var writer = new FluvioBinaryWriter();

        // 1. transactional_id: Option<String>
        // Option encoding: 0x00 for None, 0x01 + value for Some
        writer.WriteBool(false); // None - no transactional_id

        // 2. isolation: i16 (1 = ReadUncommitted, -1 = ReadCommitted)
        writer.WriteInt16(1); // ReadUncommitted

        // 3. timeout: i32 (milliseconds)
        writer.WriteInt32((int)_options.Timeout.TotalMilliseconds);

        // 4. topics: Vec<TopicProduceData>
        writer.WriteInt32(1); // One topic

        // TopicProduceData
        writer.WriteString(topic); // topic name

        // partitions: Vec<PartitionProduceData>
        writer.WriteInt32(1); // One partition

        // PartitionProduceData
        writer.WriteInt32(partition); // partition_index

        // records: RecordSet (i32 length + batches)
        WriteRecordSet(writer, records);

        // 5. smartmodules: Vec<SmartModuleInvocation>
        writer.EncodeSmartModules(_options.SmartModules, version: 25);

        var requestBody = writer.ToArray();

        // Send request with timeout
        // Create a timeout cancellation token based on the producer timeout
        using var timeoutCts = new CancellationTokenSource(_options.Timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        var responseBytes = await _spuConnection.SendRequestAsync(
            ApiKey.Produce,
            25, // API version 25 (COMMON_VERSION in Fluvio)
            _clientId,
            requestBody,
            linkedCts.Token);

        // Parse response
        using var reader = new FluvioBinaryReader(responseBytes);

        // ProduceResponse structure:
        // - topics: Vec<TopicProduceResponse>
        var topicCount = reader.ReadInt32();
        if (topicCount == 0)
        {
            throw new FluvioException("No topics in produce response");
        }

        // TopicProduceResponse
        var responseTopic = reader.ReadString();

        // partitions: Vec<PartitionProduceResponse>
        var partitionCount = reader.ReadInt32();
        if (partitionCount == 0)
        {
            throw new FluvioException("No partitions in produce response");
        }

        // PartitionProduceResponse
        var partitionIndex = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        if (errorCode != ErrorCode.None)
        {
            throw new FluvioException($"Produce failed with error code: {errorCode}");
        }

            var baseOffset = reader.ReadInt64();

            // Generate offsets for each record
            var offsets = new List<long>(records.Count);
            for (var i = 0; i < records.Count; i++)
            {
                offsets.Add(baseOffset + i);
            }

            activity?.SetTag(FluvioActivitySource.Tags.Offset, baseOffset);
            activity?.SetStatus(ActivityStatusCode.Ok);

            return offsets;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Gets partition configuration for a topic (with caching and automatic discovery).
    /// If not cached, attempts to fetch from SC metadata if available, otherwise defaults to 1 partition.
    /// </summary>
    private async Task<PartitionerConfig> GetPartitionConfigAsync(string topic, CancellationToken cancellationToken)
    {
        // Check cache first
        if (_topicPartitionCache.TryGetValue(topic, out var cached))
        {
            return cached;
        }

        // If SC connection is available, fetch partition count from metadata
        if (_scConnection != null)
        {
            try
            {
                var partitionCount = await FetchPartitionCountAsync(topic, cancellationToken);
                var config = new PartitionerConfig(partitionCount);
                _topicPartitionCache[topic] = config;
                return config;
            }
            catch
            {
                // If metadata fetch fails, fall through to default
            }
        }

        // Default to 1 partition if SC connection not available or fetch failed
        // Users can call SetPartitionCount() to override if needed
        var defaultConfig = new PartitionerConfig(1);
        _topicPartitionCache[topic] = defaultConfig;
        return defaultConfig;
    }

    /// <summary>
    /// Fetches partition count for a topic from SC metadata.
    /// </summary>
    private async Task<int> FetchPartitionCountAsync(string topic, CancellationToken cancellationToken)
    {
        using var writer = new FluvioBinaryWriter();

        // ListRequest structure for topics
        writer.WriteInt32(1); // name_filters: Vec<String> - 1 filter
        writer.WriteString(topic); // topic name filter

        var requestBody = writer.ToArray();

        var responseBytes = await _scConnection!.SendRequestAsync(
            ApiKey.AdminList,
            0, // API version
            _clientId,
            requestBody,
            cancellationToken);

        using var reader = new FluvioBinaryReader(responseBytes);

        // ListResponse<TopicSpec> structure
        // topics: Vec<Metadata<TopicSpec>>
        var topicCount = reader.ReadInt32();

        if (topicCount == 0)
        {
            throw new FluvioException($"Topic '{topic}' not found");
        }

        // Read first Metadata<TopicSpec>
        // name: String
        var responseTopic = reader.ReadString();

        // spec: TopicSpec
        var hasStorageConfig = reader.ReadBool(); // Option<StorageConfig> - false = None
        if (hasStorageConfig)
        {
            // Skip StorageConfig if present
            // This is simplified - proper implementation would decode StorageConfig
            reader.ReadInt32(); // segment_size
            reader.ReadInt32(); // max_partition_size
        }

        var replicas = reader.ReadInt32(); // replicas (i32)
        var partitionCount = reader.ReadInt32(); // partitions (i32)

        return partitionCount;
    }

    /// <summary>
    /// Sets the partition count for a topic, enabling the partitioner to work correctly.
    /// Call this method before producing to multi-partition topics.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="partitionCount">Number of partitions.</param>
    public void SetPartitionCount(string topic, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentException("Partition count must be positive", nameof(partitionCount));
        }

        _topicPartitionCache[topic] = new PartitionerConfig(partitionCount);
    }

    private void WriteRecordSet(FluvioBinaryWriter writer, List<ProduceRecord> records)
    {
        // RecordSet encoding: length (i32) + batches
        // For simplicity, we create one batch per RecordSet

        // First encode the batch to get its size
        using var batchWriter = new FluvioBinaryWriter();
        var batch = Batch<List<ProduceRecord>>.Default(records);
        batch.Encode(batchWriter, _spuConnection.TimeProvider);

        var batchBytes = batchWriter.ToArray();

        // Write RecordSet: length + batches
        writer.WriteInt32(batchBytes.Length); // Total size of all batches
        writer._stream.Write(batchBytes);     // The batch data
    }

    /// <summary>
    /// Flushes any pending records. (No-op in current implementation.)
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        // In a full implementation, this would flush any buffered records
        return Task.CompletedTask;
    }

    /// <summary>
    /// Disposes the producer. (No-op, does not own connection.)
    /// </summary>
    public ValueTask DisposeAsync()
    {
        // Producer doesn't own the connection, so nothing to dispose
        return ValueTask.CompletedTask;
    }
}
