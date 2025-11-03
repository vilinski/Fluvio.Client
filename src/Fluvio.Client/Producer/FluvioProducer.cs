using Fluvio.Client.Abstractions;
using Fluvio.Client.Network;
using Fluvio.Client.Protocol;
using Fluvio.Client.Protocol.Records;

namespace Fluvio.Client.Producer;

/// <summary>
/// Fluvio producer implementation
/// </summary>
internal sealed class FluvioProducer : IFluvioProducer
{
    private readonly FluvioConnection _connection;
    private readonly ProducerOptions _options;
    private readonly string? _clientId;

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioProducer"/> class.
    /// </summary>
    /// <param name="connection">The Fluvio connection.</param>
    /// <param name="options">Producer options.</param>
    /// <param name="clientId">Optional client ID.</param>
    public FluvioProducer(FluvioConnection connection, ProducerOptions? options, string? clientId)
    {
        _connection = connection;
        _options = options ?? new ProducerOptions();
        _clientId = clientId;
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
        var offsets = await SendBatchAsync(topic, [new ProduceRecord(value, key)], cancellationToken);
        return offsets[0];
    }

    /// <summary>
    /// Sends a batch of records to the specified topic.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="records">Records to produce.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of offsets for produced records.</returns>
    public async Task<IReadOnlyList<long>> SendBatchAsync(string topic, IEnumerable<ProduceRecord> records, CancellationToken cancellationToken = default)
    {
        var recordList = records.ToList();

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
        writer.WriteInt32(0); // partition_index = 0

        // records: RecordSet (i32 length + batches)
        WriteRecordSet(writer, recordList);

        // 5. smartmodules: Vec<SmartModuleInvocation> (empty for now)
        writer.WriteInt32(0);

        var requestBody = writer.ToArray();

        // Send request with timeout
        // Create a timeout cancellation token based on the producer timeout
        using var timeoutCts = new CancellationTokenSource(_options.Timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        var responseBytes = await _connection.SendRequestAsync(
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
        var offsets = new List<long>(recordList.Count);
        for (var i = 0; i < recordList.Count; i++)
        {
            offsets.Add(baseOffset + i);
        }

        return offsets;
    }

    private void WriteRecordSet(FluvioBinaryWriter writer, List<ProduceRecord> records)
    {
        // RecordSet encoding: length (i32) + batches
        // For simplicity, we create one batch per RecordSet

        // First encode the batch to get its size
        using var batchWriter = new FluvioBinaryWriter();
        var batch = Batch<List<ProduceRecord>>.Default(records);
        batch.Encode(batchWriter);

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
