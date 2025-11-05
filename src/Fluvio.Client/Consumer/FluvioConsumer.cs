using System.Runtime.CompilerServices;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Network;
using Fluvio.Client.Protocol;
using Fluvio.Client.Protocol.Requests;
using Fluvio.Client.Protocol.Responses;
using Microsoft.Extensions.Logging;

namespace Fluvio.Client.Consumer;

/// <summary>
/// Fluvio consumer implementation
/// </summary>
internal sealed class FluvioConsumer : IFluvioConsumer
{
    private readonly FluvioConnection _connection;
    private readonly ConsumerOptions _options;
    private readonly string? _clientId;
    private readonly ILogger? _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioConsumer"/> class.
    /// </summary>
    /// <param name="connection">The Fluvio connection.</param>
    /// <param name="options">Consumer options.</param>
    /// <param name="clientId">Optional client ID.</param>
    /// <param name="logger">Optional logger.</param>
    public FluvioConsumer(FluvioConnection connection, ConsumerOptions? options, string? clientId, ILogger? logger = null)
    {
        _connection = connection;
        _options = options ?? new ConsumerOptions();
        _clientId = clientId;
        _logger = logger;
    }

    /// <summary>
    /// Streams records from the specified topic starting at the given offset.
    /// Uses persistent StreamFetch connection for high performance with zero polling delays.
    /// If offset is not specified (null), uses OffsetResetStrategy from options.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="partition">Partition number.</param>
    /// <param name="offset">Starting offset (null to use offset reset strategy).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async enumerable of consumed records.</returns>
    public async IAsyncEnumerable<ConsumeRecord> StreamAsync(
        string topic,
        int partition = 0,
        long? offset = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Resolve starting offset based on strategy
        var startOffset = await ResolveStartOffsetAsync(topic, partition, offset, cancellationToken);

        var streamingConsumer = new StreamingConsumer(_connection, topic, partition, _options, _clientId, _logger);
        await foreach (var record in streamingConsumer.StreamAsync(startOffset, cancellationToken))
        {
            yield return record;
        }
    }

    /// <summary>
    /// Resolves the starting offset based on consumer options and stored offset.
    /// </summary>
    private async Task<long> ResolveStartOffsetAsync(
        string topic,
        int partition,
        long? explicitOffset,
        CancellationToken cancellationToken)
    {
        // If explicit offset provided, use it
        if (explicitOffset.HasValue)
        {
            return explicitOffset.Value;
        }

        // Get consumer ID if using consumer group
        var consumerId = OffsetResolver.GetConsumerId(_options.ConsumerGroup);

        // Fetch stored offset if using stored strategy
        long? storedOffset = null;
        if (consumerId != null &&
            (_options.OffsetReset == OffsetResetStrategy.StoredOrEarliest ||
             _options.OffsetReset == OffsetResetStrategy.StoredOrLatest))
        {
            try
            {
                storedOffset = await FetchLastOffsetAsync(consumerId, topic, partition, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to fetch stored offset for consumer {ConsumerId}", consumerId);
            }
        }

        // Resolve offset using strategy
        return OffsetResolver.ResolveStartOffset(storedOffset, _options.OffsetReset, explicitOffset);
    }

    /// <summary>
    /// Fetches a batch of records from the specified topic.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="partition">Partition number.</param>
    /// <param name="offset">Starting offset.</param>
    /// <param name="maxBytes">Maximum bytes to fetch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of consumed records.</returns>
    public async Task<IReadOnlyList<ConsumeRecord>> FetchBatchAsync(
        string topic,
        int partition = 0,
        long offset = 0,
        int maxBytes = 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        // Build StreamFetch request (API 1003, version 18)
        using var writer = new FluvioBinaryWriter();

        // Mandatory fields (all versions)
        // 1. topic (String with varint length)
        writer.WriteString(topic);

        // 2. partition (i32)
        writer.WriteInt32(partition);

        // 3. fetch_offset (i64)
        writer.WriteInt64(offset);

        // 4. max_bytes (i32)
        writer.WriteInt32(maxBytes);

        // 5. isolation (u8)
        writer.WriteInt8((sbyte)(_options.IsolationLevel == IsolationLevel.ReadCommitted ? 1 : 0));

        // Note: Version 10 for basic fetch without SmartModules
        // No additional fields needed for version 10

        var requestBody = writer.ToArray();

        // Send StreamFetch request (API 1003, version 10 - basic fetch)
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.StreamFetch,
            10, // API version 10 (basic StreamFetch without SmartModules)
            _clientId,
            requestBody,
            cancellationToken);

        // Parse StreamFetchResponse
        using var reader = new FluvioBinaryReader(responseBytes);

        // 1. Read topic (String)
        var responseTopic = reader.ReadString();
        if (responseTopic != topic)
        {
            throw new FluvioException($"Topic mismatch: expected '{topic}', got '{responseTopic}'");
        }

        // 2. Read stream_id (u32)
        var streamId = reader.ReadUInt32();

        // 3. Read FetchablePartitionResponse
        var partitionIndex = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();

        if (errorCode != ErrorCode.None)
        {
            throw new FluvioException($"Fetch failed: {errorCode}");
        }

        var highWaterMark = reader.ReadInt64();

        // For version 10, we don't have next_filter_offset
        // Skip log_start_offset (i64)
        var logStartOffset = reader.ReadInt64();

        // Skip aborted transactions (Option<Vec<AbortedTransaction>>)
        // Option encoding: 0 = None, 1 = Some
        var hasAborted = reader.ReadInt8() != 0;
        if (hasAborted)
        {
            var abortedCount = reader.ReadInt32();
            // Skip aborted transactions for now
            for (var i = 0; i < abortedCount; i++)
            {
                reader.ReadInt64(); // producer_id
                reader.ReadInt64(); // first_offset
            }
        }

        // 4. Read RecordSet (batches)
        try
        {
            var records = ReadRecordSet(reader, partition);
            return records;
        }
        catch (Exception ex)
        {
            throw new FluvioException($"Failed to parse record set: {ex.Message}", ex);
        }
    }

    private List<ConsumeRecord> ReadRecordSet(FluvioBinaryReader reader, int partition)
    {
        var records = new List<ConsumeRecord>();

        // RecordSet is length-prefixed (i32 length + data)
        var recordSetLength = reader.ReadInt32();

        if (recordSetLength <= 0)
        {
            return records; // Empty RecordSet
        }

        var recordSetEndPos = reader.Position + recordSetLength;

        // RecordSet contains a sequence of batches
        // Each batch has: base_offset (i64) + batch_len (i32) + batch_data

        while (reader.Position < recordSetEndPos)
        {
            // Check if we have enough bytes for batch header
            if (recordSetEndPos - reader.Position < 12) // 8 (base_offset) + 4 (batch_len)
            {
                break;
            }

            var baseOffset = reader.ReadInt64();
            var batchLen = reader.ReadInt32();

            if (batchLen <= 0 || recordSetEndPos - reader.Position < batchLen)
            {
                break; // Invalid or incomplete batch
            }

            // Read batch content
            var batchStartPos = reader.Position;

            // Skip batch header fields (we just need the records)
            reader.ReadInt32(); // partition_leader_epoch
            reader.ReadInt8();  // magic
            reader.ReadUInt32(); // crc
            reader.ReadInt16(); // attributes
            reader.ReadInt32(); // last_offset_delta
            reader.ReadInt64(); // first_timestamp
            reader.ReadInt64(); // max_timestamp
            reader.ReadInt64(); // producer_id
            reader.ReadInt16(); // producer_epoch
            reader.ReadInt32(); // first_sequence

            // Check if batch has schema (bit 13 of attributes)
            // Note: Schema ID support deferred until SmartModule integration is implemented
            // Current implementation works for basic record batches without schemas

            // Read record count
            var recordCount = reader.ReadInt32();

            // Read each record
            for (var i = 0; i < recordCount; i++)
            {
                var recordLen = reader.ReadVarLong();
                var recordStartPos = reader.Position;

                // RecordHeader
                var attributes = reader.ReadInt8();
                var timestampDelta = reader.ReadVarLong();
                var offsetDelta = reader.ReadVarLong();

                // Key (Option<Bytes>)
                var hasKey = reader.ReadInt8() != 0;
                byte[]? key = null;
                if (hasKey)
                {
                    var keyLen = reader.ReadVarLong();
                    key = reader.ReadRawBytes((int)keyLen);
                }

                // Value (Bytes)
                var valueLen = reader.ReadVarLong();
                var value = reader.ReadRawBytes((int)valueLen);

                // Headers (skip for now)
                var headerCount = reader.ReadVarLong();
                for (long h = 0; h < headerCount; h++)
                {
                    reader.ReadVarLong(); // header key len
                    reader.ReadVarLong(); // header value len
                }

                // Calculate absolute offset
                var absoluteOffset = baseOffset + offsetDelta;

                records.Add(new ConsumeRecord(
                    Offset: absoluteOffset,
                    Value: value,
                    Key: key,
                    Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(timestampDelta),
                    Partition: partition));
            }
        }

        return records;
    }

    /// <summary>
    /// Fetches the last committed offset for a consumer from the cluster.
    /// </summary>
    public async Task<long?> FetchLastOffsetAsync(
        string consumerId,
        string topic,
        int partition = 0,
        CancellationToken cancellationToken = default)
    {
        // Build FetchConsumerOffsetsRequest
        var request = new FetchConsumerOffsetsRequest
        {
            Filter = new FilterOptions
            {
                ReplicaId = new ReplicaKey
                {
                    Topic = topic,
                    Partition = partition
                },
                ConsumerId = consumerId
            }
        };

        using var writer = new FluvioBinaryWriter();
        request.WriteTo(writer);
        var requestBody = writer.ToArray();

        // Send request (API 1008, version 0)
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.FetchConsumerOffsets,
            0, // API version
            _clientId,
            requestBody,
            cancellationToken);

        // Parse response
        using var reader = new FluvioBinaryReader(responseBytes);
        var response = FetchConsumerOffsetsResponse.ReadFrom(reader);

        if (response.ErrorCode != ErrorCode.None)
        {
            throw new FluvioException($"Failed to fetch consumer offset: {response.ErrorCode}");
        }

        // Return the offset if found, null otherwise
        return response.Consumers.FirstOrDefault()?.Offset;
    }

    /// <summary>
    /// Commits (updates) the consumer offset for a specific topic/partition.
    /// </summary>
    public async Task CommitOffsetAsync(
        string consumerId,
        string topic,
        int partition,
        long offset,
        uint sessionId,
        CancellationToken cancellationToken = default)
    {
        // Build UpdateConsumerOffsetRequest
        var request = new UpdateConsumerOffsetRequest
        {
            Offset = offset,
            SessionId = sessionId
        };

        using var writer = new FluvioBinaryWriter();
        request.WriteTo(writer);
        var requestBody = writer.ToArray();

        // Send request (API 1006, version 0)
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.UpdateConsumerOffset,
            0, // API version
            _clientId,
            requestBody,
            cancellationToken);

        // Parse response
        using var reader = new FluvioBinaryReader(responseBytes);
        var response = UpdateConsumerOffsetResponse.ReadFrom(reader);

        if (response.ErrorCode != ErrorCode.None)
        {
            throw new FluvioException($"Failed to update consumer offset: {response.ErrorCode}");
        }
    }

    /// <summary>
    /// Disposes the consumer. (No-op, does not own connection.)
    /// </summary>
    public ValueTask DisposeAsync()
    {
        // Consumer doesn't own the connection, so nothing to dispose
        return ValueTask.CompletedTask;
    }
}
