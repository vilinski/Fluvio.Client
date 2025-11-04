using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Network;
using Fluvio.Client.Protocol;
using Microsoft.Extensions.Logging;

namespace Fluvio.Client.Consumer;

/// <summary>
/// High-performance streaming consumer that maintains a persistent StreamFetch connection.
/// Based on Rust implementation architecture with .NET idioms (Channels, IAsyncEnumerable).
/// Zero polling delays - records stream continuously.
/// </summary>
internal sealed class StreamingConsumer(
    FluvioConnection connection,
    string topic,
    int partition,
    ConsumerOptions options,
    string? clientId,
    ILogger? logger = null)
    : IAsyncDisposable
{
    private Channel<ConsumeRecord>? _recordChannel;
    private Task? _streamReaderTask;
    private CancellationTokenSource? _streamCts;

    /// <summary>
    /// Streams records continuously from a persistent StreamFetch connection.
    /// Uses bounded channel (capacity 100) for backpressure, matching Rust implementation.
    /// </summary>
    public async IAsyncEnumerable<ConsumeRecord> StreamAsync(
        long startOffset,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Create bounded channel for backpressure (Rust uses capacity 100)
        _recordChannel = Channel.CreateBounded<ConsumeRecord>(new BoundedChannelOptions(100)
        {
            FullMode = BoundedChannelFullMode.Wait // Block producer when full (backpressure)
        });

        // Start persistent stream reader in background
        _streamCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _streamReaderTask = Task.Run(async () => await StreamReaderLoopAsync(startOffset, _streamCts.Token), _streamCts.Token);

        // Yield records as they arrive from channel
        await foreach (var record in _recordChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return record;
        }
    }

    /// <summary>
    /// Background loop that continuously reads batches from persistent StreamFetch connection.
    /// Matches Rust implementation: single request, continuous response reading.
    /// </summary>
    private async Task StreamReaderLoopAsync(long startOffset, CancellationToken cancellationToken)
    {
        try
        {
            var currentOffset = startOffset;

            // Build StreamFetch request
            using var writer = new FluvioBinaryWriter();
            writer.WriteString(topic);
            writer.WriteInt32(partition);
            writer.WriteInt64(startOffset);
            writer.WriteInt32(options.MaxBytes);
            writer.WriteInt8((sbyte)(options.IsolationLevel == IsolationLevel.ReadCommitted ? 1 : 0));

            // SmartModules (Vec<SmartModuleInvocation>) - version 18+
            writer.EncodeSmartModules(options.SmartModules, version: 25);

            // consumer_id (Option<String>) - version 23+ (not implemented yet)
            writer.WriteBool(false); // None

            var requestBody = writer.ToArray();

            // Send SINGLE StreamFetch request and get streaming channel
            var responseStream = await connection.SendStreamingRequestAsync(
                ApiKey.StreamFetch,
                10,
                clientId,
                requestBody,
                cancellationToken);

            // Continuously read batches from the persistent stream (NO POLLING!)
            await foreach (var responseBytes in responseStream.ReadAllAsync(cancellationToken))
            {
                try
                {
                    // Parse the batch from response bytes
                    var records = ParseStreamFetchResponse(responseBytes);

                    // Write records to channel (backpressure applied here if consumer is slow)
                    foreach (var record in records)
                    {
                        await _recordChannel!.Writer.WriteAsync(record, cancellationToken);
                        currentOffset = record.Offset + 1;
                    }
                }
                catch (Exception ex)
                {
                    // Log parse error but continue streaming
                    logger?.LogWarning(ex, "Failed to parse StreamFetch response for topic {Topic}, partition {Partition}", topic, partition);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            // Stream error - could implement reconnection logic here
            logger?.LogError(ex, "StreamFetch error for topic {Topic}, partition {Partition}", topic, partition);
        }
        finally
        {
            // Complete the channel when stream ends
            _recordChannel?.Writer.Complete();
        }
    }

    /// <summary>
    /// Parses a StreamFetch response into consume records.
    /// Same parsing logic as FluvioConsumer.FetchBatchAsync but for streaming responses.
    /// </summary>
    private List<ConsumeRecord> ParseStreamFetchResponse(ReadOnlyMemory<byte> responseBytes)
    {
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
        var logStartOffset = reader.ReadInt64();

        // Skip aborted transactions
        var hasAborted = reader.ReadInt8() != 0;
        if (hasAborted)
        {
            var abortedCount = reader.ReadInt32();
            for (var i = 0; i < abortedCount; i++)
            {
                reader.ReadInt64(); // producer_id
                reader.ReadInt64(); // first_offset
            }
        }

        // 4. Read RecordSet (batches)
        return ReadRecordSet(reader);
    }

    private List<ConsumeRecord> ReadRecordSet(FluvioBinaryReader reader)
    {
        var records = new List<ConsumeRecord>();

        // RecordSet is length-prefixed
        var recordSetLength = reader.ReadInt32();
        if (recordSetLength <= 0)
        {
            return records;
        }

        var recordSetEndPos = reader.Position + recordSetLength;

        while (reader.Position < recordSetEndPos)
        {
            if (recordSetEndPos - reader.Position < 12)
            {
                break;
            }

            var baseOffset = reader.ReadInt64();
            var batchLen = reader.ReadInt32();

            if (batchLen <= 0 || recordSetEndPos - reader.Position < batchLen)
            {
                break;
            }

            // Read batch header
            reader.ReadInt32(); // partition_leader_epoch
            reader.ReadInt8();  // magic
            reader.ReadUInt32(); // crc
            var attributes = reader.ReadInt16(); // attributes - contains compression type
            reader.ReadInt32(); // last_offset_delta
            reader.ReadInt64(); // first_timestamp
            reader.ReadInt64(); // max_timestamp
            reader.ReadInt64(); // producer_id
            reader.ReadInt16(); // producer_epoch
            reader.ReadInt32(); // first_sequence

            // Calculate how much data remains for records
            // batch_len includes: partition_leader_epoch (4) + magic (1) + rest
            // We've read: partition_leader_epoch (4) + magic (1) + crc (4) + attributes (2) +
            //             last_offset_delta (4) + first_timestamp (8) + max_timestamp (8) +
            //             producer_id (8) + producer_epoch (2) + first_sequence (4) = 45 bytes
            var headerBytesRead = 45;
            var recordsLength = batchLen - headerBytesRead;
            var recordsBytes = reader.ReadRawBytes(recordsLength);

            // Parse records
            using var recordsReader = new FluvioBinaryReader(recordsBytes);
            var recordCount = recordsReader.ReadInt32();

            for (var i = 0; i < recordCount; i++)
            {
                var recordLen = recordsReader.ReadVarLong();
                var recordAttributes = recordsReader.ReadInt8();
                var timestampDelta = recordsReader.ReadVarLong();
                var offsetDelta = recordsReader.ReadVarLong();

                var hasKey = recordsReader.ReadInt8() != 0;
                byte[]? key = null;
                if (hasKey)
                {
                    var keyLen = recordsReader.ReadVarLong();
                    key = recordsReader.ReadRawBytes((int)keyLen);
                }

                var valueLen = recordsReader.ReadVarLong();
                var value = recordsReader.ReadRawBytes((int)valueLen);

                var headerCount = recordsReader.ReadVarLong();
                for (long h = 0; h < headerCount; h++)
                {
                    recordsReader.ReadVarLong();
                    recordsReader.ReadVarLong();
                }

                var absoluteOffset = baseOffset + offsetDelta;

                records.Add(new ConsumeRecord(
                    Offset: absoluteOffset,
                    Value: value,
                    Key: key,
                    Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(timestampDelta)));
            }
        }

        return records;
    }

    public async ValueTask DisposeAsync()
    {
        // Cancel the background stream reader
        if (_streamCts != null)
            await _streamCts.CancelAsync();

        // Wait for reader to complete
        if (_streamReaderTask != null)
        {
            try
            {
                await _streamReaderTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        _streamCts?.Dispose();
    }
}
