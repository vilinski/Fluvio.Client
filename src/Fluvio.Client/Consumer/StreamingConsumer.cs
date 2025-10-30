using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Network;
using Fluvio.Client.Protocol;

namespace Fluvio.Client.Consumer;

/// <summary>
/// High-performance streaming consumer that maintains a persistent StreamFetch connection.
/// Based on Rust implementation architecture with .NET idioms (Channels, IAsyncEnumerable).
/// Zero polling delays - records stream continuously.
/// </summary>
internal sealed class StreamingConsumer : IAsyncDisposable
{
    private readonly FluvioConnection _connection;
    private readonly string _topic;
    private readonly int _partition;
    private readonly ConsumerOptions _options;
    private readonly string? _clientId;

    private Channel<ConsumeRecord>? _recordChannel;
    private Task? _streamReaderTask;
    private CancellationTokenSource? _streamCts;

    public StreamingConsumer(
        FluvioConnection connection,
        string topic,
        int partition,
        ConsumerOptions options,
        string? clientId)
    {
        _connection = connection;
        _topic = topic;
        _partition = partition;
        _options = options;
        _clientId = clientId;
    }

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
            writer.WriteString(_topic);
            writer.WriteInt32(_partition);
            writer.WriteInt64(startOffset);
            writer.WriteInt32(_options.MaxBytes);
            writer.WriteInt8((sbyte)(_options.IsolationLevel == IsolationLevel.ReadCommitted ? 1 : 0));
            var requestBody = writer.ToArray();

            // Send SINGLE StreamFetch request and get streaming channel
            var responseStream = await _connection.SendStreamingRequestAsync(
                ApiKey.StreamFetch,
                10,
                _clientId,
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
                    Console.WriteLine($"Parse error: {ex.Message}");
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
            Console.WriteLine($"Stream error: {ex.Message}");
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
        if (responseTopic != _topic)
        {
            throw new FluvioException($"Topic mismatch: expected '{_topic}', got '{responseTopic}'");
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
            for (int i = 0; i < abortedCount; i++)
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

            // Skip batch header
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

            var recordCount = reader.ReadInt32();

            for (int i = 0; i < recordCount; i++)
            {
                var recordLen = reader.ReadVarLong();
                var attributes = reader.ReadInt8();
                var timestampDelta = reader.ReadVarLong();
                var offsetDelta = reader.ReadVarLong();

                var hasKey = reader.ReadInt8() != 0;
                byte[]? key = null;
                if (hasKey)
                {
                    var keyLen = reader.ReadVarLong();
                    key = reader.ReadRawBytes((int)keyLen);
                }

                var valueLen = reader.ReadVarLong();
                var value = reader.ReadRawBytes((int)valueLen);

                var headerCount = reader.ReadVarLong();
                for (long h = 0; h < headerCount; h++)
                {
                    reader.ReadVarLong();
                    reader.ReadVarLong();
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
        _streamCts?.Cancel();

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
