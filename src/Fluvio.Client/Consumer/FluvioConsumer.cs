using System.Runtime.CompilerServices;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Network;
using Fluvio.Client.Protocol;

namespace Fluvio.Client.Consumer;

/// <summary>
/// Fluvio consumer implementation
/// </summary>
internal sealed class FluvioConsumer : IFluvioConsumer
{
    private readonly FluvioConnection _connection;
    private readonly ConsumerOptions _options;
    private readonly string? _clientId;

    public FluvioConsumer(FluvioConnection connection, ConsumerOptions? options, string? clientId)
    {
        _connection = connection;
        _options = options ?? new ConsumerOptions();
        _clientId = clientId;
    }

    public async IAsyncEnumerable<ConsumeRecord> StreamAsync(
        string topic,
        int partition = 0,
        long offset = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var currentOffset = offset;

        while (!cancellationToken.IsCancellationRequested)
        {
            var records = await FetchBatchAsync(topic, partition, currentOffset, _options.MaxBytes, cancellationToken);

            if (records.Count == 0)
            {
                // No records available, wait a bit before polling again
                await Task.Delay(100, cancellationToken);
                continue;
            }

            foreach (var record in records)
            {
                yield return record;
                currentOffset = record.Offset + 1;
            }
        }
    }

    public async Task<IReadOnlyList<ConsumeRecord>> FetchBatchAsync(
        string topic,
        int partition = 0,
        long offset = 0,
        int maxBytes = 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        // Build fetch request
        using var writer = new FluvioBinaryWriter();

        // Write topic name
        writer.WriteString(topic);

        // Write partition
        writer.WriteInt32(partition);

        // Write fetch offset
        writer.WriteInt64(offset);

        // Write max bytes
        writer.WriteInt32(maxBytes);

        // Write isolation level
        writer.WriteInt8((sbyte)(_options.IsolationLevel == IsolationLevel.ReadCommitted ? 1 : 0));

        var requestBody = writer.ToArray();

        // Send request
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.Fetch,
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
            throw new FluvioException($"Fetch failed: {errorCode} - {errorMessage}");
        }

        // Read high water mark
        var highWaterMark = reader.ReadInt64();

        // Read records count
        var recordCount = reader.ReadInt32();
        var records = new List<ConsumeRecord>(recordCount);

        for (int i = 0; i < recordCount; i++)
        {
            var record = ReadRecord(reader);
            records.Add(record);
        }

        return records;
    }

    private ConsumeRecord ReadRecord(FluvioBinaryReader reader)
    {
        // Record format:
        // - offset
        // - key length + key (nullable)
        // - value length + value
        // - timestamp

        var offset = reader.ReadInt64();
        var key = reader.ReadNullableBytes();
        var value = reader.ReadBytes();
        var timestamp = reader.ReadInt64();

        return new ConsumeRecord(
            Offset: offset,
            Value: value,
            Key: key,
            Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(timestamp));
    }

    public ValueTask DisposeAsync()
    {
        // Consumer doesn't own the connection, so nothing to dispose
        return ValueTask.CompletedTask;
    }
}
