using Fluvio.Client.Abstractions;
using Fluvio.Client.Network;
using Fluvio.Client.Protocol;

namespace Fluvio.Client.Producer;

/// <summary>
/// Fluvio producer implementation
/// </summary>
internal sealed class FluvioProducer : IFluvioProducer
{
    private readonly FluvioConnection _connection;
    private readonly ProducerOptions _options;
    private readonly string? _clientId;

    public FluvioProducer(FluvioConnection connection, ProducerOptions? options, string? clientId)
    {
        _connection = connection;
        _options = options ?? new ProducerOptions();
        _clientId = clientId;
    }

    public async Task<long> SendAsync(string topic, ReadOnlyMemory<byte> value, ReadOnlyMemory<byte>? key = null, CancellationToken cancellationToken = default)
    {
        var offsets = await SendBatchAsync(topic, [new ProduceRecord(value, key)], cancellationToken);
        return offsets[0];
    }

    public async Task<IReadOnlyList<long>> SendBatchAsync(string topic, IEnumerable<ProduceRecord> records, CancellationToken cancellationToken = default)
    {
        // Build produce request
        using var writer = new FluvioBinaryWriter();

        // Write topic name
        writer.WriteString(topic);

        // Write partition (default to 0)
        writer.WriteInt32(0);

        // Write records
        var recordList = records.ToList();
        writer.WriteInt32(recordList.Count);

        foreach (var record in recordList)
        {
            WriteRecord(writer, record);
        }

        // Write delivery guarantee flags
        writer.WriteBool(_options.DeliveryGuarantee == DeliveryGuarantee.AtLeastOnce);

        var requestBody = writer.ToArray();

        // Send request
        var responseBytes = await _connection.SendRequestAsync(
            ApiKey.Produce,
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
            throw new FluvioException($"Produce failed: {errorCode} - {errorMessage}");
        }

        // Read base offset
        var baseOffset = reader.ReadInt64();

        // Generate offsets for each record
        var offsets = new List<long>(recordList.Count);
        for (int i = 0; i < recordList.Count; i++)
        {
            offsets.Add(baseOffset + i);
        }

        return offsets;
    }

    private void WriteRecord(FluvioBinaryWriter writer, ProduceRecord record)
    {
        // Record format:
        // - key length + key (nullable)
        // - value length + value
        // - timestamp (current time)
        // - headers count (0 for now)

        writer.WriteNullableBytes(record.Key);
        writer.WriteBytes(record.Value.Span);
        writer.WriteInt64(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        writer.WriteInt32(0); // No headers
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        // In a full implementation, this would flush any buffered records
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        // Producer doesn't own the connection, so nothing to dispose
        return ValueTask.CompletedTask;
    }
}
