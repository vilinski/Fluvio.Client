using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Protocol.Records;

/// <summary>
/// Generic batch structure
/// Source: fluvio-protocol/src/record/batch.rs
/// </summary>
/// <typeparam name="TRecords">Record collection type</typeparam>
public class Batch<TRecords>
{
    /// <summary>
    /// Base offset for this batch
    /// </summary>
    public Offset BaseOffset { get; set; }

    /// <summary>
    /// Batch length in bytes (calculated during encoding, not stored)
    /// </summary>
    public int BatchLen { get; private set; }

    /// <summary>
    /// Batch header containing metadata
    /// </summary>
    public BatchHeader Header { get; set; }

    /// <summary>
    /// Schema ID (only encoded if Header.HasSchema() is true)
    /// </summary>
    public SchemaId SchemaId { get; set; }

    /// <summary>
    /// Records in this batch
    /// </summary>
    public TRecords Records { get; set; }

    /// <summary>
    /// Creates a new batch with the specified records
    /// </summary>
    public Batch(TRecords records)
    {
        BaseOffset = 0;
        Header = BatchHeader.Default();
        SchemaId = 0;
        Records = records;
    }

    /// <summary>
    /// Creates a default batch
    /// </summary>
    public static Batch<TRecords> Default(TRecords records)
    {
        return new Batch<TRecords>(records);
    }
}

/// <summary>
/// Extension methods for Batch encoding
/// </summary>
public static class BatchEncoder
{
    /// <summary>
    /// Encodes a Batch
    /// </summary>
    internal static void Encode(this Batch<List<ProduceRecord>> batch, FluvioBinaryWriter writer, TimeProvider? timeProvider = null)
    {
        var timestamp = (timeProvider ?? TimeProvider.System).GetUtcNow().ToUnixTimeMilliseconds();

        // Update header timestamps
        batch.Header.FirstTimestamp = timestamp;
        batch.Header.MaxTimeStamp = timestamp;
        batch.Header.LastOffsetDelta = batch.Records.Count - 1;

        // 1. Encode records to calculate size
        var recordsBytes = EncodeRecords(batch.Records, timestamp);

        // 2. Build CRC-protected buffer
        var crcBuffer = BuildCrcBuffer(batch.Header, batch.SchemaId, recordsBytes);

        // 3. Calculate CRC
        var crc = Crc32C.Compute(crcBuffer);
        batch.Header.Crc = crc;

        // 4. Calculate batch_len
        var batchLen = 4 + 1 + crcBuffer.Length + 4; // partition_leader_epoch + magic + crc_buffer + crc

        // 5. Write batch to wire format
        writer.WriteInt64(batch.BaseOffset);
        writer.WriteInt32(batchLen);
        writer.WriteInt32(batch.Header.PartitionLeaderEpoch);
        writer.WriteInt8(batch.Header.Magic);
        writer.WriteUInt32(crc);
        writer._stream.Write(crcBuffer);
    }

    private static byte[] EncodeRecords(List<ProduceRecord> records, long baseTimestamp)
    {
        using var writer = new FluvioBinaryWriter();

        // Vec<Record> encoding: count (u32) + elements
        writer.WriteInt32(records.Count);

        // Encode each record
        for (var i = 0; i < records.Count; i++)
        {
            EncodeRecord(writer, records[i], i, baseTimestamp);
        }

        return writer.ToArray();
    }

    private static void EncodeRecord(FluvioBinaryWriter writer, ProduceRecord record, int offsetDelta, long baseTimestamp)
    {
        // Build record content
        using var recordContentWriter = new FluvioBinaryWriter();

        // RecordHeader
        recordContentWriter.WriteInt8(0); // attributes
        recordContentWriter.WriteVarLong(0); // timestamp_delta
        recordContentWriter.WriteVarLong(offsetDelta); // offset_delta

        // key: Option<Bytes>
        if (!record.Key.HasValue || record.Key.Value.Length == 0)
        {
            recordContentWriter.WriteInt8(0); // None
        }
        else
        {
            recordContentWriter.WriteInt8(1); // Some
            recordContentWriter.WriteVarLong(record.Key.Value.Length);
            recordContentWriter._stream.Write(record.Key.Value.Span);
        }

        // value: Bytes
        var valueSpan = record.Value.Span;
        recordContentWriter.WriteVarLong(valueSpan.Length);
        recordContentWriter._stream.Write(valueSpan);

        // headers: Vec<RecordHeader> = varlong count + header items
        if (record.Headers == null || record.Headers.Count == 0)
        {
            recordContentWriter.WriteVarLong(0); // No headers
        }
        else
        {
            recordContentWriter.WriteVarLong(record.Headers.Count);
            foreach (var (key, value) in record.Headers)
            {
                // RecordHeader: key (String) + value (Bytes)
                // String encoding: varlong length + UTF-8 bytes
                var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
                recordContentWriter.WriteVarLong(keyBytes.Length);
                recordContentWriter._stream.Write(keyBytes);

                // Bytes encoding: varlong length + data
                recordContentWriter.WriteVarLong(value.Length);
                recordContentWriter._stream.Write(value.Span);
            }
        }

        var recordContent = recordContentWriter.ToArray();

        // Write total record length as varlong
        writer.WriteVarLong(recordContent.Length);
        writer._stream.Write(recordContent);
    }

    private static byte[] BuildCrcBuffer(BatchHeader header, SchemaId schemaId, byte[] recordsBytes)
    {
        using var writer = new FluvioBinaryWriter();

        // CRC is computed over: attributes, last_offset_delta, timestamps, producer info, [schema_id], records
        writer.WriteInt16(header.Attributes);
        writer.WriteInt32(header.LastOffsetDelta);
        writer.WriteInt64(header.FirstTimestamp);
        writer.WriteInt64(header.MaxTimeStamp);
        writer.WriteInt64(header.ProducerId);
        writer.WriteInt16(header.ProducerEpoch);
        writer.WriteInt32(header.FirstSequence);

        if (header.HasSchema())
        {
            schemaId.Encode(writer);
        }

        writer._stream.Write(recordsBytes);

        return writer.ToArray();
    }
}
