using System.Collections.Generic;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Protocol;
using Fluvio.Client.Protocol.Records;
using Xunit;
using Xunit.Abstractions;

namespace Fluvio.Client.Tests.Protocol;

/// <summary>
/// Tests for Batch<T> encoding matching Rust tests
/// Source: fluvio-protocol/src/record/batch.rs#test_encode_and_decode_batch_basic
/// </summary>
public class BatchEncodingTests
{
    private readonly ITestOutputHelper _output;

    public BatchEncodingTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void TestEncodeBasicBatch_MatchesRustTest()
    {
        // Recreate the Rust test: test_encode_and_decode_batch_basic
        // let value = vec![0x74, 0x65, 0x73, 0x74];  // "test"
        // batch.header.first_timestamp = 1555478494747;
        // batch.header.max_time_stamp = 1555478494747;
        // Expected CRC: 1430948200

        var value = new byte[] { 0x74, 0x65, 0x73, 0x74 }; // "test"
        var record = new ProduceRecord(value);
        var records = new List<ProduceRecord> { record };

        // Create batch with default values (matching Rust)
        var batch = Batch<List<ProduceRecord>>.Default(records);

        // Override timestamps to match Rust test
        batch.Header.FirstTimestamp = 1555478494747;
        batch.Header.MaxTimeStamp = 1555478494747;
        batch.Header.LastOffsetDelta = -1; // Rust test doesn't update this

        // Encode (but override timestamp in encoder to use fixed value)
        using var writer = new FluvioBinaryWriter();

        // Build records manually to control timestamp
        var recordsBytes = EncodeRecordsWithTimestamp(records, 1555478494747);

        // Build CRC buffer
        using var crcWriter = new FluvioBinaryWriter();
        crcWriter.WriteInt16(batch.Header.Attributes);
        crcWriter.WriteInt32(batch.Header.LastOffsetDelta);
        crcWriter.WriteInt64(batch.Header.FirstTimestamp);
        crcWriter.WriteInt64(batch.Header.MaxTimeStamp);
        crcWriter.WriteInt64(batch.Header.ProducerId);
        crcWriter.WriteInt16(batch.Header.ProducerEpoch);
        crcWriter.WriteInt32(batch.Header.FirstSequence);
        if (batch.Header.HasSchema())
        {
            batch.SchemaId.Encode(crcWriter);
        }
        crcWriter._stream.Write(recordsBytes);

        var crcBuffer = crcWriter.ToArray();
        var crc = Crc32C.Compute(crcBuffer);

        _output.WriteLine($"Records bytes ({recordsBytes.Length}): {BitConverter.ToString(recordsBytes).Replace("-", " ")}");
        _output.WriteLine($"CRC buffer ({crcBuffer.Length}): {BitConverter.ToString(crcBuffer).Replace("-", " ")}");
        _output.WriteLine($"Calculated CRC: {crc} (0x{crc:X8})");
        _output.WriteLine($"Expected CRC:   1430948200 (0x554A8968)");

        // Verify CRC matches Rust test
        Assert.Equal(1430948200u, crc);

        // Write full batch
        int batchLen = 4 + 1 + crcBuffer.Length + 4;
        writer.WriteInt64(batch.BaseOffset);
        writer.WriteInt32(batchLen);
        writer.WriteInt32(batch.Header.PartitionLeaderEpoch);
        writer.WriteInt8(batch.Header.Magic);
        writer.WriteUInt32(crc);
        writer._stream.Write(crcBuffer);

        var batchBytes = writer.ToArray();
        _output.WriteLine($"\nComplete batch ({batchBytes.Length} bytes):");
        _output.WriteLine(BitConverter.ToString(batchBytes).Replace("-", " "));
    }

    [Fact]
    public void TestBatchWithSchemaId_MatchesRustTest()
    {
        // Recreate: test_encode_and_decode_batch_w_schemaid
        // Expected CRC with schema_id=42: 2943551365

        var value = new byte[] { 0x74, 0x65, 0x73, 0x74 }; // "test"
        var record = new ProduceRecord(value);
        var records = new List<ProduceRecord> { record };

        var batch = Batch<List<ProduceRecord>>.Default(records);
        batch.Header.FirstTimestamp = 1555478494747;
        batch.Header.MaxTimeStamp = 1555478494747;
        batch.Header.LastOffsetDelta = -1;

        // Set schema flag and ID
        batch.Header.Attributes = 0x10; // ATTR_SCHEMA_PRESENT (bit 4)
        batch.SchemaId = 42;

        // Debug: Check SchemaId encoding
        using var schemaIdWriter = new FluvioBinaryWriter();
        batch.SchemaId.Encode(schemaIdWriter);
        var schemaIdBytes = schemaIdWriter.ToArray();
        _output.WriteLine($"SchemaId bytes: {BitConverter.ToString(schemaIdBytes).Replace("-", " ")}");
        _output.WriteLine($"SchemaId value: {batch.SchemaId.Value}");

        // Encode
        var recordsBytes = EncodeRecordsWithTimestamp(records, 1555478494747);

        using var crcWriter = new FluvioBinaryWriter();
        crcWriter.WriteInt16(batch.Header.Attributes);
        crcWriter.WriteInt32(batch.Header.LastOffsetDelta);
        crcWriter.WriteInt64(batch.Header.FirstTimestamp);
        crcWriter.WriteInt64(batch.Header.MaxTimeStamp);
        crcWriter.WriteInt64(batch.Header.ProducerId);
        crcWriter.WriteInt16(batch.Header.ProducerEpoch);
        crcWriter.WriteInt32(batch.Header.FirstSequence);
        batch.SchemaId.Encode(crcWriter);
        crcWriter._stream.Write(recordsBytes);

        var crcBuffer = crcWriter.ToArray();
        var crc = Crc32C.Compute(crcBuffer);

        _output.WriteLine($"With SchemaId=42:");
        _output.WriteLine($"CRC buffer ({crcBuffer.Length}): {BitConverter.ToString(crcBuffer).Replace("-", " ")}");
        _output.WriteLine($"Records bytes ({recordsBytes.Length}): {BitConverter.ToString(recordsBytes).Replace("-", " ")}");
        _output.WriteLine($"Calculated CRC: {crc} (0x{crc:X8})");
        _output.WriteLine($"Expected CRC:   2943551365 (0xAF86DC05)");

        Assert.Equal(2943551365u, crc);
    }

    private byte[] EncodeRecordsWithTimestamp(List<ProduceRecord> records, long timestamp)
    {
        using var writer = new FluvioBinaryWriter();

        // Vec<Record> encoding
        writer.WriteInt32(records.Count);

        for (int i = 0; i < records.Count; i++)
        {
            using var recordWriter = new FluvioBinaryWriter();

            recordWriter.WriteInt8(0); // attributes
            recordWriter.WriteVarLong(0); // timestamp_delta
            recordWriter.WriteVarLong(i); // offset_delta

            // key: None
            recordWriter.WriteInt8(0);

            // value
            recordWriter.WriteVarLong(records[i].Value.Length);
            recordWriter._stream.Write(records[i].Value.Span);

            // headers
            recordWriter.WriteVarLong(0);

            var recordContent = recordWriter.ToArray();
            writer.WriteVarLong(recordContent.Length);
            writer._stream.Write(recordContent);
        }

        return writer.ToArray();
    }
}
