using Fluvio.Client.Abstractions;
using Fluvio.Client.Protocol;
using Xunit.Abstractions;

namespace Fluvio.Client.Tests.Protocol;

/// <summary>
/// Test RecordBatch encoding to match the Rust test:
/// https://github.com/infinyon/fluvio/blob/master/crates/fluvio-protocol/src/record/batch.rs#test_encode_and_decode_batch_basic
/// </summary>
public class RecordBatchEncodingTest(ITestOutputHelper output)
{
    [Fact]
    public void EncodeRecordBatch_MatchesRustTest()
    {
        // From Rust test: test_encode_and_decode_batch_basic()
        // let value = vec![0x74, 0x65, 0x73, 0x74];  // "test"
        // batch.header.first_timestamp = 1555478494747;
        // batch.header.max_time_stamp = 1555478494747;
        // Expected CRC: 1430948200

        var value = new byte[] { 0x74, 0x65, 0x73, 0x74 }; // "test"
        var timestamp = 1555478494747L; // Same timestamp as Rust test
        var expectedCrc = 1430948200u;

        // Build a single record
        var record = new ProduceRecord(value);
        var records = new List<ProduceRecord> { record };

        // Encode the RecordBatch using our implementation
        using var writer = new FluvioBinaryWriter();

        // Build record first
        using var recordWriter = new FluvioBinaryWriter();

        // RecordHeader
        recordWriter.WriteInt8(0); // attributes
        recordWriter.WriteVarLong(0); // timestamp_delta
        recordWriter.WriteVarLong(0); // offset_delta

        // key: Option<Bytes> - None
        recordWriter.WriteInt8(0);

        // value: Bytes
        recordWriter.WriteVarLong(value.Length); // i64 varint!
        recordWriter._stream.Write(value);

        // headers: varlong (count = 0)
        recordWriter.WriteVarLong(0);

        var recordContent = recordWriter.ToArray();
        output.WriteLine($"Record content: {BitConverter.ToString(recordContent).Replace("-", " ")}");

        // Encode as Vec<Record> - needs count prefix first!
        using var recordsWriter = new FluvioBinaryWriter();

        // Vec encoding: count (u32) + elements
        recordsWriter.WriteInt32(1); // Vec count = 1 record

        // Then the record with its length prefix
        recordsWriter.WriteVarLong(recordContent.Length); // i64 varint!
        recordsWriter._stream.Write(recordContent);

        var recordsBytes = recordsWriter.ToArray();
        output.WriteLine($"Records bytes: {BitConverter.ToString(recordsBytes).Replace("-", " ")}");

        // Build the CRC-protected section (attributes through records)
        using var crcBufferWriter = new FluvioBinaryWriter();

        crcBufferWriter.WriteInt16(0); // attributes (no compression)
        crcBufferWriter.WriteInt32(-1); // last_offset_delta = -1 (matches Rust test default!)
        crcBufferWriter.WriteInt64(timestamp); // first_timestamp
        crcBufferWriter.WriteInt64(timestamp); // max_time_stamp
        crcBufferWriter.WriteInt64(-1); // producer_id (-1 = none)
        crcBufferWriter.WriteInt16(-1); // producer_epoch
        crcBufferWriter.WriteInt32(-1); // first_sequence (should be -1!)

        // Append records to CRC buffer
        crcBufferWriter._stream.Write(recordsBytes);

        var crcBuffer = crcBufferWriter.ToArray();
        output.WriteLine($"CRC buffer ({crcBuffer.Length} bytes): {BitConverter.ToString(crcBuffer).Replace("-", " ")}");

        // Calculate CRC over the entire buffer
        var hashBytes = System.IO.Hashing.Crc32.Hash(crcBuffer);
        output.WriteLine($"Hash bytes (.NET built-in): {BitConverter.ToString(hashBytes)}");

        var crc32c = Crc32C.Compute(crcBuffer);
        output.WriteLine($"CRC32C (custom impl): {crc32c} (0x{crc32c:X8})");

        var crc = Crc32.Compute(crcBuffer);
        output.WriteLine($"Calculated CRC: {crc} (0x{crc:X8})");
        output.WriteLine($"Expected CRC:   {expectedCrc} (0x{expectedCrc:X8})");

        // Calculate batch_len
        var batchLen = 4 + 1 + crcBuffer.Length + 4; // partition_leader_epoch + magic + crcBuffer + crc

        // Write the complete batch
        writer.WriteInt64(0); // base_offset
        writer.WriteInt32(batchLen);
        writer.WriteInt32(-1); // partition_leader_epoch (default is -1!)
        writer.WriteInt8(2); // magic = 2
        writer.WriteUInt32(crc); // CRC
        writer._stream.Write(crcBuffer);

        var batchBytes = writer.ToArray();
        output.WriteLine($"\nComplete batch ({batchBytes.Length} bytes):");
        output.WriteLine(BitConverter.ToString(batchBytes).Replace("-", " "));

        // Verify CRC matches Rust test
        Assert.Equal(expectedCrc, crc);
    }
}
