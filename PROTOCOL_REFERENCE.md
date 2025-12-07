# Fluvio Protocol Reference

This document contains the exact protocol structures from the Fluvio Rust source code.

## Sources

All information is from the official Fluvio Rust implementation:
- Repository: https://github.com/infinyon/fluvio
- Protocol crate: `fluvio-protocol`
- SPU schema crate: `fluvio-spu-schema`

## ProduceRequest Structure

**Source**: `crates/fluvio-spu-schema/src/produce/request.rs`

```rust
pub struct ProduceRequest<R> {
    pub transactional_id: Option<String>,  // min_version = 3
    pub isolation: Isolation,
    pub timeout: Duration,
    pub topics: Vec<TopicProduceData<R>>,
    pub smartmodules: Vec<SmartModuleInvocation>,
    data: PhantomData<R>,
}
```

**API Details**:
- API_KEY: 0
- DEFAULT_API_VERSION: 7 (COMMON_VERSION)

**Field encoding order**:
1. `transactional_id` - Option<String> (0x00 for None, 0x01 + string for Some)
2. `isolation` - i16 (1 = ReadUncommitted, -1 = ReadCommitted)
3. `timeout` - i32 (milliseconds)
4. `topics` - i32 count + Vec<TopicProduceData>
5. `smartmodules` - i32 count + Vec<SmartModuleInvocation>

### Isolation Values

```rust
pub enum Isolation {
    ReadUncommitted = 1,   // Just wait for leader to write message
    ReadCommitted = -1,    // Wait for messages to be committed
}
```

**Wire format**: i16 value

## TopicProduceData Structure

```rust
pub struct TopicProduceData<R> {
    pub name: String,
    pub partitions: Vec<PartitionProduceData<R>>,
}
```

**Encoding**:
1. `name` - String (i16 length + UTF-8 bytes)
2. `partitions` - i32 count + Vec<PartitionProduceData>

## PartitionProduceData Structure

```rust
pub struct PartitionProduceData<R> {
    pub partition_index: PartitionId,  // i32
    pub records: R,                     // RecordBatch
}
```

**Encoding**:
1. `partition_index` - i32
2. `records` - RecordBatch (see below)

## RecordBatch Structure

**Source**: `crates/fluvio-protocol/src/record/batch.rs`

```rust
pub struct Batch<R = MemoryRecords> {
    pub base_offset: Offset,           // i64
    pub batch_len: i32,                // calculated, not stored
    pub header: BatchHeader,
    pub schema_id: SchemaId,           // only if header.has_schema()
    records: R,
}
```

### BatchHeader Structure

```rust
pub struct BatchHeader {
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_time_stamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub first_sequence: i32,
}
```

### RecordBatch Encoding Order

**From `Encoder::encode` implementation**:

```rust
fn encode<T>(self, dest: &mut T, version: Version) -> Result<(), Error>
where T: BufMut,
{
    // 1. Write preamble
    self.base_offset.encode(dest, version)?;
    let batch_len: i32 = self.calc_batch_len();
    batch_len.encode(dest, version)?;

    // 2. Write pre-CRC header fields
    self.header.partition_leader_epoch.encode(dest, version)?;
    self.header.magic.encode(dest, version)?;

    // 3. Build CRC-protected buffer
    let mut out: Vec<u8> = Vec::new();
    let buf = &mut out;
    self.header.attributes.encode(buf, version)?;
    self.header.last_offset_delta.encode(buf, version)?;
    self.header.first_timestamp.encode(buf, version)?;
    self.header.max_time_stamp.encode(buf, version)?;
    self.header.producer_id.encode(buf, version)?;
    self.header.producer_epoch.encode(buf, version)?;
    self.header.first_sequence.encode(buf, version)?;
    if self.header.has_schema() {
        self.schema_id.encode(buf, version)?;
    }
    self.records.encode(buf, version)?;

    // 4. Compute and write CRC
    let crc = crc32c::crc32c(&out);
    crc.encode(dest, version)?;

    // 5. Write the CRC-protected buffer
    dest.put_slice(&out);
    Ok(())
}
```

**Wire format order**:
1. `base_offset` (i64) - 8 bytes
2. `batch_len` (i32) - 4 bytes
3. `partition_leader_epoch` (i32) - 4 bytes
4. `magic` (i8) - 1 byte
5. `crc` (u32) - 4 bytes **[CRC of items 6-13]**
6. `attributes` (i16) - 2 bytes
7. `last_offset_delta` (i32) - 4 bytes
8. `first_timestamp` (i64) - 8 bytes
9. `max_time_stamp` (i64) - 8 bytes
10. `producer_id` (i64) - 8 bytes
11. `producer_epoch` (i16) - 2 bytes
12. `first_sequence` (i32) - 4 bytes
13. `schema_id` (u32) - 4 bytes **[only if has_schema()]**
14. `records` (variable) - encoded records

**batch_len calculation**: Size of items 3-14 (everything after batch_len itself)

**CRC calculation**: CRC32-C (Castagnoli) over items 6-14 (buffer contents)

## Record Structure

**Source**: `crates/fluvio-protocol/src/record/data.rs`

```rust
pub struct Record<B = RecordData> {
    pub preamble: RecordHeader,
    pub key: Option<B>,
    pub value: B,
    pub headers: i64,
}
```

### RecordHeader Structure

```rust
pub struct RecordHeader {
    attributes: i8,
    #[varint]
    timestamp_delta: Timestamp,  // i64 varint
    #[varint]
    offset_delta: Offset,        // i64 varint
}
```

### Record Encoding Order

**From `Encoder::encode` implementation**:

```rust
fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
where T: BufMut,
{
    // Build record content
    let mut out: Vec<u8> = Vec::new();
    self.preamble.encode(&mut out, version)?;  // RecordHeader
    self.key.encode(&mut out, version)?;        // Option<B>
    self.value.encode(&mut out, version)?;      // B (RecordData)
    self.headers.encode_varint(&mut out)?;      // i64 varint

    // Write length prefix + content
    let len: i64 = out.len() as i64;
    len.encode_varint(dest)?;  // i64 varint!
    dest.put_slice(&out);
    Ok(())
}
```

**Wire format order**:
1. `record_length` (i64 varint) - variable size
2. `attributes` (i8) - 1 byte
3. `timestamp_delta` (i64 varint) - variable size
4. `offset_delta` (i64 varint) - variable size
5. `key` (Option<RecordData>) - see below
6. `value` (RecordData) - see below
7. `headers` (i64 varint) - count

### Option<RecordData> Encoding (key field)

**Source**: Comments in `data.rs`

```
A key is encoded with a tag for Some or None:
- 0x00 represents None
- 0x01 represents Some

When Some:
  0x01 + <RecordData encoding>
```

### RecordData Encoding (value field)

**Source**: `crates/fluvio-protocol/src/record/data.rs`

```rust
fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
where T: BufMut,
{
    let len: i64 = self.0.len() as i64;  // Length as i64!
    len.encode_varint(dest)?;             // i64 varint encoding
    for v in self.0.iter() {
        v.encode(dest, version)?;         // Write bytes
    }
    Ok(())
}
```

**Wire format**:
1. `length` (i64 varint) - size of byte array
2. `bytes` - raw bytes

## Varint Encoding

**Source**: `fluvio-protocol` crate

### Varint (i32)

ZigZag encoding:
- Positive: `n * 2`
- Negative: `(n * -2) - 1`

Then write as unsigned varint (7 bits per byte, MSB continuation bit).

### VarLong (i64)

Same ZigZag encoding as varint, but for i64 values.

## CRC32-C

**Algorithm**: CRC-32C (Castagnoli polynomial)
**Library**: `crc32c` crate in Rust
**C# Equivalent**: `System.IO.Hashing.Crc32` (uses Castagnoli)

## ProduceResponse Structure

**Source**: `crates/fluvio-spu-schema/src/produce/response.rs`

```rust
pub struct ProduceResponse {
    pub responses: Vec<TopicProduceResponse>,
    #[fluvio(min_version = 1, ignorable)]
    pub throttle_time_ms: i32,
}
```

### TopicProduceResponse

```rust
pub struct TopicProduceResponse {
    pub name: String,
    pub partitions: Vec<PartitionProduceResponse>,
}
```

### PartitionProduceResponse

```rust
pub struct PartitionProduceResponse {
    pub partition_index: PartitionId,          // i32
    pub error_code: ErrorCode,                  // i16
    pub base_offset: i64,
    #[fluvio(min_version = 2, ignorable)]
    pub log_append_time_ms: i64,
    #[fluvio(min_version = 5, ignorable)]
    pub log_start_offset: i64,
}
```

**Response encoding (API version 7)**:
1. `responses` count (i32)
2. For each TopicProduceResponse:
   - `name` (String)
   - `partitions` count (i32)
   - For each PartitionProduceResponse:
     - `partition_index` (i32)
     - `error_code` (i16)
     - `base_offset` (i64)
     - `log_append_time_ms` (i64) - version 2+
     - `log_start_offset` (i64) - version 5+
3. `throttle_time_ms` (i32) - version 1+

## Error Codes

Common error codes:
- `0` - None (success)
- `6` - TopicNotFound

## Data Types

### Primitive Types (Big-Endian)
- `i8` - 1 byte signed
- `i16` - 2 bytes signed, big-endian
- `i32` - 4 bytes signed, big-endian
- `i64` - 8 bytes signed, big-endian
- `u32` - 4 bytes unsigned, big-endian

### String Encoding
- `i16` length prefix (big-endian)
- UTF-8 bytes

### Option<String> Encoding
- `0x00` for None
- `0x01` + String for Some

### Vec<T> Encoding
- `i32` count (big-endian)
- Elements

## Example: Complete ProduceRequest

```
// ProduceRequest
00                          // transactional_id: None
00 01                       // isolation: ReadUncommitted (1)
00 00 75 30                 // timeout: 30000ms
00 00 00 01                 // topics: count=1

// TopicProduceData
00 0C                       // name length: 12
74 65 73 74 2D 70 72 6F 64 75 63 65  // "test-produce"
00 00 00 01                 // partitions: count=1

// PartitionProduceData
00 00 00 00                 // partition_index: 0

// RecordBatch
00 00 00 00 00 00 00 00     // base_offset: 0
00 00 00 XX                 // batch_len: calculated
00 00 00 00                 // partition_leader_epoch: 0
02                          // magic: 2
XX XX XX XX                 // crc: calculated
00 00                       // attributes: 0
00 00 00 00                 // last_offset_delta: 0
XX XX XX XX XX XX XX XX     // first_timestamp: now
XX XX XX XX XX XX XX XX     // max_time_stamp: now
FF FF FF FF FF FF FF FF     // producer_id: -1
FF FF                       // producer_epoch: -1
00 00 00 00                 // first_sequence: 0

// Record
XX                          // record_length (varint)
00                          // attributes: 0
00                          // timestamp_delta: 0 (varint)
00                          // offset_delta: 0 (varint)
00                          // key: None
XX                          // value length (varint)
...                         // value bytes
00                          // headers: 0 (varint)

// SmartModules
00 00 00 00                 // count: 0
```

---

**Date**: 2025-10-27
**Based on**: Fluvio master branch (latest)
**Verified by**: Direct examination of Rust source code
