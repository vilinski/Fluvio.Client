# Fluvio.Client Architecture

This document describes the architecture and design of the Fluvio.Client library.

## High-Level Architecture

```
┌───────────────────────────────────────────────────────────┐
│                     Application Code                      │
└───────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│                   Public API Layer                        │
│  ┌──────────────┐  ┌───────────────┐ ┌───────────────┐    │
│  │ IFluvioClient│  │IFluvioProducer│ │IFluvioConsumer│    │
│  └──────────────┘  └───────────────┘ └───────────────┘    │
│         │                  │                  │           │
│         └──────────────────┴──────────────────┘           │
│                            │                              │
│                   ┌────────▼────────┐                     │
│                   │  FluvioClient   │                     │
│                   └─────────────────┘                     │
└───────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│                Implementation Layer                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │FluvioProducer│  │FluvioConsumer│  │  FluvioAdmin │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└───────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│                   Network Layer                           │
│              ┌──────────────────────┐                     │
│              │  FluvioConnection    │                     │
│              │  - TCP Client        │                     │
│              │  - TLS Support       │                     │
│              │  - Multiplexing      │                     │
│              └──────────────────────┘                     │
└───────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│                   Protocol Layer                          │
│  ┌──────────────────┐    ┌──────────────────┐             │
│  │FluvioBinaryWriter│    │FluvioBinaryReader│             │
│  │ - Int8/16/32/64  │    │ - Int8/16/32/64  │             │
│  │ - VarInt         │    │ - VarInt         │             │
│  │ - Strings        │    │ - Strings        │             │
│  │ - Bytes          │    │ - Bytes          │             │
│  └──────────────────┘    └──────────────────┘             │
└───────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│                  TCP/IP Network                           │
│                  (Fluvio Cluster)                         │
└───────────────────────────────────────────────────────────┘
```

## Layer Details

### 1. Public API Layer (Abstractions)

**Purpose**: Defines contracts for users of the library

**Components**:
- `IFluvioClient` - Main entry point
- `IFluvioProducer` - Producer operations
- `IFluvioConsumer` - Consumer operations
- `IFluvioAdmin` - Admin operations

**Key Characteristics**:
- Interface-based design
- Immutable configuration objects (records)
- Async/await throughout
- IAsyncDisposable for cleanup

### 2. Implementation Layer

**Purpose**: Implements the business logic for each API

#### Producer
- Encodes messages using binary protocol
- Supports single and batch sends
- Handles delivery guarantees

#### Consumer
- Decodes messages from binary protocol
- Implements streaming (IAsyncEnumerable)
- Manages offsets

#### Admin
- Manages topic lifecycle
- Queries cluster metadata

### 3. Network Layer

**Purpose**: Manages TCP connections and request/response multiplexing

**Key Features**:
```
FluvioConnection
├── TCP Client (with TLS)
├── Background Read Loop
│   └── Routes responses to pending requests
├── Write Lock (SemaphoreSlim)
│   └── Ensures thread-safe writes
└── Pending Requests Dictionary
    └── CorrelationId → TaskCompletionSource
```

**Request Flow**:
```
1. Application calls SendRequestAsync()
2. Generate correlation ID
3. Create TaskCompletionSource
4. Write request to TCP stream
5. Background loop receives response
6. Match correlation ID
7. Complete TaskCompletionSource
8. Return result to application
```

### 4. Protocol Layer

**Purpose**: Handles binary encoding/decoding

**Wire Format**:
```
┌────────────────┐
│  Size (4 bytes)│  ← Big-endian Int32
├────────────────┤
│  API Key (2)   │  ← Request type
├────────────────┤
│  API Ver (2)   │  ← Protocol version
├────────────────┤
│  Corr ID (4)   │  ← Request correlation
├────────────────┤
│  Client ID     │  ← Variable length string
├────────────────┤
│  Request Body  │  ← Request-specific data
└────────────────┘
```

## Data Flow

### Producer Flow

```
Application
    │
    ├─ SendAsync("topic", data, key)
    │
    ▼
FluvioProducer
    │
    ├─ Encode message
    │  ├─ Write topic name
    │  ├─ Write partition
    │  ├─ Write key (nullable)
    │  ├─ Write value
    │  └─ Write timestamp
    │
    ▼
FluvioConnection
    │
    ├─ Generate correlation ID
    ├─ Add to pending requests
    ├─ Write to TCP stream
    │
    ▼
Fluvio Cluster
    │
    ├─ Process request
    │
    ▼
FluvioConnection (Read Loop)
    │
    ├─ Receive response
    ├─ Match correlation ID
    ├─ Complete TaskCompletionSource
    │
    ▼
FluvioProducer
    │
    ├─ Parse response
    ├─ Extract offset
    │
    ▼
Application (receives offset)
```

### Consumer Flow

```
Application
    │
    ├─ StreamAsync("topic", offset: 0)
    │
    ▼
FluvioConsumer
    │
    ├─ Loop continuously
    │  │
    │  ├─ FetchBatchAsync()
    │  │   │
    │  │   ├─ Encode fetch request
    │  │   │  ├─ Write topic name
    │  │   │  ├─ Write partition
    │  │   │  ├─ Write offset
    │  │   │  └─ Write max bytes
    │  │   │
    │  │   ▼
    │  │  FluvioConnection
    │  │   │
    │  │   ├─ Send request
    │  │   ├─ Await response
    │  │   │
    │  │   ▼
    │  │  Decode response
    │  │   │
    │  │   ├─ Read record count
    │  │   ├─ For each record:
    │  │   │  ├─ Read offset
    │  │   │  ├─ Read key
    │  │   │  ├─ Read value
    │  │   │  └─ Read timestamp
    │  │   │
    │  │   ▼
    │  │  Return records
    │  │
    │  └─ Yield each record
    │
    ▼
Application (receives records)
```

## Threading Model

### Connection Management
```
Main Thread
    │
    ├─ Creates FluvioConnection
    ├─ Calls ConnectAsync()
    │   │
    │   └─ Spawns Background Read Loop
    │       │
    │       ├─ Runs in Task.Run()
    │       ├─ Continuously reads from TCP stream
    │       └─ Routes responses to waiting tasks
    │
    └─ Application continues...
```

### Request Handling
```
Thread 1                  Thread 2                  Read Loop Thread
    │                         │                             │
    ├─ SendAsync()            ├─ SendAsync()                │
    │   │                     │   │                         │
    │   ├─ Acquire Lock       │   ├─ Wait for Lock          │
    │   ├─ Write to TCP       │   │                         │
    │   ├─ Release Lock       │   │                         │
    │   │                     │   ├─ Acquire Lock           │
    │   │                     │   ├─ Write to TCP           │
    │   │                     │   ├─ Release Lock           │
    │   │                     │   │                         │
    │   ├─ await Response     │   ├─ await Response         │
    │   │                     │   │                         │
    │   │                     │   │                         ├─ Read Response 1
    │   │                     │   │                         ├─ Complete TCS 1
    │   │                     │   │                         │
    │   ├─ Returns            │   │                         │
    │   │                     │   │                         ├─ Read Response 2
    │   │                     │   │                         ├─ Complete TCS 2
    │   │                     │   │                         │
    │   │                     │   ├─ Returns                │
```

## Error Handling

### Error Flow
```
Application
    │
    ▼
Try/Catch Block
    │
    ▼
Fluvio API Call
    │
    ├─ Network Error?
    │   └─> IOException
    │
    ├─ Protocol Error?
    │   └─> InvalidDataException
    │
    ├─ Fluvio Error?
    │   └─> FluvioException
    │
    └─ Timeout?
        └─> OperationCanceledException
```

### Error Types

1. **FluvioException** - Business logic errors from Fluvio
   - TopicAlreadyExists
   - UnknownTopicOrPartition
   - OffsetOutOfRange

2. **IOException** - Network errors
   - Connection lost
   - Cannot connect

3. **InvalidDataException** - Protocol errors
   - Malformed response
   - Invalid message size

4. **OperationCanceledException** - Timeout/cancellation
   - Request timeout
   - User cancellation

## Resource Management

### IAsyncDisposable Pattern

```
await using var client = await FluvioClient.ConnectAsync();
    │
    ├─ Connection established
    ├─ Read loop started
    │
    ├─ ... use client ...
    │
    └─ DisposeAsync() called
        │
        ├─ Cancel read loop
        ├─ Wait for read loop to finish
        ├─ Dispose TCP client
        ├─ Dispose TLS stream
        └─ Release semaphores
```

## Performance Considerations

### Optimizations

1. **Zero Allocation Binary I/O**
   - Uses `ReadOnlyMemory<byte>` and `Span<byte>`
   - Minimizes memory allocations

2. **Connection Pooling Ready**
   - Single connection per client
   - Can be extended to pool multiple connections

3. **Batch Operations**
   - Send multiple messages in one request
   - Reduce network round trips

4. **Streaming API**
   - IAsyncEnumerable for efficient iteration
   - Back-pressure support

### Potential Improvements

1. **Memory Pooling** - Use ArrayPool for buffers
2. **Pipeline API** - Use System.IO.Pipelines for better throughput
3. **Compression** - Add gzip/snappy support
4. **Connection Pool** - Implement proper connection pooling
5. **Buffer Reuse** - Reuse byte buffers across requests

## Security

### TLS/SSL Support

```
FluvioConnection
    │
    ├─ UseTls = true?
    │   │
    │   ├─ Yes → SslStream
    │   │   │
    │   │   ├─ AuthenticateAsClientAsync()
    │   │   ├─ TLS 1.2/1.3
    │   │   └─ Certificate validation
    │   │
    │   └─ No → NetworkStream
    │
    └─ TCP Client
```

## Testing Strategy

### Unit Tests
- Protocol encoding/decoding
- Binary reader/writer
- Message serialization

### Integration Tests
- Connect to real Fluvio cluster
- End-to-end producer/consumer
- Error scenarios

### Performance Tests
- Throughput benchmarks
- Latency measurements
- Memory profiling

## Future Enhancements

1. **Compression Support**
2. **SmartModules Integration**
3. **Consumer Groups**
4. **Exactly-Once Semantics**
5. **Metrics and Observability**
6. **Advanced Partitioning**
7. **Connection Pooling**
8. **Retry Policies**

---

This architecture provides a solid foundation for a production-ready Fluvio client while maintaining simplicity and clarity.
