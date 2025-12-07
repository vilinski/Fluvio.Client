# Fluvio.Client - Pure Managed .NET Client for Fluvio

A pure managed .NET client library for [Fluvio](https://www.fluvio.io/), the distributed streaming platform. This client is built entirely in C# without native dependencies, implementing the Fluvio wire protocol directly.

## Features

- **Pure Managed Implementation** - No native dependencies, works on any .NET platform
- **Producer API** - Send messages to Fluvio topics with at-most-once or at-least-once delivery guarantees
- **Consumer API** - Stream messages from topics with configurable offsets and isolation levels
- **Admin API** - Create, delete, and list topics programmatically

## Installation

```bash
dotnet add package Fluvio.Client
```

## Quick Start

### Producer Example

```csharp
using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

// Connect to Fluvio cluster
await using var client = await FluvioClient.ConnectAsync();

// Get a producer
var producer = client.Producer();

// Send a message
var message = Encoding.UTF8.GetBytes("Hello, Fluvio!");
var offset = await producer.SendAsync("my-topic", message);
Console.WriteLine($"Sent message at offset {offset}");
```

### Consumer Example

```csharp
using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

// Connect to Fluvio cluster
await using var client = await FluvioClient.ConnectAsync();

// Get a consumer
var consumer = client.Consumer();

// Stream messages continuously
await foreach (var record in consumer.StreamAsync("my-topic", offset: 0))
{
    var value = Encoding.UTF8.GetString(record.Value.Span);
    Console.WriteLine($"[Offset: {record.Offset}] {value}");
}
```

### Admin Example

```csharp
using Fluvio.Client;
using Fluvio.Client.Abstractions;

await using var client = await FluvioClient.ConnectAsync();

var admin = client.Admin();

// Create a topic
await admin.CreateTopicAsync("my-topic", new TopicSpec(
    Partitions: 3,
    ReplicationFactor: 1
));

// List all topics
var topics = await admin.ListTopicsAsync();
foreach (var topic in topics)
{
    Console.WriteLine($"Topic: {topic.Name}, Partitions: {topic.Partitions}");
}
```

## API Documentation

### FluvioClient

Main entry point for connecting to a Fluvio cluster.

```csharp
var client = new FluvioClient(new FluvioClientOptions
{
    SpuEndpoint = "localhost:9010",  // SPU for data operations
    ScEndpoint = "localhost:9003",   // SC for admin operations
    UseTls = false,
    ClientId = "my-client",
    ConnectionTimeout = TimeSpan.FromSeconds(30),
    RequestTimeout = TimeSpan.FromSeconds(60),
    EnableAutoReconnect = true,       // Automatic reconnection on failure
    MaxReconnectAttempts = 5,         // Up to 5 reconnection attempts
    ReconnectDelay = TimeSpan.FromSeconds(2)  // 2s base delay with exponential backoff
});

await client.ConnectAsync();
```

### Producer API

```csharp
var producer = client.Producer(new ProducerOptions
{
    DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
    BatchSize = 1000,
    LingerTime = TimeSpan.FromMilliseconds(100)
});

// Send single message
var offset = await producer.SendAsync(
    topic: "my-topic",
    value: messageBytes,
    key: keyBytes  // optional
);

// Send batch
var records = new List<ProduceRecord>
{
    new(Encoding.UTF8.GetBytes("message 1")),
    new(Encoding.UTF8.GetBytes("message 2"))
};
var offsets = await producer.SendBatchAsync("my-topic", records);
```

### Consumer API

```csharp
var consumer = client.Consumer(new ConsumerOptions
{
    MaxBytes = 1024 * 1024,  // 1MB
    IsolationLevel = IsolationLevel.ReadCommitted
});

// Stream continuously
await foreach (var record in consumer.StreamAsync("my-topic", partition: 0, offset: 0))
{
    // Process record
}

// Fetch batch
var records = await consumer.FetchBatchAsync(
    topic: "my-topic",
    partition: 0,
    offset: 0,
    maxBytes: 1024 * 1024
);
```

#### SmartModule Support

Apply transformations and filters using SmartModules:

```csharp
// Using a predefined SmartModule
var consumer = client.Consumer(new ConsumerOptions
{
    SmartModules =
    [
        new SmartModuleInvocation
        {
            Name = "my-filter",  // Name of pre-deployed SmartModule
            Kind = SmartModuleKindType.Filter,
            Parameters = new Dictionary<string, string>
            {
                ["threshold"] = "100"
            }
        }
    ]
});

// Chain multiple SmartModules
var consumer = client.Consumer(new ConsumerOptions
{
    SmartModules =
    [
        new SmartModuleInvocation
        {
            Name = "filter-errors",
            Kind = SmartModuleKindType.Filter
        },
        new SmartModuleInvocation
        {
            Name = "map-to-json",
            Kind = SmartModuleKindType.Map
        }
    ]
});

// Using an aggregate SmartModule with initial accumulator
var consumer = client.Consumer(new ConsumerOptions
{
    SmartModules =
    [
        new SmartModuleInvocation
        {
            Name = "running-sum",
            Kind = SmartModuleKindType.Aggregate,
            Accumulator = Encoding.UTF8.GetBytes("0")  // Initial value
        }
    ]
});
```

### Admin API

```csharp
var admin = client.Admin();

// Create topic
await admin.CreateTopicAsync("my-topic", new TopicSpec
{
    Partitions = 3,
    ReplicationFactor = 1,
    IgnoreRackAssignment = false
});

// Delete topic
await admin.DeleteTopicAsync("my-topic");

// List topics
var topics = await admin.ListTopicsAsync();

// Get specific topic
var topic = await admin.GetTopicAsync("my-topic");
```

## Architecture

This client implements the Fluvio wire protocol directly in managed C#:

1. **Binary Protocol Layer** - Big-endian binary encoding/decoding (Kafka-inspired)
2. **TCP Connection Layer** - Async TCP client with TLS support and connection pooling
3. **Request/Response Handling** - Correlation ID-based multiplexing
4. **High-Level APIs** - Producer, Consumer, and Admin abstractions

## Protocol Compatibility

This client is compatible with Fluvio's wire protocol, which is inspired by Apache Kafka's protocol but optimized for Fluvio's architecture. The protocol uses:

- Big-endian byte order
- Size-prefixed messages
- Correlation ID-based request/response matching
- Binary key/value records

## Requirements

- .NET 8.0 or later
- Fluvio cluster (local or remote)

## Performance

The Fluvio C# client delivers **production-ready performance**:

- **Producer**: 113-115 μs per message, **738,550 msg/s** in batch mode (100 messages)
- **Consumer**: ~1 μs per message with streaming, 0.65 ms for batch of 1000
- **Protocol**: 5 ns record creation, zero-copy design with minimal allocations

See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for detailed performance analysis.

## Observability & Testability

The client includes comprehensive observability features for production deployments:

### Distributed Tracing

Built-in support for distributed tracing using .NET's `ActivitySource` (OpenTelemetry-compatible):

```csharp
// Enable tracing by configuring an ActivityListener or using OpenTelemetry SDK
using var listener = new ActivityListener
{
    ShouldListenTo = source => source.Name == "Fluvio.Client",
    Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllData,
    ActivityStarted = activity => Console.WriteLine($"Started: {activity.OperationName}"),
    ActivityStopped = activity => Console.WriteLine($"Stopped: {activity.OperationName} ({activity.Duration})")
};
ActivitySource.AddActivityListener(listener);

// Operations are automatically traced
var offset = await producer.SendAsync("my-topic", messageBytes);
await foreach (var record in consumer.StreamAsync("my-topic")) { /* ... */ }
```

Trace data includes:
- **Operation context**: Topic, partition, offset, record count
- **Performance metrics**: Request/response sizes, timing
- **Correlation**: Automatic parent-child span relationships
- **Error tracking**: Exception details and error codes


## Development

### Building

```bash
dotnet build
```

### Running Tests

```bash
dotnet test
```

### Running Benchmarks

```bash
# Start Fluvio cluster
fluvio cluster start

# Run performance benchmarks
cd benchmarks/Fluvio.Client.Benchmarks
dotnet run -c Release
```

### Running Examples

```bash
# Run producer example
dotnet run --project examples/ProducerExample

# Run consumer example (in another terminal)
dotnet run --project examples/ConsumerExample
```

## Documentation

- [GETTING_STARTED.md](GETTING_STARTED.md) - Quick start guide and examples
- [ARCHITECTURE.md](ARCHITECTURE.md) - System design and architecture details
- [PROTOCOL_REFERENCE.md](PROTOCOL_REFERENCE.md) - Wire protocol implementation details
- [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) - Performance benchmarks and analysis
- [TODO.md](TODO.md) - Feature roadmap and Rust parity tracking

## Limitations

**Important Note**: This is an initial implementation that demonstrates the feasibility of a pure managed .NET client for Fluvio. The actual Fluvio wire protocol may differ from this implementation, which is based on publicly available documentation and the Kafka-inspired protocol structure.

For production use, you may need to:

1. Verify protocol compatibility with your Fluvio version
2. Add more robust error handling
3. Implement additional features like topic mirroring, etc.
4. Add comprehensive integration tests with a real Fluvio cluster

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

MIT License - See LICENSE file for details

## Acknowledgments

- [Fluvio](https://www.fluvio.io/) - The distributed streaming platform
- [InfinyOn](https://infinyon.com/) - Creators of Fluvio

## Disclaimer

This is an unofficial client implementation. For official client libraries, please visit the [Fluvio documentation](https://www.fluvio.io/docs/).
