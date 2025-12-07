# Getting Started with Fluvio.Client

This guide will help you get started with the Fluvio.Client .NET library.

## Prerequisites

1. **.NET 8.0 SDK** or later
2. **Fluvio cluster** running locally or remotely

### Installing Fluvio

If you don't have Fluvio installed:

```bash
# Install Fluvio CLI
curl -fsS https://hub.infinyon.cloud/install/install.sh | bash

# Start local Fluvio cluster
fluvio cluster start

# Verify installation
fluvio version
```

## Installation

### From NuGet (when published)

```bash
dotnet add package Fluvio.Client
```

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/fluvio-dotnet-client.git
cd fluvio-dotnet-client

# Build the solution
dotnet build

# Add local reference to your project
dotnet add reference path/to/Fluvio.Client/Fluvio.Client.csproj
```

## Basic Usage

### 1. Create and Connect to Fluvio

```csharp
using Fluvio.Client;
using Fluvio.Client.Abstractions;

// Connect using config from ~/.fluvio/config (active profile)
await using var client = await FluvioClient.ConnectAsync();

// Or specify a different profile
await using var client = await FluvioClient.ConnectAsync(
    new FluvioClientOptions(Profile: "cloud")
);

// Or with fully custom options (overrides config)
var options = new FluvioClientOptions(
    Endpoint: "localhost:9010",
    ScEndpoint: "localhost:9003",
    UseTls: false,
    ClientId: "my-app"
);
await using var client = await FluvioClient.ConnectAsync(options);
```

**Configuration Priority:**
1. Explicitly provided options
2. `~/.fluvio/config` (active profile or specified profile)
3. Defaults (localhost:9010 for SPU, localhost:9003 for SC, TLS off)

### 2. Create a Topic

```csharp
var admin = client.Admin();

// Create a topic with defaults (1 partition, replication factor 1)
await admin.CreateTopicAsync("my-topic");

// Or create with custom settings
await admin.CreateTopicAsync("large-topic", new TopicSpec(
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

### 3. Produce Messages

```csharp
using System.Text;

var producer = client.Producer();

// Send a single message
var message = Encoding.UTF8.GetBytes("Hello, Fluvio!");
var offset = await producer.SendAsync("my-topic", message);
Console.WriteLine($"Message sent at offset: {offset}");

// Send a message with a key
var key = Encoding.UTF8.GetBytes("user-123");
var value = Encoding.UTF8.GetBytes("User login event");
await producer.SendAsync("my-topic", value, key);

// Send multiple messages in a batch
var records = new List<ProduceRecord>
{
    new(Encoding.UTF8.GetBytes("Message 1")),
    new(Encoding.UTF8.GetBytes("Message 2")),
    new(Encoding.UTF8.GetBytes("Message 3"))
};
var offsets = await producer.SendBatchAsync("my-topic", records);
```

### 4. Consume Messages

```csharp
// Create consumer with defaults
var consumer = client.Consumer();

// Stream messages continuously from beginning
await foreach (var record in consumer.StreamAsync("my-topic", partition: 0, offset: 0))
{
    var value = Encoding.UTF8.GetString(record.Value.Span);
    Console.WriteLine($"[{record.Offset}] {value}");
}

// Or fetch a batch of messages
var records = await consumer.FetchBatchAsync("my-topic", partition: 0, offset: 0);
foreach (var record in records)
{
    var value = Encoding.UTF8.GetString(record.Value.Span);
    Console.WriteLine($"[{record.Offset}] {value}");
}
```

## Advanced Usage

### Producer with Configuration

```csharp
var producer = client.Producer(new ProducerOptions(
    BatchSize: 1000,
    MaxRequestSize: 1024 * 1024,  // 1 MB (matches Rust client)
    LingerTime: TimeSpan.FromMilliseconds(100),
    Timeout: TimeSpan.FromSeconds(5),  // Default: 5 seconds
    DeliveryGuarantee: DeliveryGuarantee.AtLeastOnce
));

// Send messages...
await producer.SendAsync("my-topic", messageBytes);

// Flush any pending messages
await producer.FlushAsync();
```

**Producer Options:**
- `BatchSize`: Number of records per batch (default: 1000)
- `MaxRequestSize`: Max size in bytes for a single request (default: 1 MB)
- `LingerTime`: How long to wait for more records before sending (default: 0)
- `Timeout`: Request timeout (default: 5 seconds)
- `DeliveryGuarantee`: At least once or at most once (default: AtLeastOnce)

### Consumer with Configuration

```csharp
var consumer = client.Consumer(new ConsumerOptions(
    MaxBytes: 1024 * 1024,  // 1 MB batch size (default)
    IsolationLevel: IsolationLevel.ReadCommitted  // Default
));

// Stream from specific offset
await foreach (var record in consumer.StreamAsync("my-topic", partition: 0, offset: 100))
{
    // Process record...
}
```

**Consumer Options:**
- `MaxBytes`: Maximum bytes to fetch per request (default: 1 MB)
- `IsolationLevel`: Read committed or read uncommitted (default: ReadCommitted)

### Working with JSON Messages

```csharp
using System.Text.Json;

// Define your message type
public record UserEvent(string UserId, string Action, DateTime Timestamp);

// Produce JSON messages
var producer = client.Producer();
var userEvent = new UserEvent("user-123", "login", DateTime.UtcNow);
var json = JsonSerializer.Serialize(userEvent);
var bytes = Encoding.UTF8.GetBytes(json);
await producer.SendAsync("events", bytes);

// Consume JSON messages
var consumer = client.Consumer();
await foreach (var record in consumer.StreamAsync("events", offset: 0))
{
    var json = Encoding.UTF8.GetString(record.Value.Span);
    var userEvent = JsonSerializer.Deserialize<UserEvent>(json);
    Console.WriteLine($"User {userEvent.UserId} performed {userEvent.Action}");
}
```

### Error Handling

```csharp
try
{
    // Create topic with defaults
    await admin.CreateTopicAsync("my-topic");
}
catch (FluvioException ex)
{
    if (ex.Message.Contains("TopicAlreadyExists"))
    {
        Console.WriteLine("Topic already exists, continuing...");
    }
    else
    {
        throw;
    }
}
```

## Running the Examples

This repository includes example projects:

```bash
# Terminal 1: Run the producer
cd examples/ProducerExample
dotnet run

# Terminal 2: Run the consumer
cd examples/ConsumerExample
dotnet run
```

## Testing with Fluvio CLI

You can verify your .NET client is working correctly by using the Fluvio CLI:

```bash
# Create a topic
fluvio topic create test-topic

# Produce messages from your .NET app
# Then consume them with the CLI
fluvio consume test-topic -B

# Or produce with CLI and consume with .NET app
echo "Hello from CLI" | fluvio produce test-topic
```

## Troubleshooting

### Connection Issues

If you can't connect to Fluvio:

```bash
# Check if Fluvio is running
fluvio version

# Check cluster status
fluvio cluster status

# Verify the endpoint
fluvio profile current
```

### Protocol Compatibility

This is an initial implementation based on publicly available documentation. If you encounter protocol incompatibilities:

1. Check your Fluvio version: `fluvio version`
2. Report issues with details about your Fluvio version
3. Consider using the official Fluvio clients for production use

## Next Steps

- Read the [API Documentation](README.md#api-documentation)
- Check out the [example projects](examples/)
- Learn about [Fluvio concepts](https://www.fluvio.io/docs/fluvio/concepts/)

## Getting Help

- [Fluvio Documentation](https://www.fluvio.io/docs/)
- [Fluvio Discord](https://discord.gg/infinyon)
- [GitHub Issues](https://github.com/yourusername/fluvio-dotnet-client/issues)
