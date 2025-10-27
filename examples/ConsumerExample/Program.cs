using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

// Create and connect to Fluvio cluster
var options = new FluvioClientOptions(
    Endpoint: "localhost:9003",
    UseTls: false,
    ClientId: "consumer-example"
);

Console.WriteLine("Connecting to Fluvio cluster...");

await using var client = await FluvioClient.ConnectAsync(options);

Console.WriteLine("Connected!");

// Get a consumer
var consumer = client.Consumer(new ConsumerOptions(
    MaxBytes: 1024 * 1024,
    IsolationLevel: IsolationLevel.ReadCommitted
));

Console.WriteLine("\nConsuming messages from 'my-topic' (starting from offset 0)...");
Console.WriteLine("Press Ctrl+C to stop\n");

// Stream messages continuously
await foreach (var record in consumer.StreamAsync("my-topic", partition: 0, offset: 0))
{
    var key = record.Key.HasValue ? Encoding.UTF8.GetString(record.Key.Value.Span) : "(no key)";
    var value = Encoding.UTF8.GetString(record.Value.Span);
    var timestamp = record.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff");

    Console.WriteLine($"[Offset: {record.Offset}] [{timestamp}] Key: {key} | Value: {value}");
}
