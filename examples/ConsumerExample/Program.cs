using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

// Connect to Fluvio cluster using config from ~/.fluvio/config
Console.WriteLine("Connecting to Fluvio cluster...");

await using var client = await FluvioClient.ConnectAsync();

Console.WriteLine("Connected!");

var consumer = client.Consumer();

Console.WriteLine("\nConsuming messages from 'my-topic' (starting from offset 0)...");
Console.WriteLine("Press Ctrl+C to stop\n");

await foreach (var record in consumer.StreamAsync("my-topic", partition: 0, offset: 0))
{
    var key = record.Key.HasValue ? Encoding.UTF8.GetString(record.Key.Value.Span) : "(no key)";
    var value = Encoding.UTF8.GetString(record.Value.Span);
    var timestamp = record.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff");

    Console.WriteLine($"[Offset: {record.Offset}] [{timestamp}] Key: {key} | Value: {value}");
}
