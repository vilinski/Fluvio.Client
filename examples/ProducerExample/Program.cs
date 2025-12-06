using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

Console.WriteLine("Connecting to Fluvio cluster...");

await using var client = await FluvioClient.ConnectAsync();

Console.WriteLine("Connected!");

// Create a topic first (if it doesn't exist)
var admin = client.Admin();
try
{
    await admin.CreateTopicAsync("my-topic");
    Console.WriteLine("Topic 'my-topic' created");
}
catch (FluvioException ex) when (ex.Message.Contains("TopicAlreadyExists"))
{
    Console.WriteLine("Topic 'my-topic' already exists");
}

// Get a producer
var producer = client.Producer();

// Send some messages
Console.WriteLine("\nSending messages...");

for (var i = 0; i < 10; i++)
{
    var message = $"Hello, Fluvio! Message #{i}";
    var messageBytes = Encoding.UTF8.GetBytes(message);
    var key = Encoding.UTF8.GetBytes($"key-{i}");

    var offset = await producer.SendAsync("my-topic", messageBytes, key);
    Console.WriteLine($"Sent message #{i} at offset {offset}");

    await Task.Delay(1000); // Wait 1 second between messages
}

// Send a batch of messages
Console.WriteLine("\nSending batch of messages...");

var batchRecords = new List<ProduceRecord>();
for (var i = 10; i < 15; i++)
{
    var message = $"Batch message #{i}";
    var messageBytes = Encoding.UTF8.GetBytes(message);
    batchRecords.Add(new ProduceRecord(messageBytes));
}

var offsets = await producer.SendBatchAsync("my-topic", batchRecords);
Console.WriteLine($"Sent batch of {batchRecords.Count} messages, offsets: {string.Join(", ", offsets)}");

await producer.FlushAsync();

Console.WriteLine("\nAll messages sent successfully!");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();
