using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Benchmarks;

/// <summary>
/// Benchmarks comparing polling-based consumer vs. streaming consumer performance
/// </summary>
[SimpleJob(RuntimeMoniker.Net80)]
[MemoryDiagnoser]
[MarkdownExporter]
public class ConsumerBenchmarks
{
    private FluvioClient? _client;
    private IFluvioConsumer? _consumer;
    private string? _topicName;
    private const int MessageCount = 1000;

    [GlobalSetup]
    public async Task Setup()
    {
        var options = new FluvioClientOptions(
            SpuEndpoint: "localhost:9010",
            UseTls: false,
            ClientId: "benchmark-consumer"
        );

        _client = await FluvioClient.ConnectAsync(options);
        _consumer = _client.Consumer();

        // Create benchmark topic and populate with messages
        _topicName = $"benchmark-consumer-{Guid.NewGuid():N}"[..20];
        var admin = _client.Admin();
        await admin.CreateTopicAsync(_topicName, new TopicSpec(Partitions: 1, ReplicationFactor: 1));

        // Pre-populate topic with test messages
        var producer = _client.Producer();
        for (int i = 0; i < MessageCount; i++)
        {
            await producer.SendAsync(_topicName, Encoding.UTF8.GetBytes($"Benchmark message {i}"));
        }

        // Wait for messages to be committed
        await Task.Delay(1000);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_client != null && _topicName != null)
        {
            try
            {
                var admin = _client.Admin();
                await admin.DeleteTopicAsync(_topicName);
            }
            catch
            {
                // Best effort
            }

            await _client.DisposeAsync();
        }
    }

    [Benchmark(Description = "Streaming consumer - 1000 messages", Baseline = true)]
    public async Task<int> StreamingConsumer1000Messages()
    {
        var count = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var record in _consumer!.StreamAsync(_topicName!, 0, 0, cts.Token))
        {
            count++;
            if (count >= MessageCount)
                break;
        }

        return count;
    }

    [Benchmark(Description = "Fetch batch - 1000 messages")]
    public async Task<int> FetchBatch1000Messages()
    {
        var count = 0;
        long currentOffset = 0;

        while (count < MessageCount)
        {
            var records = await _consumer!.FetchBatchAsync(_topicName!, partition: 0, offset: currentOffset);
            if (records.Count == 0)
                break;

            count += records.Count;
            currentOffset = records[^1].Offset + 1;
        }

        return count;
    }

    [Benchmark(Description = "Streaming consumer - 100 messages")]
    public async Task<int> StreamingConsumer100Messages()
    {
        var count = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await foreach (var record in _consumer!.StreamAsync(_topicName!, 0, 0, cts.Token))
        {
            count++;
            if (count >= 100)
                break;
        }

        return count;
    }

    [Benchmark(Description = "Fetch batch - 100 messages")]
    public async Task<int> FetchBatch100Messages()
    {
        var count = 0;
        long currentOffset = 0;

        while (count < 100)
        {
            var records = await _consumer!.FetchBatchAsync(_topicName!, partition: 0, offset: currentOffset);
            if (records.Count == 0)
                break;

            count += records.Count;
            currentOffset = records[^1].Offset + 1;
        }

        return count;
    }
}
