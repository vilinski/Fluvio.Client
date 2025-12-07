using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Benchmarks;

/// <summary>
/// Benchmarks for producer throughput and latency
/// </summary>
[SimpleJob(RuntimeMoniker.Net80)]
[MemoryDiagnoser]
[MarkdownExporter]
public class ProducerBenchmarks
{
    private FluvioClient? _client;
    private IFluvioProducer? _producer;
    private string? _topicName;
    private readonly byte[] _smallMessage = Encoding.UTF8.GetBytes("Hello, Fluvio!");
    private readonly byte[] _mediumMessage = new byte[1024]; // 1KB
    private readonly byte[] _largeMessage = new byte[10 * 1024]; // 10KB

    [GlobalSetup]
    public async Task Setup()
    {
        // Connect to local Fluvio cluster
        var options = new FluvioClientOptions(
            SpuEndpoint: "localhost:9010",
            ScEndpoint: "localhost:9003",
            UseTls: false,
            ClientId: "benchmark-producer"
        );

        _client = await FluvioClient.ConnectAsync(options);
        _producer = _client.Producer();

        // Create benchmark topic
        _topicName = $"benchmark-producer-{Guid.NewGuid():N}"[..20];
        var admin = _client.Admin();
        await admin.CreateTopicAsync(_topicName, new TopicSpec(Partitions: 1, ReplicationFactor: 1));

        // Initialize message payloads
        Array.Fill(_mediumMessage, (byte)'M');
        Array.Fill(_largeMessage, (byte)'L');
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

    [Benchmark(Description = "Single small message (14 bytes)")]
    public async Task<long> SendSingleSmallMessage()
    {
        return await _producer!.SendAsync(_topicName!, _smallMessage);
    }

    [Benchmark(Description = "Single medium message (1 KB)")]
    public async Task<long> SendSingleMediumMessage()
    {
        return await _producer!.SendAsync(_topicName!, _mediumMessage);
    }

    [Benchmark(Description = "Single large message (10 KB)")]
    public async Task<long> SendSingleLargeMessage()
    {
        return await _producer!.SendAsync(_topicName!, _largeMessage);
    }

    [Benchmark(Description = "Batch of 10 messages")]
    public async Task<IReadOnlyList<long>> SendBatch10Messages()
    {
        var records = Enumerable.Range(0, 10)
            .Select(i => new ProduceRecord(Encoding.UTF8.GetBytes($"Batch message {i}")))
            .ToList();

        return await _producer!.SendBatchAsync(_topicName!, records);
    }

    [Benchmark(Description = "Batch of 100 messages")]
    public async Task<IReadOnlyList<long>> SendBatch100Messages()
    {
        var records = Enumerable.Range(0, 100)
            .Select(i => new ProduceRecord(Encoding.UTF8.GetBytes($"Batch message {i}")))
            .ToList();

        return await _producer!.SendBatchAsync(_topicName!, records);
    }

    [Benchmark(Description = "Sequential send 100 messages", Baseline = true)]
    public async Task SendSequential100Messages()
    {
        for (int i = 0; i < 100; i++)
        {
            await _producer!.SendAsync(_topicName!, _smallMessage);
        }
    }
}
