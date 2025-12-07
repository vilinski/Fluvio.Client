using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

namespace Fluvio.Client.Benchmarks;

/// <summary>
/// Benchmarks for record serialization and batch encoding performance
/// This tests the public API rather than internal protocol details
/// </summary>
[SimpleJob(RuntimeMoniker.Net80)]
[MemoryDiagnoser]
[MarkdownExporter]
public class ProtocolBenchmarks
{
    private const string TestString = "Hello, Fluvio! This is a test string for benchmarking.";
    private byte[] _smallMessage = null!;
    private byte[] _mediumMessage = null!;
    private byte[] _largeMessage = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Prepare test messages of various sizes
        _smallMessage = Encoding.UTF8.GetBytes("Hello");
        _mediumMessage = Encoding.UTF8.GetBytes(TestString);
        _largeMessage = Encoding.UTF8.GetBytes(string.Join("", Enumerable.Repeat(TestString, 100)));
    }

    [Benchmark(Description = "Create ProduceRecord - small", Baseline = true)]
    public ProduceRecord CreateSmallRecord()
    {
        return new ProduceRecord(_smallMessage);
    }

    [Benchmark(Description = "Create ProduceRecord - medium")]
    public ProduceRecord CreateMediumRecord()
    {
        return new ProduceRecord(_mediumMessage);
    }

    [Benchmark(Description = "Create ProduceRecord - large")]
    public ProduceRecord CreateLargeRecord()
    {
        return new ProduceRecord(_largeMessage);
    }

    [Benchmark(Description = "Create batch of 10 records")]
    public List<ProduceRecord> CreateBatch10()
    {
        return Enumerable.Range(0, 10)
            .Select(i => new ProduceRecord(Encoding.UTF8.GetBytes($"Message {i}")))
            .ToList();
    }

    [Benchmark(Description = "Create batch of 100 records")]
    public List<ProduceRecord> CreateBatch100()
    {
        return Enumerable.Range(0, 100)
            .Select(i => new ProduceRecord(Encoding.UTF8.GetBytes($"Message {i}")))
            .ToList();
    }

    [Benchmark(Description = "Memory copy - 1KB")]
    public byte[] CopyMemory1KB()
    {
        var source = new byte[1024];
        var destination = new byte[1024];
        Array.Copy(source, destination, 1024);
        return destination;
    }

    [Benchmark(Description = "Memory copy - 10KB")]
    public byte[] CopyMemory10KB()
    {
        var source = new byte[10240];
        var destination = new byte[10240];
        Array.Copy(source, destination, 10240);
        return destination;
    }
}
