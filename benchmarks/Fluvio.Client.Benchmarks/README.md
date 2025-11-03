# Fluvio.Client Benchmarks

Performance benchmarks for the Fluvio C# client library using BenchmarkDotNet.

## Prerequisites

1. **Fluvio Cluster**: Ensure a local Fluvio cluster is running
   ```bash
   fluvio cluster start
   # or
   fluvio cluster resume
   ```

2. **Clean State**: Clean up any existing benchmark topics
   ```bash
   fluvio topic list | grep benchmark | awk '{print $1}' | xargs -I {} fluvio topic delete {}
   ```

## Running Benchmarks

### All Benchmarks
```bash
cd benchmarks/Fluvio.Client.Benchmarks
dotnet run -c Release
```

### Specific Benchmark Classes
```bash
# Producer benchmarks only
dotnet run -c Release --filter "*ProducerBenchmarks*"

# Consumer benchmarks only
dotnet run -c Release --filter "*ConsumerBenchmarks*"

# Protocol benchmarks only
dotnet run -c Release --filter "*ProtocolBenchmarks*"
```

### Specific Methods
```bash
# Single benchmark method
dotnet run -c Release --filter "*ProducerBenchmarks.SendSingleSmallMessage*"
```

## Benchmark Categories

### 1. ProducerBenchmarks
Measures producer throughput and latency:
- Single message send (small, medium, large)
- Batch sending (10, 100 messages)
- Sequential vs. batch comparison

**Metrics**:
- Messages per second
- Latency (mean, median, p95, p99)
- Memory allocation

### 2. ConsumerBenchmarks
Compares polling vs. streaming consumer performance:
- Streaming consumer (100, 1000 messages)
- Fetch batch consumer (100, 1000 messages)
- Latency comparison

**Expected Results**:
- Streaming: ~5ms latency per batch
- Polling: ~100-200ms latency per batch
- **20-50x performance improvement** with streaming

### 3. ProtocolBenchmarks
Measures record creation and batch encoding performance:
- ProduceRecord creation (small, medium, large messages)
- Batch creation (10, 100 records)
- Memory copy operations

**Target Performance**:
- < 1μs for record creation
- < 100μs for batch of 100 records
- Minimal memory allocation

## Results

Benchmark results are saved to `BenchmarkDotNet.Artifacts/results/` in markdown format.

### Expected Performance Goals

| Benchmark | Target | Notes |
|-----------|--------|-------|
| Producer throughput | > 10,000 msg/sec | Single partition |
| Streaming consumer | < 5ms per batch | Zero polling delay |
| Polling consumer | ~100-200ms per batch | Baseline comparison |
| Record creation | < 1μs | Per record |
| Batch creation (100) | < 100μs | Minimal allocation |

## Interpreting Results

### Producer Performance
```
| Method                    | Mean     | Error    | StdDev   | Allocated |
|-------------------------- |---------:|---------:|---------:|----------:|
| SendSingleSmallMessage    | 1.234 ms | 0.012 ms | 0.011 ms |     256 B |
| SendBatch100Messages      | 10.45 ms | 0.105 ms | 0.098 ms |   5,120 B |
| SendSequential100Messages | 125.6 ms | 1.234 ms | 1.153 ms |  25,600 B |
```

**Analysis**:
- Batching shows ~12x improvement vs. sequential
- Low memory allocation indicates efficient implementation

### Consumer Performance
```
| Method                         | Mean      | Ratio | Allocated |
|------------------------------- |----------:|------:|----------:|
| StreamingConsumer1000Messages  | 50.12 ms  | 1.00  | 64.5 KB   |
| FetchBatch1000Messages         | 2,345 ms  | 46.8  | 128 KB    |
```

**Analysis**:
- **46x faster** with streaming consumer
- Confirms zero-polling performance advantage
- Lower memory allocation with streaming

## Troubleshooting

### Cluster Connection Issues
```bash
# Check cluster status
fluvio cluster status

# Restart if needed
fluvio cluster shutdown
fluvio cluster resume
```

### Topic Cleanup
```bash
# Clean up benchmark topics
fluvio topic list | grep benchmark | awk '{print $1}' | xargs -I {} fluvio topic delete {}
```

### Build Issues
```bash
# Clean and rebuild
dotnet clean
dotnet build -c Release
```

## Continuous Benchmarking

For tracking performance over time:

1. **Baseline**: Run benchmarks before changes
2. **Compare**: Run after changes with `--filter *`
3. **Analyze**: Look for regressions > 10%
4. **Document**: Save results to git for history

## Advanced Options

### Memory Profiling
```bash
dotnet run -c Release --filter "*" --memory
```

### Export Results
```bash
dotnet run -c Release --exporters json,html,markdown
```

### Custom Parameters
```bash
# Run with warmup and iterations
dotnet run -c Release -- --warmupCount 3 --iterationCount 10
```

## Contributing

When adding new benchmarks:
1. Follow existing pattern (Setup, Cleanup, [Benchmark])
2. Use meaningful descriptions
3. Include baseline comparisons
4. Document expected performance
5. Clean up resources properly

## References

- [BenchmarkDotNet Documentation](https://benchmarkdotnet.org/)
- [Fluvio Documentation](https://www.fluvio.io/docs/)
- [Performance Best Practices](https://learn.microsoft.com/en-us/dotnet/core/testing/performance-testing)
