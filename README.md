# Fluvio.Client - .NET Client for Fluvio

[![Build Status](https://github.com/vilinski/Fluvio.Client/actions/workflows/build.yml/badge.svg)](https://github.com/vilinski/Fluvio.Client/actions/workflows/build.yml)
[![NuGet](https://img.shields.io/nuget/v/Fluvio.Client.svg)](https://www.nuget.org/packages/Fluvio.Client)

A .NET client library for [Fluvio](https://www.fluvio.io/), the distributed streaming platform.
It wraps the official [`fluvio`](https://crates.io/crates/fluvio) Rust client through a thin,
hand-written Rust FFI layer, so protocol handling, retries, and cluster discovery are all
delegated to the same client the Fluvio project itself maintains. See
[docs/architecture.md](docs/architecture.md) for how the FFI layer works.

## Features

- **Producer API** - Send single records, batches, and route across partitions with a custom
  `IPartitioner`
- **Consumer API** - Stream records via `IAsyncEnumerable`, or fetch bounded batches, with
  offset persistence (manual or auto-commit)
- **Admin API** - Create, delete, list, and inspect topics programmatically
- **Cross-platform** - NuGet package bundles native binaries for linux-x64, osx-arm64, osx-x64,
  and win-x64

## Installation

```bash
dotnet add package Fluvio.Client
```

## Quick Start

```csharp
using System.Text;
using Fluvio.Client;
using Fluvio.Client.Abstractions;

await using var client = await FluvioClient.ConnectAsync();

var producer = client.Producer();
var offset = await producer.SendAsync("my-topic", Encoding.UTF8.GetBytes("Hello, Fluvio!"));

var consumer = client.Consumer();
await foreach (var record in consumer.StreamAsync("my-topic", offset: 0))
{
    Console.WriteLine($"[{record.Offset}] {Encoding.UTF8.GetString(record.Value.Span)}");
    break;
}
```

For a full walkthrough (topics, batching, offsets, JSON payloads, error handling), see
[docs/getting-started.md](docs/getting-started.md).

## Documentation

- [Getting Started](docs/getting-started.md) - installation, configuration, and usage guide
- [Architecture](docs/architecture.md) - the FFI layer and async/cancellation design
- [Benchmarks](docs/benchmarks.md) - producer/consumer throughput, local and over a real remote cluster
- [Integration Testing](docs/integration-testing.md) - running the test suite locally and in CI
- [Publishing](docs/publishing.md) - packaging and releasing to NuGet
- [Production Readiness](docs/production-readiness.md) - ⚠️ predates this FFI rewrite, kept for
  history only

## Development

```bash
# Build (also builds the native library in debug mode)
dotnet build

# Unit tests (no cluster required)
dotnet test --filter "FullyQualifiedName!~Integration"

# Integration tests (requires a running Fluvio cluster)
dotnet test --filter "FullyQualifiedName~Integration"

# Benchmarks (requires a running Fluvio cluster)
cd benchmarks/Fluvio.Client.Benchmarks && dotnet run -c Release
```

See [AGENTS.md](AGENTS.md) for a fuller build/test/architecture reference, common gotchas, and
known gaps.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

No LICENSE file is currently checked into this repository — treat the project as unlicensed
(all rights reserved) until one is added.

## Disclaimer

This is an unofficial client. For official client libraries, see the
[Fluvio documentation](https://www.fluvio.io/docs/).
