# Benchmarks

BenchmarkDotNet results for `benchmarks/Fluvio.Client.Benchmarks`, run against a local Fluvio
cluster and against the real `hetzner-tls` remote cluster (see
[docs/integration-testing.md](integration-testing.md) for what that profile is), plus a
side-by-side comparison against
[`fluvio_dotnet_flash`](https://github.com/vilinski/fluvio_dotnet_flash)'s own local-only
numbers, which used a different, earlier FFI prototype of this same idea.

Run with:

```bash
cd benchmarks/Fluvio.Client.Benchmarks
dotnet run -c Release --filter "*"                                    # local cluster
FLUVIO_TEST_PROFILE=hetzner-tls dotnet run -c Release --filter "*"    # hetzner-tls
```

Environment: Apple M1 Max, macOS 27.0, .NET 8.0.20, `BenchmarkDotNet v0.15.7`.

## Producer

| Benchmark | This client, local | This client, hetzner-tls | flash, local |
| --- | ---: | ---: | ---: |
| Single small message (14 B) | 181.6 µs | 29.17 ms | — |
| Single medium message (1 KB) | 184.1 µs | 31.14 ms | — |
| Single large message (10 KB) | 204.8 µs | 73.54 ms | — |
| Sequential send, per message (100 msgs) | 176.4 µs | 29.77 ms | 146.7 µs |
| "Batch" send, per message (100 msgs) | 173.8 µs | 32.71 ms | 1.23 µs |

**The batch numbers are the headline finding.** `IFluvioProducer.SendBatchAsync` in this client
loops calling `SendAsync` once per record — it is not a real batched wire call, which is why its
per-message cost (173.8 µs local / 32.71 ms hetzner) is statistically the same as sequential
sends. flash's `SendAllAsync` performs a real single-request batch, and its 1.23 µs/message is
~140x faster than this client's "batch" call for the same 100-message workload. This gap is a
known, already-tracked limitation (`ProducerOptions.BatchSize`/`LingerTime` are accepted but not
wired to real batching behavior — see the "Known excluded tests" section of
[docs/integration-testing.md](integration-testing.md)), not a regression introduced by this
benchmark.

hetzner-tls's per-message producer cost (~30 ms) is dominated by real network round-trip latency
to the remote cluster — every send is its own request/response over TLS to a different
continent-scale network path than localhost, so this is expected and not comparable 1:1 with the
local numbers.

## Consumer

| Benchmark | This client, local | This client, hetzner-tls |
| --- | ---: | ---: |
| Streaming consumer, per message (1000 msgs) | 14.8 µs | 105 µs |
| Fetch batch, per message (1000 msgs) | 209 µs | 263 µs |
| Streaming consumer, per message (100 msgs) | 21.5 µs | 961 µs |
| Fetch batch, per message (100 msgs) | 2,071 µs | 2,563 µs |

Streaming (`StreamAsync`, a real pull-based native stream — see
[docs/architecture.md](architecture.md)) is consistently faster than `FetchBatchAsync` per
message at both message counts and against both clusters, since a stream keeps one native
connection open across many records instead of paying a per-call round trip; the gap narrows over
hetzner-tls (both pay the same network latency baseline) but streaming still wins by ~2.5x there
too. flash's own report did not include comparable consumer numbers (its `Consume` benchmark
result was excluded from its published run).

## Known gaps this data surfaces

- `SendBatchAsync` provides no throughput benefit over sequential sends — see above.
- These numbers are single-run BenchmarkDotNet reports on one machine and one remote cluster;
  treat them as directional, not a formal performance SLA.
