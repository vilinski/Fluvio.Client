# AGENTS.md

Guidance for coding agents working in this repository.

## Project Overview

Fluvio.Client is a .NET client library for the Fluvio streaming platform, built on a
hand-written Rust FFI layer that wraps the official [`fluvio`](https://crates.io/crates/fluvio)
Rust client. There is no custom wire-protocol implementation here — see
[docs/architecture.md](docs/architecture.md) for the full design.

- **C# library** (`src/Fluvio.Client/`): managed wrapper exposing async Producer, Consumer, and
  Admin APIs (`src/Fluvio.Client.Abstractions/` holds the public interfaces/records)
- **Rust native library** (`native/fluvio-dotnet/`): a standalone Cargo crate (not a workspace
  member) exposing `extern "C"` functions
- **Cross-platform packaging**: NuGet package intended to bundle native binaries for linux-x64,
  osx-arm64, osx-x64, win-x64 (see "Known gaps" below — this isn't fully wired yet)

## Build Commands

```bash
# Build the whole solution (also builds the Rust native library in debug mode automatically)
dotnet build

# Native library only
cd native/fluvio-dotnet
cargo build            # debug
cargo build --release  # release

# Pack for a specific runtime (only mode that bundles the native lib - see "Known gaps")
dotnet pack src/Fluvio.Client/Fluvio.Client.csproj -c Release -p:RuntimeIdentifier=osx-arm64

# Unit tests (no cluster required)
dotnet test --filter "FullyQualifiedName!~Integration"

# Integration tests (requires a running Fluvio cluster - see docs/integration-testing.md)
dotnet test --filter "FullyQualifiedName~Integration"

# Single test
dotnet test --filter "FullyQualifiedName~TestMethodName"

# Native unit tests
cd native/fluvio-dotnet && cargo test

# Examples
dotnet run --project examples/ProducerExample
dotnet run --project examples/ConsumerExample

# Benchmarks (see docs/benchmarks.md)
cd benchmarks/Fluvio.Client.Benchmarks && dotnet run -c Release
```

## Architecture

### FFI layer (`native/fluvio-dotnet/src/`)

- `lib.rs` - `extern "C"` entry points
- `tcb.rs` - the TCB (Task Control Block) async-completion pattern and `spawn_guarded` (panic
  boundary - every spawned future is wrapped so a Rust panic completes the C# task with a
  failure instead of hanging it forever)
- `cancel.rs` - the cancellation primitive (`ffi_cancel_new`/`trigger`/`drop`, `race()`) every
  non-streaming call races against
- `runtime.rs` - the single, process-wide Tokio runtime
- `client.rs`, `producer.rs`, `consumer.rs`, `admin.rs` - the FFI functions per API surface
- `ffi_types.rs` - shared C-compatible types, including `string_from_raw` (every FFI string
  parameter must go through this - a null `(ptr, len)` pair is a normal, reachable case from C#,
  not a caller error, and must not reach `slice::from_raw_parts` directly)
- `error.rs` - native error classification into C# exception types

No bindgen/cbindgen/Interoptopus codegen - every P/Invoke declaration in `Interop/Native.cs` is
hand-written and must be kept in sync manually with the Rust `extern "C"` signatures.

### C# managed layer (`src/Fluvio.Client/`)

- `Interop/Native.cs` - P/Invoke declarations and native library resolution (see below)
- `Interop/RustResource.cs` - `SafeHandle` wrapper with `RunAsyncWithIncrement` for safe access
  across `await` points
- `Interop/CancellationBridge.cs` - bridges a C# `CancellationToken` to a native cancel handle
- `Interop/Callbacks.cs` - `TaskCompletionSource`/`UnmanagedCallersOnly` bridging for the TCB
  pattern
- `FluvioClient.cs`, `Producer/FluvioProducer.cs`, `Consumer/FluvioConsumer.cs`,
  `Admin/FluvioAdmin.cs` - the public API implementations

### Native library resolution

`Interop/Native.cs`'s `Resolve` method, in order:

1. `FLUVIO_DOTNET_NATIVE_PATH` environment variable, if set
2. `native/fluvio-dotnet/target/{debug,release}` relative to the running assembly (development)
3. The .NET runtime's default resolution (a packaged NuGet consumer's `runtimes/{rid}/native/`)

### Consumer offsets

`ConsumerOptions` (`src/Fluvio.Client.Abstractions/IFluvioClient.cs`) controls offset behavior via
`OffsetReset` (`Earliest`/`Latest`/`StoredOrEarliest`/`StoredOrLatest`), `ConsumerGroup`,
`AutoCommit`, and `AutoCommitInterval` - not a `ConsumerId`/`Manual`/`Auto` strategy enum. A
`ConsumerGroup` is required to persist/resume offsets across runs.

## Key Technical Constraints

- Targets .NET 8.0
- Rust edition 2024 (`native/fluvio-dotnet/Cargo.toml`)
- All native async operations run on the single global Tokio runtime
- `FluvioProducer`'s send/flush timeout is `ProducerOptions.Timeout` (default 5s), applied via
  `FluvioProducer.EffectiveSendTimeout` - not a hardcoded constant
- `IFluvioConsumer.FetchBatchAsync` has no internal timeout against an empty topic/partition; it
  blocks until data arrives or the caller's `CancellationToken` fires
- `IFluvioConsumer.StreamAsync` is a real pull-based native stream (`ffi_stream_new`/`ffi_stream_next`),
  not a polling loop

## Common Gotchas

- **Wrong library version loaded**: `FLUVIO_DOTNET_NATIVE_PATH`, if set, takes priority over the
  repo's debug/release builds. Unset it or point it at `native/fluvio-dotnet/target/debug` for
  local development.
- **Native library not found**: `dotnet build` builds the Rust library automatically. If issues
  persist, run `cargo build` in `native/fluvio-dotnet/` manually.
- **Integration test failures against `local`**: the local dev cluster has a known capacity
  degradation after sustained sequential client churn (a ~60s "Socket io Timed out" on topic
  creation) - reset with `fluvio cluster delete --force && fluvio cluster start --local`. This
  does not reproduce against the real `hetzner-tls` cluster; see docs/integration-testing.md.
- **Memory leaks**: every native handle must be disposed (`RustResource`/`SafeHandle`). New FFI
  string parameters must go through `string_from_raw`, not `slice::from_raw_parts` directly - a
  null pointer (which an empty C# string's `fixed` produces) is UB there, not a panic.
- **Async deadlocks**: never block on async methods from sync contexts; always `await`.

## Known Gaps (tracked, not yet fixed)

- `IFluvioProducer.SendBatchAsync` loops individual `SendAsync` calls instead of a real batched
  wire call - see [docs/benchmarks.md](docs/benchmarks.md) for the measured cost of this.
  `ProducerOptions.BatchSize`/`LingerTime` are accepted but not wired to real batching behavior
  (see the excluded-tests list in [docs/integration-testing.md](docs/integration-testing.md)).
- `.github/workflows/nuget-publish.yml` (tag-triggered) does not pass a `RuntimeIdentifier`, so it
  does not invoke the native-bundling MSBuild targets in `Fluvio.Client.csproj` - a real
  tag-triggered release today would ship a package with no native library. See
  [docs/publishing.md](docs/publishing.md).

## Adding New FFI Functions

1. Add the Rust implementation in the relevant `native/fluvio-dotnet/src/*.rs` file
2. Add the corresponding `[LibraryImport]` declaration in `src/Fluvio.Client/Interop/Native.cs`
   by hand
3. Add the high-level C# wrapper in the relevant class
4. Route every string/binary parameter through `string_from_raw` / the null-guarded slice
   pattern, not a raw `slice::from_raw_parts`
5. Wrap the async entry point's spawned future in `spawn_guarded`
