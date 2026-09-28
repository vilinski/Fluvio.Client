# Fluvio.Client: Rust-FFI Transport Rewrite

Status: approved for planning
Date: 2026-09-25

## Context

`Fluvio.Client` currently implements the Fluvio SPU/SC binary wire protocol
entirely in managed C# (`src/Fluvio.Client/Protocol/`, `Network/FluvioConnection.cs`),
talking raw TCP directly to a Fluvio cluster. `CLAUDE.md` describes a Rust-FFI
architecture (`native/`, `Interop.cs`, P/Invoke, MSBuild native-packaging
targets) that does **not** exist in this repo — it documents an earlier FFI
prototype (`memory-bank/tasks/TASK008-implement-native-consumer-ffi.md`, ~70%
done) that was abandoned in favor of the current pure-managed implementation.
Empty leftover projects (`src/FluvioCSharp/`, `tests/FluvioCSharp.Tests/`,
`tests/FluvioCSharp.IntegrationTests/`) and a stray empty
`runtimes/osx-arm64/native/` directory are remnants of that abandoned attempt.

This spec replaces the pure-managed transport with a Rust FFI layer that
wraps the official `fluvio` Rust client, following the async-FFI design from
["Async FFI Framework for Rust ↔ C# Interop"](https://www.scylladb.com/2026/08/31/async-ffi-framework-for-rust-c-interop/)
(ScyllaDB) and the existing sibling implementation at
`/Users/vilinski/Projects/rust/fluvio_dotnet_flash`, which already applies
that design to a Fluvio client.

## Goals

- Replace the hand-rolled wire protocol and TCP transport with a Rust FFI
  crate that wraps the official `fluvio` Rust client.
- Follow the TCB (Task Control Block) callback-based async FFI pattern:
  no polling, no async-FFI crate/macro, no `bindgen`/`cbindgen`/codegen of
  any kind — hand-written mirrored structs on both sides, cross-checked by
  compile-time size assertions.
- Preserve `Fluvio.Client.Abstractions` (the public interfaces) as closely
  as possible; only change what genuinely has no equivalent in the new
  transport.
- Preserve example projects and the integration test suite as the
  behavioral regression contract.
- Remove now-unnecessary managed dependencies (compression codecs, CRC32C
  hashing, Polly) once their responsibilities move to the Rust client.
- Make `CLAUDE.md`'s description of the native build/packaging pipeline
  actually true.

## Non-goals

- No admin topic-watch streaming API (the reference repo's `TopicWatcher`)
  — not present in current `IFluvioAdmin`, not being added now.
- No change to `IFluvioClient`, `IFluvioProducer`, `IPartitioner`, or the
  `SmartModule`/`RecordHeaders` public types beyond what's forced by the
  transport swap.
- No connection pooling / multi-connection changes — out of scope.

## Architecture

### 1. Rust native crate

New crate at `native/fluvio-dotnet-native` (`cdylib`, name `fluvio_dotnet`),
wrapping `fluvio` (with the `admin` feature) and
`fluvio-controlplane-metadata`.

```toml
[lib]
name = "fluvio_dotnet"
crate-type = ["cdylib"]

[dependencies]
fluvio = { version = "...", features = ["admin"] }
fluvio-controlplane-metadata = "..."
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync"] }
futures = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
once_cell = "1"

[profile.release]
lto = "thin"
```

No `bindgen`, `cbindgen`, `interoptopus`, or any other FFI-codegen crate.
(This also means deleting the currently-unused `Interoptopus` mention in
`CLAUDE.md`'s "Code Generation" section — see Documentation below.)

Modules, mirroring the reference repo's layout:

- `runtime.rs` — global `Lazy<tokio::runtime::Runtime>` singleton (`rt-multi-thread`),
  plus `ffi_runtime_init() -> i32` for deterministic eager init from C#.
- `tcb.rs` — `Tcb` struct + `complete_success`/`complete_failure`/`complete_error`
  helpers.
- `ffi_types.rs` — `FFISlice`, `FFIString`, `FFIBool`, `FFIRecord`, with
  `const _: () = assert!(...)` size checks.
- `error.rs` — error codes + `anyhow::Error -> (code, message)` classification.
- `client.rs` — connect/disconnect/health-check.
- `producer.rs` — send/send-batch/flush/partition-count.
- `consumer.rs` — stream-next/fetch-batch/fetch-last-offset/commit-offset,
  cancellation via `Notify`.
- `admin.rs` — topic/SPU/partition/SmartModule CRUD, JSON-serialized DTOs
  for list/describe results.

### 2. Async FFI pattern (TCB)

Every async operation takes a trailing `Tcb` by value:

```rust
#[repr(C)]
#[derive(Copy, Clone)]
pub struct Tcb {
    pub tcs: *mut c_void,
    pub on_success: *mut c_void, // extern "C" fn(*mut c_void, *mut c_void)
    pub on_failure: *mut c_void, // extern "C" fn(*mut c_void, i32, *const u8, usize)
}
unsafe impl Send for Tcb {}
unsafe impl Sync for Tcb {}
```

The Rust fn spawns the operation on the shared runtime and returns
immediately; on completion it invokes one of the two callback pointers.
Success carries one machine word (handle pointer, boxed offset, string
pointer, or null); failure carries `(code, message ptr+len)`.

C# bridges this with a `TaskCompletionSource<nint>(RunContinuationsAsynchronously)`
wrapped in a `GCHandle`, and two `[UnmanagedCallersOnly(CallConvs = new[]
{ typeof(CallConvCdecl) })]` static methods whose function pointers are
captured once as `static readonly nint` fields (no per-call delegate
allocation). `RunContinuationsAsynchronously` is required to avoid
starving Tokio worker threads with synchronous .NET continuations (the
core problem the ScyllaDB article calls out).

`[LibraryImport]` source-generated P/Invoke stubs are used for the
P/Invoke declarations themselves (.NET's built-in generator) — this is
not FFI codegen in the sense being excluded; it only generates the marshal
stub for a single hand-declared `extern` signature, same as the reference
repo.

### 3. Handles and lifetime

Client/producer/consumer/admin instances are `Box::into_raw` opaque
pointers (`*mut c_void`), freed by dedicated `ffi_*_drop` functions
(`Box::from_raw` + drop). C# wraps each in a `SafeHandle` subclass
(`RustResource`) storing the drop delegate. Every public API method routes
its native call through `RunWithIncrement`/`RunAsyncWithIncrement`
(built on `SafeHandle.DangerousAddRef`/`DangerousRelease`) so the handle's
refcount stays bumped across the whole native call/await, preventing the
`SafeHandle` finalizer from freeing the resource mid-operation.

Record buffers are a separate `SafeHandle` (`NativeBuffer`) wrapping an
`FFIRecord*` (offset, timestamp, partition, key `FFISlice`, value
`FFISlice`), freed via `ffi_record_free`. `ConsumeRecord.Value`/`.Key` are
exposed as zero-copy `ReadOnlyMemory<byte>` via a `MemoryManager<byte>`
wrapping the raw pointer, until the record is disposed.

### 4. Streaming (`IFluvioConsumer.StreamAsync`)

Pull-based, one FFI round-trip per record via `ffi_stream_next`, matching
`IAsyncEnumerable`'s pull model 1:1 (backpressure and cancellation "for
free," per the reference repo's rationale). The stream is stored as
`Arc<Mutex<Option<Stream>>>` + `Arc<Notify>` for cancellation; each poll
takes the stream out of the mutex, races it against `cancel.notified()`
via `tokio::select!`, puts it back into the mutex **before** completing
the TCB (to avoid a synchronous re-entrant `next` call finding the stream
still taken), and completes with the record pointer, `null` (EOF), or a
`CANCELLED` failure.

`StreamingConsumer.cs`'s hand-rolled background-task/bounded-channel loop
is deleted entirely; the Rust side now owns the fetch loop and
`IFluvioConsumer.StreamAsync` becomes a thin enumerator over
`ffi_stream_next`. `CancellationToken.Register` calls a native
`ffi_stream_close` (idempotent `Notify::notify_one`), and
`DisposeAsync`/`IAsyncEnumerator` disposal calls `Close()` before
`Dispose()` to unblock any in-flight poll before the handle is freed.

### 5. Errors

Failure callback: `(code: i32, message ptr+len)`. Error codes:

```rust
pub mod codes {
    pub const GENERIC: i32 = 1;
    pub const CONNECTION: i32 = 2;
    pub const TOPIC_NOT_FOUND: i32 = 3;
    pub const TOPIC_ALREADY_EXISTS: i32 = 4;
    pub const CANCELLED: i32 = 5;
    pub const INVALID_ARGUMENT: i32 = 6;
    pub const UNAUTHORIZED: i32 = 7;
}
```

Classified from the `anyhow::Error` chain (substring match on the
underlying `fluvio` error), same approach as the reference repo. C# keeps
`FluvioException` as the base type and maps codes to
`FluvioConnectionException`, `TopicNotFoundException`,
`TopicAlreadyExistsException`, and `OperationCanceledException` (code 5) —
preserving the existing `catch (FluvioException ex) when
(ex.Message.Contains(...))` pattern used in examples.

### 6. Compression

Deleted: `src/Fluvio.Client/Compression/CompressionUtils.cs` and the
`K4os.Compression.LZ4`, `K4os.Compression.LZ4.Streams`, `Snappier`,
`ZstdSharp.Port` package references. The `Compression` enum on
`ProducerOptions` is passed through to the Rust producer config; the
official `fluvio` client already implements gzip/snappy/lz4/zstd.

### 7. Config / TLS

`FluvioClientOptions` (endpoint, TLS flag, timeouts, logger) is serialized
to JSON and passed to `ffi_client_connect`, which builds a `fluvio::config::
ConfigFile`/`FluvioConfig` on the Rust side. The existing hand-rolled TOML
parser in `Config/FluvioConfig.cs` is deleted — profile resolution
(`~/.fluvio/config`, `current_profile`, cluster endpoint/TLS policy) is
delegated to the official Rust client's own config loading, which already
implements this correctly and more completely (it currently has no
client-cert/CA support in the C# version).

### 8. Public API changes

- `IFluvioConsumer.CommitOffsetAsync`: drop the `uint sessionId` parameter
  (a `StreamFetch`-session artifact with no FFI equivalent). New
  signature: `Task CommitOffsetAsync(string consumerId, string topic, int
  partition, long offset, CancellationToken cancellationToken = default)`.
- `TopicSpecModels.cs` (internal wire-encoding types: `ReplicaSpec`,
  `TopicSpecFull`, `DeduplicationConfig`, etc.) is deleted; admin
  operations build the Rust client's own `TopicSpec`/metadata types
  directly. No public-surface impact — these types are internal.
- Everything else in `Fluvio.Client.Abstractions` (`IFluvioClient`,
  `IFluvioProducer`, `IFluvioAdmin`, `IPartitioner`, `SmartModule`,
  `RecordHeaders`, all option/result records) is unchanged.

### 9. Dependency cleanup

Removed from `Fluvio.Client.csproj`: `K4os.Compression.LZ4`,
`K4os.Compression.LZ4.Streams`, `Snappier`, `ZstdSharp.Port`,
`System.IO.Hashing` (CRC32C was only used by the deleted hand-rolled batch
encoding), `Polly` (resilience — retry/circuit-breaker — moves to the
Rust client / connection layer; no equivalent wrapper is kept at the C#
level). No new C# package dependencies beyond what P/Invoke and
`[LibraryImport]` already require (none). No `bindgen`/`cbindgen`/codegen
crate on the Rust side.

`Fluvio.Client.csproj` gains the native-packaging MSBuild targets
`CLAUDE.md` already describes but that don't currently exist:
`BuildNativeForRid` (invokes `cargo build --target <rust-triple>` when
packing with a specific RID), `CopyNativeToRuntimesFolder`, and
`IncludeNativeInPackage`, plus a `BuildNativeDebug` target so plain
`dotnet build` builds the native crate in debug mode automatically.
Native library resolution in a new `Interop.cs` follows the order already
documented: `FLUVIO_DOTNET_NATIVE_PATH` env var → repo-relative
`native/target/{debug,release}` probing (dev) → NuGet
`runtimes/{rid}/native/` layout (packaged) → default resolution fallback.

### 10. Removed code

- `src/Fluvio.Client/Protocol/**` (binary reader/writer, request/response
  encoding, batch/header encoding, SmartModule encoder)
- `src/Fluvio.Client/Network/FluvioConnection.cs`
- `src/Fluvio.Client/Compression/CompressionUtils.cs`
- `src/Fluvio.Client/Config/FluvioConfig.cs` (hand-rolled TOML parser)
- `src/Fluvio.Client/Admin/TopicSpecModels.cs`
- `src/Fluvio.Client/Consumer/StreamingConsumer.cs`
- Empty leftover projects/dirs from the abandoned prototype:
  `src/FluvioCSharp/`, `tests/FluvioCSharp.Tests/`,
  `tests/FluvioCSharp.IntegrationTests/`, `tests/Fluvio.Client.IntegrationTests/`
  (empty, not in the `.sln`), stray `runtimes/osx-arm64/native/` dirs.
- `memory-bank/tasks/TASK008-implement-native-consumer-ffi.md` superseded
  by this spec — left in place as historical record, not deleted (it's a
  memory-bank artifact, not code).

### 11. Testing strategy

- Keep `tests/Fluvio.Client.Tests/Integration/**` as the behavioral
  regression suite (exercises the public interfaces against a real
  cluster); update only what's forced by the `CommitOffsetAsync` signature
  change.
- Delete `Protocol/*`, `SmartModule/SmartModuleEncoderTests.cs` (test the
  now-deleted hand-rolled wire encoding).
- Keep `Consumer/OffsetResolverTests.cs`, `Headers/*`,
  `Producer/PartitionerTests.cs` (pure logic, transport-independent) —
  adjust only if their subject code moves.
- Delete `Compression/CompressionUtilsTests.cs` (subject deleted).
- Add new Rust-side unit tests for FFI struct layout and error
  classification (`cargo test`), and new C#-side tests for the
  handle-lifecycle/callback-bridge plumbing (`RustResource`,
  `Callbacks`/TCB bridging) using a minimal fake native surface or the
  real native lib built in debug.
- Update `benchmarks/Fluvio.Client.Benchmarks/ProtocolBenchmarks.cs`
  (protocol-specific) — delete; keep/adjust `ConsumerBenchmarks.cs` /
  `ProducerBenchmarks.cs` against the new transport.

### 12. CI / packaging

Update `.github/workflows/build-test-publish.yml` per `CLAUDE.md`'s
already-described pipeline: build native libs for linux-x64, osx-arm64,
osx-x64, win-x64 in parallel, run unit + integration tests per platform,
collect artifacts into `runtimes/{rid}/native/`, pack the multi-platform
NuGet package. (This workflow file needs to be created/updated to match;
current state to be confirmed during planning.)

## Open items for the implementation plan

- Exact `fluvio` / `fluvio-controlplane-metadata` crate versions to pin.
- Whether `ffi_client_connect` takes a JSON config blob or discrete
  primitive args (JSON blob recommended, matches admin DTOs elsewhere).
- Rust edition/toolchain pin for the new `native/` crate (repo currently
  has no Rust toolchain file).
