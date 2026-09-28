# Fluvio.Client Architecture

This document describes the current architecture: a thin, hand-written Rust FFI layer
wrapping the official [`fluvio`](https://crates.io/crates/fluvio) Rust client, exposed to
.NET through a managed C# layer. There is no custom wire-protocol implementation in this
repository — all protocol handling, retries, and cluster discovery are delegated to the
official Rust client.

## High-level layout

```text
┌─────────────────────────────────────────────────────────────┐
│                      Application Code                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│         Public API (Fluvio.Client.Abstractions)              │
│   IFluvioClient · IFluvioProducer · IFluvioConsumer          │
│                      IFluvioAdmin                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Managed Layer (Fluvio.Client)                   │
│  FluvioClient · FluvioProducer · FluvioConsumer · FluvioAdmin │
│  Interop/: P/Invoke declarations, SafeHandle wrappers,        │
│            TaskCompletionSource bridging, cancellation        │
└─────────────────────────────────────────────────────────────┘
                              │  P/Invoke (LibraryImport)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        Native FFI Layer (native/fluvio-dotnet, Rust)         │
│  extern "C" fn entry points · shared Tokio runtime ·          │
│  TCB (Task Control Block) async completion · cancellation     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Official `fluvio` Rust client crate              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                      Fluvio Cluster (SC + SPUs)
```

## The async FFI pattern (TCB)

Every native async operation follows the same shape, based on the "Async FFI Framework for
Rust ↔ C# Interop" design:

1. **C# call site** builds a `Tcb` (Task Control Block) struct: a `GCHandle`-rooted
   `TaskCompletionSource<nint>` plus two `[UnmanagedCallersOnly]` function pointers
   (`on_success`, `on_failure`).
2. The `extern "C"` Rust function receives the `Tcb` by value, spawns the actual work
   (an `async` block calling into the `fluvio` crate) on a single, process-wide Tokio
   runtime (`runtime.rs`), and returns immediately — the P/Invoke call never blocks the
   calling thread.
3. When the spawned future resolves, Rust invokes one of the two callback pointers, which
   completes the C# `TaskCompletionSource`. C# code just `await`s the `Task` as usual.
4. Every spawned future is wrapped in `spawn_guarded` (`tcb.rs`), which catches Rust panics
   via `catch_unwind` and completes the `Tcb` with a failure instead of leaving the C# task
   pending forever.

## Cancellation

`cancel.rs` implements a small cancellation primitive independent of the TCB: a native
handle (`ffi_cancel_new`) backed by a `tokio::sync::Notify`, triggered from C# via
`ffi_cancel_trigger` when a `CancellationToken` fires. Every non-streaming native call races
its real work against this notification (`cancel::race`), so cancelling the C# token
actually stops the in-flight Rust future rather than merely abandoning the `Task`.

## Memory and lifetime management

- **Native handles** (client, producer, consumer, admin, stream) are wrapped in C#
  `SafeHandle` subclasses (`RustResource`), giving automatic, GC-safe cleanup and
  `DangerousAddRef`/`DangerousRelease` protection across `await` points.
- **Strings** cross the boundary as UTF-8 `(ptr, len)` pairs; a null pointer (which a
  zero-length C# array pins to) is treated as an empty string on the Rust side rather than
  passed to `slice::from_raw_parts`, which would be undefined behavior.
- **Records** are allocated by Rust and freed by C# once consumed
  (`fluvio_record_owned_free`); a `RecordPtrGuard` on the Rust side frees any
  not-yet-handed-off records on every exit path (success, error, cancellation, panic) so a
  partially filled batch can never leak.
- **Errors** are C-allocated strings, freed via `fluvio_error_free` after being copied into
  a managed `FluvioException`.

## Streaming

`FluvioConsumer.StreamAsync` returns a real `IAsyncEnumerable<ConsumeRecord>` backed by a
pull-based native stream handle (`ffi_stream_new` / `ffi_stream_next`): each `MoveNextAsync`
call is a single native async round-trip, not a polling loop.

## Native library resolution

`Interop/Native.cs` resolves the native library in this order:

1. `FLUVIO_DOTNET_NATIVE_PATH` environment variable, if set.
2. `native/fluvio-dotnet/target/{debug,release}` relative to the running assembly — for
   development, so `dotnet test`/`dotnet run` inside this repo just work after `cargo build`.
3. The .NET runtime's own default resolution — for a packaged NuGet consumer, this finds the
   bundled `runtimes/{rid}/native/` asset.

See the root `AGENTS.md` for build commands and `docs/getting-started.md` for usage.
