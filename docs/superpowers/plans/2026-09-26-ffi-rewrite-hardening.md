# FFI Rewrite Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the correctness, concurrency, and feature-parity defects found by code review and by live testing (against both a local Fluvio cluster and the `hetzner-tls` Hetzner cluster) after the initial Rust-FFI transport rewrite, and finalize CI to run integration tests against `hetzner-tls` in CI.

**Architecture:** No architectural change — this hardens the existing TCB/callback FFI design (`docs/superpowers/specs/2026-09-25-rust-ffi-rewrite-design.md`) in place: a shared cancellation/timeout primitive is added to the `Tcb` bridge and applied to every non-streaming call site; a panic boundary is added around every spawned Rust task; producer/consumer semantics bugs are fixed; headers and custom partitioning are implemented end-to-end; and CI is finalized against the real Hetzner cluster.

**Tech Stack:** Same as the original rewrite — Rust (`fluvio` 0.50.1, `tokio`), .NET 8/C# 12, plus real integration testing this time: both `local` (127.0.0.1:9003, TLS disabled) and `hetzner-tls` (vilinski.dev:9103, TLS verified via `/Users/vilinski/.fluvio/certs/`) clusters are confirmed reachable from this machine.

**Spec:** `docs/superpowers/specs/2026-09-25-rust-ffi-rewrite-design.md` (original design) plus this plan's fixes, which supersede the original plan's now-completed 9 tasks.

**Baseline:** Branch `worktree-rust-ffi-rewrite`, HEAD `5b31a84` (all 9 original tasks committed), plus a set of **uncommitted** changes already made in this worktree by a prior agent ("Codex") — see Task 0, which adopts and finalizes them before any other task starts.

## Global Constraints

- No `bindgen`/`cbindgen`/codegen crate — this still holds; every fix is hand-written, matching the original spec.
- Every fix in this plan must be verified against a REAL cluster (`local` first, then `hetzner-tls`) — no more "no cluster available" carve-outs. If a test still can't run against a real cluster, that itself is a plan failure to report, not a reason to skip.
- Do not weaken, skip, or delete a test to make it pass. If a test's assumption was wrong (e.g. expects an infinite stream to end), fix the test's logic to match correct production semantics, never the other way around.
- Producer/consumer public API shape (`IFluvioProducer`, `IFluvioConsumer`, `ProducerOptions`, `ConsumerOptions`) stays the same except where a task explicitly says otherwise (Task 8 removes dead resilience options — a deliberate, documented API reduction).
- Every native entry point that spawns a Tokio task must, after this plan, guarantee its `Tcb` is completed exactly once even if the spawned future panics (Task 1) — later tasks depend on this to get clean failures instead of hangs while debugging.
- `.orchestration/`-style scratch files: none needed this round since work happens directly in this session/subagents, not via agterm; if agterm-per-task is used again, gitignore any scratch dir before first use as in the original run.

## Review Focus

- **A panicking Rust task under real cluster load (network blip, malformed server response) hangs the caller forever instead of throwing** — Task 1 must be verified with an actual test that forces a panic path and asserts the C# `Task` faults instead of hanging, not just code inspection.
- **Cancelling a `FetchBatchAsync`/admin call/`SendAsync` mid-flight must actually cancel the native operation, not just the C# wait** — Task 2's tests must prove the native side stops working (e.g. via a side effect or timing), not just that the C# `Task` throws `OperationCanceledException` while Rust keeps running unobserved.
- **Two producers concurrently calling `GetOrCreateProducerHandleAsync` for the same topic while a third calls `DisposeAsync`** — Task 7's fix must be proven under an actual concurrent stress test (`Task.WhenAll` of many concurrent `SendAsync` + one `DisposeAsync`), not just reasoned about.
- **A header value containing bytes that aren't valid UTF-8, or a header map with zero entries vs. `null`** — Task 5's header round-trip test must cover binary header values and the null-vs-empty distinction, not just a single ASCII string case.
- **`StreamAsync` resuming from a stored offset when the stored offset is exactly the topic's current end (nothing new to consume yet)** — Task 4's fix must have a test that commits an offset, reconnects, and confirms `StreamAsync` waits for new records rather than replaying or erroring, not just that it uses a non-null starting offset.

---

## File Structure

**New:**

- `native/fluvio-dotnet/src/cancel.rs` — shared cancellation-token FFI type (`CancelHandle`, `ffi_cancel_new`/`ffi_cancel_trigger`/`ffi_cancel_drop`) used by every non-streaming async call.
- `src/Fluvio.Client/Interop/CancellationBridge.cs` — C# side: wraps a `CancellationToken` into a native `CancelHandle` + registration, used by every call site that currently ignores its token.
- `tests/Fluvio.Client.Tests/Integration/ProducerPartitionerIntegrationTests.cs` — new, bounded tests replacing the two hanging partitioner tests (moved out of `ProducerIntegrationTests.cs` per Task 9's split).

**Modified:**

- `native/fluvio-dotnet/src/tcb.rs` — panic boundary (Task 1).
- `native/fluvio-dotnet/src/{client,producer,consumer,admin}.rs` — cancellation wiring (Task 2), headers (Task 5), partitioner (Task 6), offset fixes (Task 3, Task 4), leak fixes (Task 10).
- `src/Fluvio.Client/Interop/Native.cs`, `Callbacks.cs` — cancellation plumbing signatures.
- `src/Fluvio.Client/Producer/FluvioProducer.cs` — timeout (Task 2), partitioner (Task 6), dispose race fix (Task 7).
- `src/Fluvio.Client/Consumer/FluvioConsumer.cs` — stored-offset fix (Task 4).
- `src/Fluvio.Client/Admin/FluvioAdmin.cs` — `IgnoreRackAssignment` fix (Task 10, folded from the review finding).
- `src/Fluvio.Client.Abstractions/IFluvioClient.cs` — remove dead resilience options (Task 8).
- `src/Fluvio.Client/FluvioException.cs` — remove or wire `IncompatiblePlatformVersionException` (Task 8).
- `examples/ProducerExample/Program.cs`, `examples/StreamingConsumerExample/Program.cs` — typed-exception catches instead of message substring matching (Task 8).
- `.github/workflows/integration-tests.yml`, `docs/integration-testing.md` — finalize CI against `hetzner-tls` (Task 11).
- `tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs`, `BatchFlushIntegrationTests.cs` — bounded-stream fixes, dispose-race regression test (Task 9, Task 7).

**Adopted as-is (already done, uncommitted, in the worktree — Task 0 commits them):**

- `native/fluvio-dotnet/src/client.rs`, `src/Fluvio.Client/FluvioClient.cs`, `src/Fluvio.Client.Abstractions/IFluvioClient.cs` (connect docs), `tests/Fluvio.Client.Tests/Integration/{AdminBasicTest,ConnectionIntegrationTests,FluvioIntegrationTestBase}.cs`, `tests/Fluvio.Client.Tests/Integration/IntegrationTestConfig.cs` (new file).

---

### Task 0: Adopt and commit the in-progress connect/profile-resolution changes

**Files:**

- Review/keep as-is: `native/fluvio-dotnet/src/client.rs`, `src/Fluvio.Client/FluvioClient.cs`, `src/Fluvio.Client.Abstractions/IFluvioClient.cs`, `tests/Fluvio.Client.Tests/Integration/{AdminBasicTest,ConnectionIntegrationTests,FluvioIntegrationTestBase,IntegrationTestConfig}.cs`
- Do NOT adopt yet (park separately, see Task 11): `.github/workflows/integration-tests.yml` (draft, unverified secret), `docs/integration-testing.md`, `docs/CODEX-HANDOFF-2026-09-26.md`

**Interfaces:**

- Produces: `ffi_client_connect`'s JSON now accepts `{ endpoint?, profile?, clientId?, useTls? }` and resolves a real named/current Fluvio profile (preserving TLS cert config) when `profile` is given or all fields are omitted; `FluvioNativeConfig.ToJson(FluvioClientOptions)` (replacing the old 2-arg `ToJson(string, bool)`); `IntegrationTestConfig` exposing however it resolves `FLUVIO_TEST_PROFILE` (read the file to confirm its exact public surface before writing Task 11, which depends on it).
- Consumed by: every later task's integration tests connect via whatever `FluvioIntegrationTestBase.InitializeAsync` now does — read it to confirm before writing new tests.

- [x] **Step 1: Read every uncommitted diff and the new files in full**

```bash
git diff native/fluvio-dotnet/src/client.rs src/Fluvio.Client/FluvioClient.cs src/Fluvio.Client.Abstractions/IFluvioClient.cs tests/Fluvio.Client.Tests/Integration/AdminBasicTest.cs tests/Fluvio.Client.Tests/Integration/ConnectionIntegrationTests.cs tests/Fluvio.Client.Tests/Integration/FluvioIntegrationTestBase.cs
cat tests/Fluvio.Client.Tests/Integration/IntegrationTestConfig.cs
```

Confirm: (a) `ffi_client_connect` fails closed on a named profile that doesn't exist (per the handoff note) rather than silently falling back to another cluster; (b) `IntegrationTestConfig` defaults to `localhost:9003`/no TLS when `FLUVIO_TEST_PROFILE` is unset, and otherwise loads that named native profile; (c) nothing in these diffs weakens or skips an existing test (per this plan's Global Constraints).

- [x] **Step 2: Build and run the native + managed unit suite**

Run: `cd native/fluvio-dotnet && cargo build && cargo test`
Expected: 9 passed (same as before — these diffs don't touch runtime/tcb/ffi_types/error).

Run: `dotnet build Fluvio.Client.sln --configuration Release /p:TreatWarningsAsErrors=true && dotnet test Fluvio.Client.sln --filter "FullyQualifiedName!~Integration"`
Expected: 0 warnings, 0 errors, 72 passed.

- [x] **Step 3: Run the connection/admin integration tests against `local`**

```bash
fluvio profile switch local
FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ConnectionIntegrationTests|FullyQualifiedName~AdminBasicTest" --configuration Release
```

Expected: all pass (per the handoff note, these already passed under Codex).

- [x] **Step 4: Run the same tests against `hetzner-tls`**

```bash
FLUVIO_TEST_PROFILE=hetzner-tls dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ConnectionIntegrationTests|FullyQualifiedName~AdminBasicTest" --configuration Release
```

Expected: all pass, proving TLS + client-cert connect actually works end-to-end against the real Hetzner cluster.

- [x] **Step 5: Commit**

```bash
git add native/fluvio-dotnet/src/client.rs src/Fluvio.Client/FluvioClient.cs src/Fluvio.Client.Abstractions/IFluvioClient.cs tests/Fluvio.Client.Tests/Integration/AdminBasicTest.cs tests/Fluvio.Client.Tests/Integration/ConnectionIntegrationTests.cs tests/Fluvio.Client.Tests/Integration/FluvioIntegrationTestBase.cs tests/Fluvio.Client.Tests/Integration/IntegrationTestConfig.cs
git commit -m "feat: resolve real Fluvio profiles (incl. TLS certs) for native connect"
```

Leave `.github/workflows/integration-tests.yml`, `docs/integration-testing.md`, and `docs/CODEX-HANDOFF-2026-09-26.md` uncommitted/untouched for now (Task 11 and cleanup handle them).

---

### Task 1: Panic boundary + exactly-once TCB completion

**Files:**

- Modify: `native/fluvio-dotnet/src/tcb.rs`
- Modify: `native/fluvio-dotnet/src/{client,producer,consumer,admin}.rs` (wrap every `runtime().spawn(async move { ... })` body)
- Test: `native/fluvio-dotnet/src/tcb.rs` (unit test), plus one C#-visible regression test

**Interfaces:**

- Consumes: existing `Tcb`, `complete_success`, `complete_error` from Task 1 of the original plan.
- Produces: `pub fn spawn_guarded<F>(tcb: Tcb, fut: F) where F: std::future::Future<Output = ()> + Send + 'static` — every call site in `client.rs`/`producer.rs`/`consumer.rs`/`admin.rs` replaces `crate::runtime::runtime().spawn(async move { <body> })` with `crate::tcb::spawn_guarded(tcb, async move { <body> })`, where `<body>` no longer takes `tcb` as a captured variable for completion (it still uses it to call `complete_success`/`complete_error` itself on the happy path; `spawn_guarded` only catches the panic case).
- Consumed by: nothing further changes at call sites beyond the wrap — this task touches every `.rs` file but only mechanically.

- [x] **Step 1: Write the failing native test**

```rust
// native/fluvio-dotnet/src/tcb.rs, in #[cfg(test)] mod tests
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

extern "C" fn record_success(ctx: *mut c_void, _result: *mut c_void) {
    unsafe { (*(ctx as *const AtomicBool)).store(true, Ordering::SeqCst) };
}
extern "C" fn record_failure(ctx: *mut c_void, code: i32, _msg: *const u8, _len: usize) {
    unsafe {
        let pair = ctx as *const (AtomicBool, AtomicI32);
        (*pair).0.store(true, Ordering::SeqCst);
        (*pair).1.store(code, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn spawn_guarded_completes_failure_when_future_panics() {
    let flag = Box::leak(Box::new((AtomicBool::new(false), AtomicI32::new(0))));
    let tcb = Tcb {
        tcs: flag as *const _ as *mut c_void,
        on_success: record_success as *mut c_void,
        on_failure: record_failure as *mut c_void,
    };
    crate::runtime::runtime().spawn_blocking(move || {}); // ensure runtime warm
    let handle = crate::runtime::runtime().spawn(async move {
        spawn_guarded(tcb, async { panic!("boom") }).await;
    });
    handle.await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(flag.0.load(Ordering::SeqCst), "callback must fire even on panic");
    assert_eq!(flag.1.load(Ordering::SeqCst), crate::error::codes::GENERIC);
}
```

- [x] **Step 2: Run it to confirm it fails (function doesn't exist yet)**

Run: `cd native/fluvio-dotnet && cargo test spawn_guarded_completes_failure_when_future_panics`
Expected: compile error — `spawn_guarded` not found.

- [x] **Step 3: Implement `spawn_guarded`**

```rust
// native/fluvio-dotnet/src/tcb.rs
use std::panic::AssertUnwindSafe;
use futures::FutureExt;

/// Spawns `fut` on the shared runtime, guaranteeing `tcb` is completed exactly once:
/// if `fut` runs to completion it is responsible for calling `complete_success`/
/// `complete_error`/`complete_string_success` itself; if `fut` panics, this catches
/// it and completes `tcb` with a generic failure instead of leaving the C# Task
/// pending forever.
pub fn spawn_guarded<F>(tcb: Tcb, fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    crate::runtime::runtime().spawn(async move {
        let result = AssertUnwindSafe(fut).catch_unwind().await;
        if let Err(panic) = result {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "native task panicked".to_string());
            unsafe { complete_failure(tcb, crate::error::codes::GENERIC, msg) };
        }
    });
}
```

Add `futures = { version = "0.3", features = ["std"] }`'s `FutureExt`/`catch_unwind` — already a dependency; `catch_unwind` requires `std` feature which is default, no `Cargo.toml` change needed. Confirm with `cargo build`.

- [x] **Step 4: Run the test to confirm it passes**

Run: `cd native/fluvio-dotnet && cargo test spawn_guarded_completes_failure_when_future_panics`
Expected: PASS.

- [x] **Step 5: Replace every `runtime().spawn(async move { ... })` with `spawn_guarded`**

In each of `client.rs`, `producer.rs`, `consumer.rs`, `admin.rs`: change

```rust
crate::runtime::runtime().spawn(async move { <body> });
```

to

```rust
crate::tcb::spawn_guarded(tcb, async move { <body> });
```

The `<body>` itself is unchanged — it still owns calling `complete_success`/`complete_error` on every normal path; `spawn_guarded` only adds the panic-time fallback. This is a mechanical find-and-replace across all ~22 entry points; verify none was missed:

```bash
grep -rn "runtime::runtime().spawn(async move" native/fluvio-dotnet/src/
```

Expected: no output (every spawn site now goes through `spawn_guarded`).

- [x] **Step 6: Add a C#-visible regression test proving a real panic doesn't hang**

This requires a way to deliberately trigger a native panic from C# for testing. Add a debug-only FFI function:

```rust
// native/fluvio-dotnet/src/lib.rs, guarded so it never ships in release
#[cfg(debug_assertions)]
#[no_mangle]
pub extern "C" fn ffi_debug_trigger_panic(tcb: crate::tcb::Tcb) {
    crate::tcb::spawn_guarded(tcb, async { panic!("debug-triggered panic for testing") });
}
```

```csharp
// tests/Fluvio.Client.Tests/Interop/PanicBoundaryTests.cs (new file)
using Fluvio.Client.Interop;

namespace Fluvio.Client.Tests.Interop;

public class PanicBoundaryTests
{
    [LibraryImport("fluvio_dotnet", EntryPoint = "ffi_debug_trigger_panic")]
    private static partial void DebugTriggerPanic(Tcb tcb);

    [Fact]
    public async Task NativePanicCompletesTaskWithFailureInsteadOfHanging()
    {
        var task = Callbacks.CallAsync(tcb => DebugTriggerPanic(tcb));
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(task, completed);
        await Assert.ThrowsAsync<FluvioException>(() => task);
    }
}
```

Adjust `Tcb`/`Callbacks` visibility (`internal` → confirm `InternalsVisibleTo Fluvio.Client.Tests` already covers `Interop`, per the original `Fluvio.Client.csproj`). Make `PanicBoundaryTests` class `partial` (required for `LibraryImport`).

Run: `dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~PanicBoundaryTests"`
Expected: PASS, completing in well under 5 seconds (proving no hang).

- [x] **Step 7: Full regression run**

Run: `cd native/fluvio-dotnet && cargo test && cd - && dotnet test Fluvio.Client.sln --filter "FullyQualifiedName!~Integration"`
Expected: all native + unit tests pass (73 including the new panic test).

- [x] **Step 8: Commit**

```bash
git add native/fluvio-dotnet/src tests/Fluvio.Client.Tests/Interop/PanicBoundaryTests.cs
git commit -m "fix: add panic boundary so a native task panic faults the C# Task instead of hanging it"
```

---

### Task 2: Wire cancellation through every non-streaming FFI call

**Files:**

- Create: `native/fluvio-dotnet/src/cancel.rs`
- Modify: `native/fluvio-dotnet/src/lib.rs`, `client.rs`, `producer.rs`, `consumer.rs`, `admin.rs` (every non-streaming `extern "C" fn` gains a trailing `cancel: *mut c_void` param before `tcb: Tcb`)
- Create: `src/Fluvio.Client/Interop/CancellationBridge.cs`
- Modify: `src/Fluvio.Client/Interop/Native.cs` (every non-streaming P/Invoke signature gains a `nint cancel` param)
- Modify: `src/Fluvio.Client/{FluvioClient,Producer/FluvioProducer,Consumer/FluvioConsumer,Admin/FluvioAdmin}.cs` (every call site passes a real cancellation handle instead of ignoring `cancellationToken`)
- Test: `tests/Fluvio.Client.Tests/Integration/ConsumerIntegrationTests.cs` (existing `FetchBatchAsync_EmptyTopic_BlocksUntilTimeout`, now should genuinely pass), plus new cancellation tests for producer/admin

**Interfaces:**

- Produces (Rust): `cancel::CancelHandle` (`Arc<Notify>` wrapper); `#[no_mangle] extern "C" fn ffi_cancel_new() -> *mut c_void`; `extern "C" fn ffi_cancel_trigger(handle: *mut c_void)`; `extern "C" fn ffi_cancel_drop(handle: *mut c_void)`; a helper `pub async fn race<T>(cancel: *mut c_void, fut: impl Future<Output = T>) -> Result<T, Cancelled>` that every call site's body wraps its work in via `tokio::select!` against the handle's `Notify`.
- Produces (C#): `CancellationBridge.Register(nint nativeCancelHandlePtr, CancellationToken ct) -> IDisposable` (registers `ct.Register` to call `ffi_cancel_trigger`, returns a disposable that also frees the handle); every call site does: `var (cancelPtr, registration) = CancellationBridge.Create(ct); using (registration) { ... invoke native with cancelPtr ... }`.
- Consumed by: this is the shared primitive Task 6's producer-timeout fix and Task 9's `FetchBatchAsync_EmptyTopic_BlocksUntilTimeout` fix both build on.

- [x] **Step 1: Write `cancel.rs` with a unit test**

```rust
// native/fluvio-dotnet/src/cancel.rs
use std::os::raw::c_void;
use std::sync::Arc;
use tokio::sync::Notify;

pub struct CancelHandle {
    pub notify: Arc<Notify>,
}

#[no_mangle]
pub extern "C" fn ffi_cancel_new() -> *mut c_void {
    Box::into_raw(Box::new(CancelHandle { notify: Arc::new(Notify::new()) })) as *mut c_void
}

/// # Safety
/// `handle` must have come from `ffi_cancel_new` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_cancel_trigger(handle: *mut c_void) {
    if handle.is_null() { return; }
    let handle = &*(handle as *const CancelHandle);
    handle.notify.notify_waiters();
}

/// # Safety
/// `handle` must have come from `ffi_cancel_new` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_cancel_drop(handle: *mut c_void) {
    if handle.is_null() { return; }
    drop(Box::from_raw(handle as *mut CancelHandle));
}

pub struct Cancelled;

/// Races `fut` against the cancel handle's notification. `cancel` may be null
/// (no cancellation requested for this call) in which case `fut` always wins.
///
/// # Safety
/// `cancel`, if non-null, must have come from `ffi_cancel_new` and remain valid
/// (not dropped) for the duration of this call.
pub async unsafe fn race<T>(cancel: *mut c_void, fut: impl std::future::Future<Output = T>) -> Result<T, Cancelled> {
    if cancel.is_null() {
        return Ok(fut.await);
    }
    let handle = &*(cancel as *const CancelHandle);
    tokio::select! {
        v = fut => Ok(v),
        _ = handle.notify.notified() => Err(Cancelled),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn race_returns_cancelled_when_triggered_before_future_completes() {
        let handle = ffi_cancel_new();
        let h2 = handle;
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            unsafe { ffi_cancel_trigger(h2) };
        });
        let result = unsafe { race(handle, futures::future::pending::<()>()).await };
        assert!(result.is_err());
        unsafe { ffi_cancel_drop(handle) };
    }

    #[tokio::test]
    async fn race_returns_ok_when_future_completes_first() {
        let handle = ffi_cancel_new();
        let result = unsafe { race(handle, async { 42 }).await };
        assert!(matches!(result, Ok(42)));
        unsafe { ffi_cancel_drop(handle) };
    }

    #[tokio::test]
    async fn race_with_null_cancel_never_cancels() {
        let result = unsafe { race(std::ptr::null_mut(), async { 7 }).await };
        assert!(matches!(result, Ok(7)));
    }
}
```

Add `pub mod cancel;` to `lib.rs`.

- [x] **Step 2: Run the new tests**

Run: `cd native/fluvio-dotnet && cargo test cancel::`
Expected: 3 passed.

- [x] **Step 3: Apply `cancel`/`race` to `ffi_consumer_fetch_batch` first (it has the concrete failing integration test)**

```rust
// native/fluvio-dotnet/src/consumer.rs — modify ffi_consumer_fetch_batch's signature and body
pub extern "C" fn ffi_consumer_fetch_batch(
    client: *mut c_void,
    topic: *const u8, topic_len: usize,
    partition: u32, offset: i64, max_bytes: u32,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    // ... existing setup unchanged ...
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*client };
        let work = async {
            let consumer = client.partition_consumer(topic, partition as i32).await?;
            let mut stream = consumer.stream(Offset::absolute(offset)?).await?;
            let mut out = Vec::new();
            let mut bytes_read = 0usize;
            // loop body unchanged, but drop the old per-record 50ms timeout —
            // cancellation is now the caller's responsibility via `cancel`
            loop {
                if bytes_read >= max_bytes as usize { break; }
                match stream.next().await {
                    Some(Ok(record)) => {
                        bytes_read += record.value().len();
                        out.push(box_record(record.offset(), record.timestamp(), partition,
                            record.key().map(|k| k.to_vec()), record.value().to_vec()));
                    }
                    _ => break,
                }
            }
            Ok::<_, anyhow::Error>(out)
        };
        match unsafe { crate::cancel::race(cancel, work).await } {
            Ok(Ok(records)) => { /* box into FFIRecordArray and complete_success, unchanged */ }
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}
```

Note: removing the old 50ms-per-record internal timeout changes `FetchBatchAsync`'s default (no-`CancellationToken`) behavior from "returns quickly with whatever's available" to "waits indefinitely for `max_bytes` worth of data". Re-check `ConsumerIntegrationTests.cs` for any test relying on the old quick-return-when-empty behavior WITHOUT passing a cancellation token — if one exists, that test must now pass an explicit short-timeout `CancellationToken` instead (this is the "resolve the contract" issue the handoff note flagged). Fix the test, don't reintroduce an internal timeout that fights explicit cancellation.

- [x] **Step 4: Wire the C# side for `FetchBatchAsync`**

```csharp
// src/Fluvio.Client/Interop/CancellationBridge.cs
namespace Fluvio.Client.Interop;

internal static class CancellationBridge
{
    public static (nint handle, IDisposable registration) Create(CancellationToken ct)
    {
        var handle = Native.CancelNew();
        if (!ct.CanBeCanceled)
            return (handle, NoopDisposable.Instance);

        var reg = ct.Register(static state =>
        {
            var h = (nint)state!;
            Native.CancelTrigger(h);
        }, handle);

        return (handle, new CombinedDisposable(reg, handle));
    }

    private sealed class NoopDisposable : IDisposable
    {
        public static readonly NoopDisposable Instance = new();
        public void Dispose() { }
    }

    private sealed class CombinedDisposable(CancellationTokenRegistration reg, nint handle) : IDisposable
    {
        public void Dispose()
        {
            reg.Dispose();
            Native.CancelDrop(handle);
        }
    }
}
```

Add to `Native.cs`: `CancelNew`, `CancelTrigger`, `CancelDrop` `[LibraryImport]` declarations, and update `ConsumerFetchBatch`'s signature to add the `nint cancel` param.

```csharp
// FluvioConsumer.cs — FetchBatchAsync
public async Task<IReadOnlyList<ConsumeRecord>> FetchBatchAsync(string topic, int partition = 0, long offset = 0, int maxBytes = 1024 * 1024, CancellationToken cancellationToken = default)
{
    var (cancelHandle, registration) = Interop.CancellationBridge.Create(cancellationToken);
    using var _ = registration;
    var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic);
    nint arrayPtr;
    unsafe
    {
        fixed (byte* tp = topicBytes)
        {
            arrayPtr = await _clientHandle.RunAsyncWithIncrement(h =>
                Interop.Callbacks.CallAsync(tcb =>
                    Interop.Native.ConsumerFetchBatch(h, tp, (nuint)topicBytes.Length, (uint)partition, offset, (uint)maxBytes, cancelHandle, tcb)));
        }
    }
    return Interop.NativeBuffer.ReadRecordArrayAndFree(arrayPtr, partition);
}
```

- [x] **Step 5: Run the previously-failing test**

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~FetchBatchAsync_EmptyTopic_BlocksUntilTimeout"`
Expected: PASS — the 2-second `CancellationTokenSource` now genuinely cancels the native fetch.

- [x] **Step 6: Apply the same `cancel`/`Tcb` pattern to every remaining non-streaming entry point**

Apply identically to: `ffi_client_connect`, `ffi_client_health_check`, `ffi_producer_new`, `ffi_producer_send`, `ffi_producer_flush`, `ffi_consumer_fetch_last_offset`, `ffi_consumer_commit_offset`, and all 12 `ffi_admin_*` functions. Each gains a `cancel: *mut c_void` param before `tcb`, wraps its body in `crate::cancel::race(cancel, ...)`, and completes with `codes::CANCELLED` on the cancelled branch. Update every corresponding `Native.cs` P/Invoke declaration and every C# call site in `FluvioClient.cs`, `FluvioProducer.cs`, `FluvioConsumer.cs`, `FluvioAdmin.cs` to go through `CancellationBridge.Create`.

This is mechanical but touches ~18 more call sites — do it file by file (`client.rs`+`FluvioClient.cs`, then `producer.rs`+`FluvioProducer.cs`, then the rest of `consumer.rs`+`FluvioConsumer.cs`, then `admin.rs`+`FluvioAdmin.cs`), rebuilding after each file to catch signature mismatches early:

```bash
cd native/fluvio-dotnet && cargo build   # after each .rs file
dotnet build Fluvio.Client.sln --configuration Release   # after each .cs file
```

- [x] **Step 7: Add cancellation regression tests for producer and admin**

```csharp
// tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs — add
[Fact]
public async Task SendAsync_CancelledBeforeCompletion_ThrowsOperationCanceledException()
{
    var topic = await CreateTestTopicAsync();
    var producer = Client!.Producer();
    using var cts = new CancellationTokenSource();
    cts.Cancel();
    await Assert.ThrowsAsync<OperationCanceledException>(
        () => producer.SendAsync(topic, new byte[] { 1, 2, 3 }, cancellationToken: cts.Token));
}
```

```csharp
// tests/Fluvio.Client.Tests/Integration/AdminIntegrationTests.cs — add
[Fact]
public async Task ListTopicsAsync_CancelledBeforeCompletion_ThrowsOperationCanceledException()
{
    var admin = Client!.Admin();
    using var cts = new CancellationTokenSource();
    cts.Cancel();
    await Assert.ThrowsAsync<OperationCanceledException>(() => admin.ListTopicsAsync(cts.Token));
}
```

- [x] **Step 8: Full regression run against both clusters**

```bash
cd native/fluvio-dotnet && cargo test && cd -
dotnet build Fluvio.Client.sln --configuration Release /p:TreatWarningsAsErrors=true
FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~Integration" --configuration Release
FLUVIO_TEST_PROFILE=hetzner-tls dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~Integration" --configuration Release
```

Expected: cancellation-related tests pass on both; note (don't fix yet — later tasks own these) any remaining failures from headers/partitioner/offset issues.

- [x] **Step 9: Commit**

```bash
git add native/fluvio-dotnet/src src/Fluvio.Client/Interop src/Fluvio.Client/FluvioClient.cs src/Fluvio.Client/Producer/FluvioProducer.cs src/Fluvio.Client/Consumer/FluvioConsumer.cs src/Fluvio.Client/Admin/FluvioAdmin.cs tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs tests/Fluvio.Client.Tests/Integration/AdminIntegrationTests.cs
git commit -m "fix: wire CancellationToken through every FFI call, not just streaming"
```

**Verification notes (orchestrator, post-execution):** the executing session reported "completed" without
committing (stuck in a background-wait loop); the orchestrator picked up verification directly. Found and
fixed one additional bug while verifying: `ffi_producer_send`'s `value` parameter (native/fluvio-dotnet/src/producer.rs)
was missing the same null-pointer guard `key` already had — a zero-length `ReadOnlyMemory<byte>` value
pins to a null pointer in C#, and `slice::from_raw_parts` on a null pointer is UB that **aborts the whole
process** (not a catchable panic, bypasses Task 1's `spawn_guarded` entirely since aborts don't unwind).
Fixed with the same `if value.is_null() { Vec::new() } else { ... }` pattern as `key`. Verified against
both `local` (reset via `fluvio cluster start` after an unrelated cluster-health issue — a stale local
cluster reported "invalid partition size", unrelated to this task) and `hetzner-tls`: 13 native tests, 73
unit tests, 23 targeted integration tests (connection/admin/consumer/cancellation) pass on both clusters.
Also discovered: running the full `ProducerIntegrationTests`/`BatchFlushIntegrationTests` suites with
default xUnit parallelization against the single-node `local` cluster causes a 60s timeout on an unrelated
concurrent `CreateTopicAsync` call — confirmed to be test-parallelism resource contention, not a Task 2
regression (passes when `-- xunit.parallelizeTestCollections=false` is set). Also discovered (unrelated to
this task, noted for the backlog, not fixed here): `ProducerOptions.BatchSize`/`LingerTime` don't appear to
be wired to any real batching behavior in the FFI producer — `Producer_WithLingerTime_FlushesAfterDelay`,
`Producer_ZeroLingerTime_DisablesAutoFlush`, `Producer_WithSmallBatchSize_FlushesAutomatically`, and
`Producer_Dispose_FlushesBufferedRecords` all fail on assertions about batch/linger timing. This is a
gap in the original Task 3 scope of the first plan, not something Task 2 touched — needs its own task in
a future plan revision.

---

### Task 3: Producer send/flush timeout (30s, matching prior documented behavior)

**Files:**

- Modify: `src/Fluvio.Client/Producer/FluvioProducer.cs`
- Test: `tests/Fluvio.Client.Tests/Producer/ProducerTimeoutTests.cs` (new, unit-level — no cluster needed, uses a cancellation-respecting fake)

**Interfaces:**

- Consumes: Task 2's `CancellationBridge`/cancellation wiring on `ProducerSend`/`ProducerFlush`.
- Produces: `FluvioProducer` internally links any caller-supplied `CancellationToken` with a 30-second timeout via `CancellationTokenSource.CreateLinkedTokenSource` before calling into native, for both `SendAsync` and `FlushAsync`.

- [x] **Step 1: Write the failing test**

Since this doesn't need a real cluster (it tests C#-level timeout wiring, not native behavior), fake it by asserting the linked token's timeout is set correctly via a small internal seam:

```csharp
// src/Fluvio.Client/Producer/FluvioProducer.cs — extract a testable helper
internal static CancellationTokenSource CreateSendTimeoutSource(CancellationToken callerToken) =>
    CancellationTokenSource.CreateLinkedTokenSource(callerToken) is var cts && (cts.CancelAfter(TimeSpan.FromSeconds(30)) == default)
        ? cts : cts; // CancelAfter returns void; see Step 3 for the real, non-contorted implementation
```

```csharp
// tests/Fluvio.Client.Tests/Producer/ProducerTimeoutTests.cs
using Fluvio.Client.Producer;

namespace Fluvio.Client.Tests.Producer;

public class ProducerTimeoutTests
{
    [Fact]
    public void CreateSendTimeoutSource_CancelsAfter30Seconds_WhenCallerTokenNeverCancels()
    {
        using var cts = FluvioProducer.CreateSendTimeoutSource(CancellationToken.None);
        Assert.False(cts.IsCancellationRequested);
        // Can't wait 30s in a unit test; assert via reflection/timer inspection instead —
        // simplest robust check: cancel a linked source manually and confirm caller-token
        // cancellation still propagates (proves it's actually linked, not a bare fresh token).
    }

    [Fact]
    public void CreateSendTimeoutSource_PropagatesCallerCancellation()
    {
        using var callerCts = new CancellationTokenSource();
        using var linked = FluvioProducer.CreateSendTimeoutSource(callerCts.Token);
        callerCts.Cancel();
        Assert.True(linked.IsCancellationRequested);
    }
}
```

- [x] **Step 2: Run to confirm it fails**

Run: `dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ProducerTimeoutTests"`
Expected: FAIL (method doesn't exist / doesn't compile).

- [x] **Step 3: Implement cleanly**

```csharp
// src/Fluvio.Client/Producer/FluvioProducer.cs
internal static CancellationTokenSource CreateSendTimeoutSource(CancellationToken callerToken)
{
    var cts = CancellationTokenSource.CreateLinkedTokenSource(callerToken);
    cts.CancelAfter(TimeSpan.FromSeconds(30));
    return cts;
}
```

Use it in `SendAsync`, `SendBatchAsync`, and `FlushAsync`:

```csharp
public async Task<long> SendAsync(string topic, ReadOnlyMemory<byte> value, ReadOnlyMemory<byte>? key = null, CancellationToken cancellationToken = default)
{
    using var timeoutCts = CreateSendTimeoutSource(cancellationToken);
    // ... existing body uses timeoutCts.Token instead of cancellationToken when creating the CancellationBridge handle ...
}
```

- [x] **Step 4: Run to confirm it passes**

Run: `dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ProducerTimeoutTests"`
Expected: PASS.

- [x] **Step 5: Add one real integration test proving the timeout actually aborts a stalled send**

This can't easily simulate a stalled broker without infrastructure control; instead assert the wiring end-to-end with a very short timeout override for testability:

```csharp
// FluvioProducer.cs — allow override via internal static field for testing only, documented as such
internal static TimeSpan SendTimeoutOverride = TimeSpan.FromSeconds(30);
// CreateSendTimeoutSource uses SendTimeoutOverride instead of the hardcoded literal.
```

```csharp
// tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs — add
[Fact]
public async Task SendAsync_RespectsInternalTimeout_WhenSetVeryShort()
{
    var original = Fluvio.Client.Producer.FluvioProducer.SendTimeoutOverride;
    Fluvio.Client.Producer.FluvioProducer.SendTimeoutOverride = TimeSpan.FromMilliseconds(1);
    try
    {
        var topic = await CreateTestTopicAsync();
        var producer = Client!.Producer();
        // A real send against a healthy cluster may still beat 1ms depending on timing;
        // assert it EITHER completes fast OR throws OperationCanceledException — never hangs.
        var task = producer.SendAsync(topic, new byte[] { 1 });
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(task, completed);
    }
    finally
    {
        Fluvio.Client.Producer.FluvioProducer.SendTimeoutOverride = original;
    }
}
```

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~SendAsync_RespectsInternalTimeout"`
Expected: PASS (completes within 5s either way, proving no hang).

- [x] **Step 6: Commit**

```bash
git add src/Fluvio.Client/Producer/FluvioProducer.cs tests/Fluvio.Client.Tests/Producer/ProducerTimeoutTests.cs tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs
git commit -m "fix: bound producer send/flush with a 30s timeout, matching prior documented behavior"
```

**Verification notes (orchestrator, post-execution):** the executing session again got stuck in a
background-wait loop without committing; orchestrator picked up verification directly (second occurrence
of this pattern — worth a harness/prompt-design fix before running more tasks this way). Found and fixed
a real cross-test contamination bug while verifying: the plan's own Step 5 design (a mutable
`SendTimeoutOverride` static field flipped by a test) leaked across concurrently-scheduled xUnit test
classes even with collection parallelization nominally disabled, causing unrelated `BatchFlushIntegrationTests`
sends to fail with `TaskCanceledException`. Fixed by removing the mutable static entirely — `CreateSendTimeoutSource`
now takes an optional `TimeSpan? timeout` parameter instead, and the integration test passes an
already-short-fused token as the caller token rather than mutating shared state. No static state is
shared between tests, so it cannot leak regardless of xUnit's scheduling. Verified: 13 native tests, 75
unit tests (73 + 2 new), and the specific `SendAsync_RespectsInternalTimeout_WhenVeryShort` test pass
cleanly on both `local` (778ms) and `hetzner-tls` (932ms).

Also discovered (unrelated to this task, real, but out of scope — noted for the backlog): running several
`ProducerIntegrationTests` back-to-back against the `local` dev cluster intermittently hits a genuine
60-second socket timeout **inside the official `fluvio` Rust crate itself** ("Socket io Timed out: 60
secs waiting for response. API_KEY=1001" — this is the `fluvio-socket`/`fluvio-protocol` crate's own
error text, not ours) on a plain `CreateTopicAsync` call, reproducing even on vanilla tests untouched by
this plan (`SendAsync_SingleMessage_Success`). It happens regardless of `xunit.parallelizeTestCollections`
and survives a full cluster reset, so it is not test-parallelism contention (ruled out by direct testing)
but appears to be the local single-node dev SC becoming unresponsive after a number of sequential
`Fluvio::connect_with_config` + `admin()` cycles in a short window — possibly connections not being torn
down promptly on the SC side between test-per-`FluvioClient` reconnects. Needs its own investigation task
in a future plan revision; does not reproduce against `hetzner-tls` in the runs so far.

---

### Task 4: Fix `CommitOffsetAsync` semantics and `StreamAsync` stored-offset resume

**Files:**

- Modify: `native/fluvio-dotnet/src/consumer.rs` (`ffi_consumer_commit_offset`)
- Modify: `src/Fluvio.Client/Consumer/FluvioConsumer.cs` (`StreamAsync`)
- Test: `tests/Fluvio.Client.Tests/Integration/ConsumerIntegrationTests.cs`

**Interfaces:**

- Consumes: `ffi_consumer_fetch_last_offset`/`FetchLastOffsetAsync` (already exists from the original Task 4).
- Produces: `ffi_consumer_commit_offset` no longer requires a record to exist at the committed offset; `FluvioConsumer.StreamAsync` calls `FetchLastOffsetAsync` first when `_options.OffsetReset` is `StoredOrEarliest`/`StoredOrLatest` and a consumer group is configured, passing the real stored offset into `OffsetResolver.ResolveStartOffset` instead of `null`.

- [x] **Step 1: Investigate the real `fluvio` offset-commit API**

Run: `cargo doc --open -p fluvio` (or read `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/fluvio-0.50.1/src/` per the handoff note) to find the correct way to persist a consumer offset WITHOUT requiring a live record at that exact offset — likely a `ConsumerOffsetsClient`/`fetch_offset`/`commit_offset`-style API storing a value directly (this is how the original Task 4 plan assumed it worked; find out why the current implementation instead races a stream for a record and correct it).

- [x] **Step 2: Write the failing test**

```csharp
// tests/Fluvio.Client.Tests/Integration/ConsumerIntegrationTests.cs — add
[Fact]
public async Task CommitOffsetAsync_TipOfLog_SucceedsWithoutRequiringAnExistingRecord()
{
    var topic = await CreateTestTopicAsync();
    var producer = Client!.Producer();
    var offset = await producer.SendAsync(topic, new byte[] { 1 });
    await producer.FlushAsync();

    var consumer = Client!.Consumer();
    // Commit ONE PAST the last produced offset — the log tip, where no record exists yet.
    await consumer.CommitOffsetAsync("test-consumer", topic, 0, offset + 1);

    var stored = await consumer.FetchLastOffsetAsync("test-consumer", topic, 0);
    Assert.Equal(offset + 1, stored);
}
```

- [x] **Step 3: Run to confirm it fails**

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~CommitOffsetAsync_TipOfLog"`
Expected: FAIL — times out or throws "no record found at offset" per the handoff note.

- [x] **Step 4: Fix `ffi_consumer_commit_offset` to use the correct API from Step 1**

(Exact code depends on Step 1's findings — implement using whatever the real `fluvio` crate's offset-storage API is, removing the stream-and-wait-for-a-record logic entirely.)

- [x] **Step 5: Run to confirm it passes**

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~CommitOffsetAsync_TipOfLog"`
Expected: PASS.

- [x] **Step 6: Fix `StreamAsync`'s stored-offset resume**

```csharp
// FluvioConsumer.cs — StreamAsync, replace the hardcoded null
public async IAsyncEnumerable<ConsumeRecord> StreamAsync(string topic, int partition = 0, long? offset = null,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    long? storedOffset = null;
    var consumerId = OffsetResolver.GetConsumerId(_options.ConsumerGroup);
    if (offset is null && consumerId is not null &&
        _options.OffsetReset is OffsetResetStrategy.StoredOrEarliest or OffsetResetStrategy.StoredOrLatest)
    {
        try
        {
            storedOffset = await FetchLastOffsetAsync(consumerId, topic, partition, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to fetch stored offset for {ConsumerId}/{Topic}:{Partition}, falling back to reset strategy", consumerId, topic, partition);
        }
    }
    var startOffset = offset ?? OffsetResolver.ResolveStartOffset(storedOffset, _options.OffsetReset);
    // ... rest unchanged ...
}
```

- [x] **Step 7: Write the resume test (the Review Focus item — commit then confirm the next stream waits, doesn't replay)**

```csharp
// tests/Fluvio.Client.Tests/Integration/ConsumerIntegrationTests.cs — add
[Fact]
public async Task StreamAsync_ResumesFromStoredOffset_WaitsForNewRecordsInsteadOfReplaying()
{
    var topic = await CreateTestTopicAsync();
    var producer = Client!.Producer();
    var lastOffset = await producer.SendAsync(topic, new byte[] { 1 });
    await producer.FlushAsync();

    const string consumerGroup = "resume-test-group";
    await Client!.Consumer(new ConsumerOptions(ConsumerGroup: consumerGroup))
        .CommitOffsetAsync(OffsetResolver.GetConsumerId(consumerGroup)!, topic, 0, lastOffset + 1);

    var options = new ConsumerOptions(ConsumerGroup: consumerGroup, OffsetReset: OffsetResetStrategy.StoredOrLatest);
    var consumer = Client!.Consumer(options);
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
    var received = new List<ConsumeRecord>();
    try
    {
        await foreach (var record in consumer.StreamAsync(topic, cancellationToken: cts.Token))
        {
            received.Add(record);
        }
    }
    catch (OperationCanceledException) { /* expected: no new records within 3s */ }

    Assert.Empty(received); // must NOT have replayed the already-committed record
}
```

- [x] **Step 8: Run both new tests plus the full suite against `local`, then `hetzner-tls`**

```bash
FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~Integration" --configuration Release
FLUVIO_TEST_PROFILE=hetzner-tls dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~Integration" --configuration Release
```

- [x] **Step 9: Commit**

```bash
git add native/fluvio-dotnet/src/consumer.rs src/Fluvio.Client/Consumer/FluvioConsumer.cs tests/Fluvio.Client.Tests/Integration/ConsumerIntegrationTests.cs
git commit -m "fix: correct commit-offset semantics and make StreamAsync resume from stored offset"
```

---

### Task 5: Record headers, end-to-end

**SUPERSEDED (commits 9fd65e6..3d1233a) — headers were removed, not implemented.** Investigating Step 1
led to checking the actual `fluvio` 0.50.1 source: `TopicProducerPool::send(key, value)` takes no headers
parameter, and `ConsumerRecord`/`Record` expose no headers accessor anywhere in the public API (the
wire-level struct's `headers` field is an unused `i64` placeholder, not a key-value array). The official
Rust client this entire FFI rewrite wraps has no way to set or read per-record headers at all — the old
pre-rewrite implementation only had this feature because it spoke the raw wire protocol directly, which
this rewrite deliberately stopped doing. This is a platform/dependency limitation, not a missing method to
find, so it was escalated to the user (a product trade-off, not an implementation ruling) rather than
decided unilaterally. The user chose to remove header support entirely (`ProduceRecord.Headers`,
`ConsumeRecord.Headers`, `RecordHeaders.cs`, `HeadersIntegrationTests.cs`, the `HeadersExample` project)
over building a client-side envelope workaround, which would have made records non-interoperable with
other Fluvio clients/tools. The steps below are left as originally written for the historical record of
what was planned; none of them were executed as written.

**Files:**

- Modify: `native/fluvio-dotnet/src/ffi_types.rs` (`FFIRecord` gains a headers field), `producer.rs` (`ffi_producer_send` gains a headers param), `consumer.rs` (materialize headers into `FFIRecord`)
- Modify: `src/Fluvio.Client/Interop/NativeTypes.cs`/`NativeBuffer.cs`, `Native.cs`
- Modify: `src/Fluvio.Client/Producer/FluvioProducer.cs`, `src/Fluvio.Client/Consumer/FluvioConsumer.cs`
- Test: `tests/Fluvio.Client.Tests/Integration/HeadersIntegrationTests.cs` (existing — should now genuinely pass)

**Interfaces:**

- Produces (Rust): headers cross the FFI boundary as a JSON string (`FFIString`) — `{"key1":"base64value1","key2":"base64value2"}` — appended as an extra param to `ffi_producer_send` (nullable/empty = no headers) and an extra `headers: FFIString` field on `FFIRecord` (empty string = no headers, distinct from a present-but-empty JSON object `{}`).
- Produces (C#): `FluvioProducer.SendAsync`/`SendBatchAsync` serialize `ProduceRecord.Headers` (`IReadOnlyDictionary<string, ReadOnlyMemory<byte>>?`) to that JSON shape before the native call; `FluvioConsumer`'s record materialization deserializes `FFIRecord.headers` back into `ConsumeRecord.Headers`.

- [ ] **Step 1: Write the failing integration test (likely already exists — confirm and extend)**

Read `tests/Fluvio.Client.Tests/Integration/HeadersIntegrationTests.cs` first; per the handoff note all 8 non-empty-header cases already fail. Add binary-value and empty-vs-null coverage if missing:

```csharp
[Fact]
public async Task SendAsync_HeaderWithBinaryValue_RoundTripsExactBytes()
{
    var topic = await CreateTestTopicAsync();
    var binaryValue = new byte[] { 0x00, 0xFF, 0x80, 0x01, 0xFE };
    var headers = new Dictionary<string, ReadOnlyMemory<byte>> { ["binary-header"] = binaryValue };
    var producer = Client!.Producer();
    await producer.SendAsync(topic, new byte[] { 1 }, headers: headers);
    await producer.FlushAsync();

    var records = await Client!.Consumer().FetchBatchAsync(topic, offset: 0);
    var record = Assert.Single(records);
    Assert.NotNull(record.Headers);
    Assert.True(record.Headers!["binary-header"].Span.SequenceEqual(binaryValue));
}

[Fact]
public async Task SendAsync_NoHeaders_ConsumeRecordHeadersIsNull()
{
    var topic = await CreateTestTopicAsync();
    var producer = Client!.Producer();
    await producer.SendAsync(topic, new byte[] { 1 }); // no headers arg
    await producer.FlushAsync();

    var records = await Client!.Consumer().FetchBatchAsync(topic, offset: 0);
    Assert.Null(Assert.Single(records).Headers);
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~HeadersIntegrationTests"`
Expected: FAIL (headers dropped).

- [ ] **Step 3: Add the headers field to `FFIRecord` and update size assertions**

```rust
// native/fluvio-dotnet/src/ffi_types.rs
#[repr(C)]
pub struct FFIRecord {
    pub offset: i64,
    pub timestamp: i64,
    pub partition: u32,
    pub key: FFISlice,
    pub value: FFISlice,
    pub headers: FFISlice, // UTF-8 JSON, empty (len=0) means "no headers"
}
```

Update `box_record` to accept and box a `headers_json: String`, and update the size assert (`72` = `56 + 16`, recompute exactly and assert it). Update `ffi_record_free` to also free the headers allocation.

- [ ] **Step 4: Add headers param to `ffi_producer_send` and materialize `FFIRecord.headers` in consumer paths**

```rust
// producer.rs — ffi_producer_send gains headers_json: *const u8, headers_json_len: usize
// parse (if non-empty) into a HashMap<String, Vec<u8>> (base64-decode each value) and pass
// to the fluvio record builder's header-setting API (check cargo doc for the exact method —
// fluvio::RecordKey / Record builder header support).
```

```rust
// consumer.rs — wherever box_record is called (fetch_batch, stream_next), serialize the
// source record's headers (if the fluvio Record type exposes them) to the same JSON shape
// before calling box_record.
```

- [ ] **Step 5: Update C# interop and `FluvioProducer`/`FluvioConsumer`**

Update `Native.cs`'s `ProducerSend` signature, `NativeTypes.cs`'s `FFIRecordLayout` (add `Headers` field), `NativeBuffer.ToConsumeRecord` (deserialize headers JSON into `IReadOnlyDictionary<string, ReadOnlyMemory<byte>>?`), and `FluvioProducer.SendAsync`/`SendBatchAsync` (serialize `ProduceRecord.Headers` to the JSON shape, base64-encoding each value, before the native call).

- [ ] **Step 6: Run the tests again**

Run: `cd native/fluvio-dotnet && cargo build && cargo test && cd -`
Run: `dotnet build Fluvio.Client.sln --configuration Release`
Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~HeadersIntegrationTests"`
Expected: all pass, including the two new binary/null cases.

- [ ] **Step 7: Run against `hetzner-tls` too**

Run: `FLUVIO_TEST_PROFILE=hetzner-tls dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~HeadersIntegrationTests"`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add native/fluvio-dotnet/src src/Fluvio.Client/Interop src/Fluvio.Client/Producer/FluvioProducer.cs src/Fluvio.Client/Consumer/FluvioConsumer.cs tests/Fluvio.Client.Tests/Integration/HeadersIntegrationTests.cs
git commit -m "feat: implement record headers end-to-end over the FFI boundary"
```

---

### Task 6: Custom partitioner support in the producer

**Files:**

- Modify: `src/Fluvio.Client/Producer/FluvioProducer.cs` (compute target partition via `IPartitioner` in C#, pass explicit partition to native)
- Modify: `native/fluvio-dotnet/src/producer.rs` (`ffi_producer_send` gains a `partition: i32` param, `-1` = let the server/default partitioner choose)
- Test: `tests/Fluvio.Client.Tests/Integration/ProducerPartitionerIntegrationTests.cs` (new — see Task 9 for the test split)

**Interfaces:**

- Consumes: `IPartitioner.SelectPartition`, `PartitionerConfig` (unchanged, from `Fluvio.Client.Abstractions`); `ProducerOptions.Partitioner`.
- Produces: `FluvioProducer` calls `_options.Partitioner?.SelectPartition(...)` when set (falling back to the existing `SiphashRoundRobinPartitioner` default per current behavior) to compute an explicit partition index BEFORE calling native `ffi_producer_send`, which now sends directly to that partition.

- [x] **Step 1: Investigate explicit-partition send in the real `fluvio` crate**

Check `cargo doc --open -p fluvio` / the cached source under `~/.cargo/registry/.../fluvio-0.50.1/src/` for how to send to a specific partition (likely a partition-targeted send variant on `TopicProducer`, or a `RecordKey`/producer-config option). Confirm `_options.Partitioner`/`PartitionerConfig.AvailablePartitions` needs the actual partition count — check whether that's already available (e.g. from `CreateTopicAsync`'s known partition count, or query via admin) since `PartitionerConfig` requires it.

- [x] **Step 2: Write the failing test**

```csharp
// tests/Fluvio.Client.Tests/Integration/ProducerPartitionerIntegrationTests.cs
public class ProducerPartitionerIntegrationTests : FluvioIntegrationTestBase
{
    private sealed class AlwaysPartitionZero : IPartitioner
    {
        public int SelectPartition(string topic, ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte> value, PartitionerConfig config) => 0;
    }

    [Fact]
    public async Task SendAsync_WithCustomPartitioner_AllRecordsGoToSelectedPartition()
    {
        var topic = await CreateTestTopicAsync(partitions: 3);
        var producer = Client!.Producer(new ProducerOptions(Partitioner: new AlwaysPartitionZero()));
        for (var i = 0; i < 10; i++)
        {
            await producer.SendAsync(topic, new byte[] { (byte)i });
        }
        await producer.FlushAsync();

        var partition0 = await Client!.Consumer().FetchBatchAsync(topic, partition: 0, offset: 0);
        var partition1 = await Client!.Consumer().FetchBatchAsync(topic, partition: 1, offset: 0);
        var partition2 = await Client!.Consumer().FetchBatchAsync(topic, partition: 2, offset: 0);

        Assert.Equal(10, partition0.Count);
        Assert.Empty(partition1);
        Assert.Empty(partition2);
    }
}
```

- [x] **Step 3: Run to confirm it fails**

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ProducerPartitionerIntegrationTests"`
Expected: FAIL — records spread across all 3 partitions (current default behavior ignores `Partitioner`).

- [x] **Step 4: Implement**

Update `ffi_producer_send`'s signature and body per Step 1's findings; update `FluvioProducer.SendAsync` to call `_options.Partitioner.SelectPartition(...)` (falling back to `SiphashRoundRobinPartitioner` when unset, matching current default) and pass the resulting partition index to native.

- [x] **Step 5: Run to confirm it passes**

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ProducerPartitionerIntegrationTests"`
Expected: PASS.

- [x] **Step 6: Confirm the default (no explicit partitioner) round-robin/siphash behavior still works**

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~PartitionerTests"`
Expected: existing unit tests for `SiphashRoundRobinPartitioner` still pass unchanged (pure logic, untouched).

- [x] **Step 7: Run against `hetzner-tls`**

Run: `FLUVIO_TEST_PROFILE=hetzner-tls dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ProducerPartitionerIntegrationTests"`
Expected: PASS.

- [x] **Step 8: Commit**

```bash
git add native/fluvio-dotnet/src/producer.rs src/Fluvio.Client/Producer/FluvioProducer.cs tests/Fluvio.Client.Tests/Integration/ProducerPartitionerIntegrationTests.cs
git commit -m "feat: honor IPartitioner/ProducerOptions.Partitioner by sending to an explicit partition"
```

---

### Task 7: Fix the producer create/dispose race

**Files:**

- Modify: `src/Fluvio.Client/Producer/FluvioProducer.cs`
- Test: `tests/Fluvio.Client.Tests/Integration/BatchFlushIntegrationTests.cs`

**Interfaces:**

- Consumes: nothing new.
- Produces: `FluvioProducer` tracks in-flight handle-creation `Task`s (not just completed handles) so `DisposeAsync`/`FlushAsync` can await any creation still in progress before acting, and marks itself "sealed" atomically so a creation that races a concurrent `Dispose` either completes and is immediately disposed, or is rejected before starting — never leaves a handle created-but-untracked.

- [x] **Step 1: Read the current implementation and confirm the race**

Read `FluvioProducer.cs`'s `GetOrCreateProducerHandleAsync`, `DisposeAsync`, and `FlushAsync` in full. Confirm the handoff note's diagnosis: `DisposeAsync` can mark disposed / snapshot the (possibly still-empty) handle dictionary / dispose `_producerCreationLock` while a concurrent `GetOrCreateProducerHandleAsync` is mid-flight and later publishes a handle into a disposed structure.

- [x] **Step 2: Write the failing stress test**

```csharp
// tests/Fluvio.Client.Tests/Integration/BatchFlushIntegrationTests.cs — add
[Fact]
public async Task ConcurrentSendAndDispose_DoesNotHangOrThrowUnexpectedly()
{
    var topic = await CreateTestTopicAsync();
    var producer = Client!.Producer();

    var sendTasks = Enumerable.Range(0, 20)
        .Select(i => Task.Run(async () =>
        {
            try { await producer.SendAsync(topic, new byte[] { (byte)i }); }
            catch (ObjectDisposedException) { /* acceptable if dispose won the race */ }
        }))
        .ToArray();

    var disposeTask = Task.Run(async () =>
    {
        await Task.Delay(5); // let some sends start first
        await producer.DisposeAsync();
    });

    var all = Task.WhenAll(sendTasks.Append(disposeTask));
    var completed = await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(10)));
    Assert.Same(all, completed); // must not hang
}
```

- [x] **Step 3: Run to confirm it's flaky/fails/hangs**

Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ConcurrentSendAndDispose"` (repeat 5-10 times — races are often intermittent)
Expected: fails or hangs at least once across repeated runs.

- [x] **Step 4: Fix the race**

Restructure `FluvioProducer`'s handle lifecycle: use a single `SemaphoreSlim` (or a lock) guarding a `Dictionary<string, Task<RustResource>>` (not `Dictionary<string, RustResource>`) so `GetOrCreateProducerHandleAsync` always awaits the SAME in-flight creation `Task` rather than racing a fresh one; add a `_disposed` flag checked under the same lock before starting a new creation, and have `DisposeAsync` await all currently-tracked creation `Task`s (not just completed handles) before disposing each one, then dispose the lock/semaphore last.

- [x] **Step 5: Run the stress test repeatedly to confirm the fix**

Run: `for i in {1..10}; do FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ConcurrentSendAndDispose" --configuration Release || break; done`
Expected: 10/10 passes, no hangs.

- [x] **Step 6: Run against `hetzner-tls`**

Run: `FLUVIO_TEST_PROFILE=hetzner-tls dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ConcurrentSendAndDispose"`
Expected: PASS.

- [x] **Step 7: Commit**

```bash
git add src/Fluvio.Client/Producer/FluvioProducer.cs tests/Fluvio.Client.Tests/Integration/BatchFlushIntegrationTests.cs
git commit -m "fix: eliminate producer create/dispose race on concurrent SendAsync and DisposeAsync"
```

---

### Task 8: Remove dead resilience options and fix typed-exception usage

**Files:**

- Modify: `src/Fluvio.Client.Abstractions/IFluvioClient.cs` (remove `EnableCircuitBreaker`, `MaxRetries`, `RetryBaseDelay`, `CircuitBreakerFailureThreshold`, `CircuitBreakerDuration`, `EnableAutoReconnect`, `MaxReconnectAttempts`, `ReconnectDelay` from `FluvioClientOptions`)
- Modify: `src/Fluvio.Client/FluvioException.cs` (remove `IncompatiblePlatformVersionException`)
- Modify: `tests/Fluvio.Client.Tests/PlatformVersionTests.cs` (remove or repurpose — see Step 3)
- Modify: `examples/ProducerExample/Program.cs`, `examples/StreamingConsumerExample/Program.cs` (catch typed exceptions instead of message substrings)

**Interfaces:**

- Produces: `FluvioClientOptions` no longer has resilience knobs that silently do nothing (per spec §9: "resilience moves to the Rust client / connection layer; no equivalent wrapper is kept at the C# level" — this task makes that decision visible in the API instead of leaving dead fields). `FluvioException`'s hierarchy (`FluvioConnectionException`, `TopicNotFoundException`, `TopicAlreadyExistsException`) is what callers should catch, not `IncompatiblePlatformVersionException` (removed) or message substrings.

- [x] **Step 1: Grep for every usage of the fields being removed**

```bash
grep -rn "EnableCircuitBreaker\|MaxRetries\|RetryBaseDelay\|CircuitBreakerFailureThreshold\|CircuitBreakerDuration\|EnableAutoReconnect\|MaxReconnectAttempts\|ReconnectDelay\|IncompatiblePlatformVersionException" src/ tests/ examples/ --include="*.cs"
```

Confirm every hit is either the definition itself or a place this task will update.

- [x] **Step 2: Remove the dead options from `FluvioClientOptions`**

Delete the eight parameters/properties and their XML doc comments from the `FluvioClientOptions` record in `IFluvioClient.cs`.

- [x] **Step 3: Remove `IncompatiblePlatformVersionException` and its test**

Delete the class from `FluvioException.cs`. In `PlatformVersionTests.cs`: if every test in that file only constructs/asserts on `IncompatiblePlatformVersionException` directly (per the review finding), delete the file entirely (`git rm tests/Fluvio.Client.Tests/PlatformVersionTests.cs`) rather than leave a test file asserting on a deleted type. If any test in that file covers something else (re-check before deleting), keep those and remove only the dead ones.

- [x] **Step 4: Build and fix fallout**

Run: `dotnet build Fluvio.Client.sln --configuration Release /p:TreatWarningsAsErrors=true`
Expected: compile errors at every remaining usage — fix each (should be none beyond what Step 1 found, but this catches anything missed).

- [x] **Step 5: Fix the example projects' exception handling**

```csharp
// examples/ProducerExample/Program.cs — replace
catch (FluvioException ex) when (ex.Message.Contains("AlreadyExists"))
// with
catch (TopicAlreadyExistsException)
```

Apply the same pattern to `examples/StreamingConsumerExample/Program.cs`'s equivalent catch.

- [x] **Step 6: Build and run all examples to confirm they still work**

Run: `dotnet build Fluvio.Client.sln --configuration Release`
Run each example against `local` manually (e.g. `dotnet run --project examples/ProducerExample --` with `FLUVIO_TEST_PROFILE`-equivalent env var or whatever config the example reads) and confirm the "topic already exists" path is hit and handled gracefully on a second run.

- [x] **Step 7: Full unit test run**

Run: `dotnet test Fluvio.Client.sln --filter "FullyQualifiedName!~Integration"`
Expected: passes (count will be slightly lower than before if `PlatformVersionTests.cs` was deleted — confirm the exact new count and note it, don't be alarmed by a lower number here).

- [x] **Step 8: Commit**

```bash
git add src/Fluvio.Client.Abstractions/IFluvioClient.cs src/Fluvio.Client/FluvioException.cs tests/Fluvio.Client.Tests/PlatformVersionTests.cs examples/ProducerExample/Program.cs examples/StreamingConsumerExample/Program.cs
git commit -m "fix: remove dead resilience options and use typed exceptions instead of message matching"
```

---

### Task 9: Fix the two hanging partitioner tests and split them out

**Files:**

- Modify: `tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs` (remove the two hanging tests, per the handoff note's diagnosis)
- Modify: `tests/Fluvio.Client.Tests/Integration/ProducerPartitionerIntegrationTests.cs` (created in Task 6 — confirm it already covers the intent of the removed tests; add any missing case, e.g. `SendAsync_SameKey_GoesToSamePartition`)

**Interfaces:**

- Consumes: Task 6's explicit-partition send.
- Produces: no more tests that enumerate an infinite `StreamAsync` without a cancellation bound.

- [x] **Step 1: Locate and read the two hanging tests in full**

```bash
grep -n "SendAsync_WithSpecificPartitioner_AllRecordsGoToSamePartition\|SendAsync_SameKey_GoesToSamePartition" tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs
```

Read their full bodies to understand exactly what they were trying to prove.

- [x] **Step 2: Confirm `ProducerPartitionerIntegrationTests.cs` (Task 6) already proves the specific-partitioner case; add the same-key case if missing**

```csharp
// tests/Fluvio.Client.Tests/Integration/ProducerPartitionerIntegrationTests.cs — add if not covered
[Fact]
public async Task SendAsync_SameKey_AlwaysGoesToSamePartition()
{
    var topic = await CreateTestTopicAsync(partitions: 3);
    var producer = Client!.Producer(); // default SiphashRoundRobinPartitioner
    var key = new byte[] { 0xAB, 0xCD };
    for (var i = 0; i < 10; i++)
    {
        await producer.SendAsync(topic, new byte[] { (byte)i }, key: key);
    }
    await producer.FlushAsync();

    var counts = new List<int>();
    for (var p = 0; p < 3; p++)
    {
        counts.Add((await Client!.Consumer().FetchBatchAsync(topic, partition: p, offset: 0)).Count);
    }
    Assert.Single(counts.Where(c => c == 10)); // all 10 landed in exactly one partition
}
```

- [x] **Step 3: Delete the two hanging tests from `ProducerIntegrationTests.cs`**

```bash
git rm --cached /dev/null 2>/dev/null || true  # no-op guard; actually edit the file to remove the two test methods
```

Edit `ProducerIntegrationTests.cs` directly to delete `SendAsync_WithSpecificPartitioner_AllRecordsGoToSamePartition` and `SendAsync_SameKey_GoesToSamePartition` in full (method + attributes), since their intent now lives in `ProducerPartitionerIntegrationTests.cs` with bounded, non-hanging assertions.

- [x] **Step 4: Run the full producer + partitioner integration suite with a hard timeout to prove no more hangs**

```bash
timeout 60 dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ProducerIntegrationTests|FullyQualifiedName~ProducerPartitionerIntegrationTests" --configuration Release
```

Expected: completes well within 60s (previously hung indefinitely).

- [x] **Step 5: Run against `hetzner-tls`**

```bash
FLUVIO_TEST_PROFILE=hetzner-tls timeout 60 dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ProducerIntegrationTests|FullyQualifiedName~ProducerPartitionerIntegrationTests" --configuration Release
```

Expected: same.

- [x] **Step 6: Commit**

```bash
git add tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs tests/Fluvio.Client.Tests/Integration/ProducerPartitionerIntegrationTests.cs
git commit -m "test: replace hanging unbounded-stream partitioner tests with bounded FetchBatch-based ones"
```

---

### Task 10: Fix remaining review-finding leftovers (allocation leaks, `IgnoreRackAssignment`)

**Files:**

- Modify: `native/fluvio-dotnet/src/consumer.rs` (`ffi_consumer_fetch_batch`, `ffi_stream_next` — free partially-accumulated records on an error/cancellation path instead of leaking)
- Modify: `src/Fluvio.Client/Admin/FluvioAdmin.cs` (`BuildTopicSpecJson` includes `IgnoreRackAssignment`)
- Modify: `native/fluvio-dotnet/src/admin.rs` (`ffi_admin_create_topic` reads and applies it instead of hardcoding `None`)
- Test: `tests/Fluvio.Client.Tests/Integration/AdminIntegrationTests.cs`

**Interfaces:**

- Consumes: nothing new.
- Produces: no change to any public signature — pure bug fixes.

- [x] **Step 1: Fix the allocation leak in `ffi_consumer_fetch_batch`**

In the loop that accumulates `out: Vec<*mut c_void>` via `box_record`, if the loop exits via an error AFTER some records were already boxed, free those before returning the error:

```rust
// on the error path inside the loop, before propagating the error:
for ptr in out.drain(..) {
    unsafe { crate::ffi_types::ffi_record_free(ptr) };
}
```

Apply the same pattern anywhere else records are boxed before a possible later failure in the same function (check `ffi_stream_next` too, though it boxes only one record at a time so the leak surface there is narrower — confirm and fix if present).

- [x] **Step 2: Write a regression test for `IgnoreRackAssignment`**

```csharp
// tests/Fluvio.Client.Tests/Integration/AdminIntegrationTests.cs — add
[Fact]
public async Task CreateTopicAsync_WithIgnoreRackAssignment_DoesNotThrow()
{
    var admin = Client!.Admin();
    var topicName = GenerateTopicName();
    await admin.CreateTopicAsync(topicName, new TopicSpec(Partitions: 1, ReplicationFactor: 1, IgnoreRackAssignment: true));
    var topic = await admin.GetTopicAsync(topicName);
    Assert.NotNull(topic);
}
```

(This is a smoke test since a single-node dev cluster can't easily prove rack-assignment was actually skipped vs. irrelevant — the point is the flag reaches native and round-trips without error; if the real `fluvio` admin API surfaces the value back in topic metadata, assert on that instead.)

- [x] **Step 3: Wire `IgnoreRackAssignment` through**

```csharp
// FluvioAdmin.cs — BuildTopicSpecJson
writer.WriteBoolean("ignoreRackAssignment", spec.IgnoreRackAssignment);
```

```rust
// admin.rs — ffi_admin_create_topic, replace the hardcoded None
let ignore_rack: bool = spec_value["ignoreRackAssignment"].as_bool().unwrap_or(false);
let spec = TopicSpec::new_computed(partitions, replication, if ignore_rack { Some(true) } else { None });
```

(Adjust to whatever `TopicSpec::new_computed`'s actual third-parameter semantics are, per the original Task 6's own note that this needed checking against `cargo doc`.)

- [x] **Step 4: Run tests**

Run: `cd native/fluvio-dotnet && cargo build && cargo test && cd -`
Run: `FLUVIO_TEST_PROFILE=local dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~AdminIntegrationTests|FullyQualifiedName~ConsumerIntegrationTests"`
Expected: pass, including the new `IgnoreRackAssignment` test.

- [x] **Step 5: Commit**

```bash
git add native/fluvio-dotnet/src/consumer.rs native/fluvio-dotnet/src/admin.rs src/Fluvio.Client/Admin/FluvioAdmin.cs tests/Fluvio.Client.Tests/Integration/AdminIntegrationTests.cs
git commit -m "fix: stop leaking boxed records on fetch errors, wire IgnoreRackAssignment through to native"
```

---

### Task 11: Finalize CI against `hetzner-tls`

**Files:**

- Modify: `.github/workflows/integration-tests.yml` (adopt/finalize Codex's draft)
- Modify: `docs/integration-testing.md` (adopt/finalize)
- Delete: `docs/CODEX-HANDOFF-2026-09-26.md` (superseded by this plan and its commits — historical value only, not meant to stay in the tree)

**Interfaces:**

- Consumes: `IntegrationTestConfig`'s `FLUVIO_TEST_PROFILE` env var (from Task 0).
- Produces: a working GitHub Actions job that runs the integration suite against the real `hetzner-tls` cluster using a `FLUVIO_CONFIG` (or equivalent) GitHub secret, verified end-to-end at least once via `workflow_dispatch` before merging.

- [x] **Step 1: Read the current draft in full**

```bash
git diff .github/workflows/integration-tests.yml
cat docs/integration-testing.md
```

- [x] **Step 2: Confirm with the user whether the `FLUVIO_CONFIG` secret (or whatever the draft expects) already exists in the GitHub repo**

This requires the user's own GitHub access — do not attempt to inspect or guess secret names/values yourself. Ask directly: "Does a secret matching what this workflow expects already exist in the repo's Actions secrets? If not, here's exactly what needs to be added: <quote the draft's expected secret name/shape>."

- [x] **Step 3: Once confirmed, validate the workflow YAML**

```bash
# If actionlint or a similar tool is available:
which actionlint && actionlint .github/workflows/integration-tests.yml
# Otherwise at minimum:
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/integration-tests.yml'))" && echo "valid YAML"
```

- [x] **Step 4: Trigger a real run via `workflow_dispatch` and confirm it passes**

```bash
gh workflow run integration-tests.yml --ref worktree-rust-ffi-rewrite
gh run watch  # or poll `gh run list --workflow=integration-tests.yml`
```

(Requires the branch to be pushed — confirm with the user before pushing, since this plan's branch hasn't been pushed to `origin` yet.)

- [x] **Step 5: Fix anything the real CI run surfaces that local testing didn't (e.g. network policy differences, secret formatting)**

- [x] **Step 6: Delete the now-superseded handoff doc and commit everything**

```bash
git rm docs/CODEX-HANDOFF-2026-09-26.md
git add .github/workflows/integration-tests.yml docs/integration-testing.md
git commit -m "ci: run integration tests against the real hetzner-tls cluster"
```

---

## Self-Review Notes

- **Coverage:** every one of the 10 original code-review findings has an owning task — StreamAsync/stored-offset (Task 4), cancellation (Task 2), commit-offset semantics (Task 4), headers (Task 5), panic safety (Task 1), producer timeout (Task 3), dead resilience options (Task 8), `IgnoreRackAssignment` (Task 10), `IncompatiblePlatformVersionException` (Task 8), error-message matching in examples (Task 8). Every Codex-diagnosed hang/bug also has an owning task — hanging tests (Task 9), missing partitioner (Task 6), dispose race (Task 7), allocation leaks (Task 10). CI finalization is Task 11.
- **Type consistency:** `CancellationBridge.Create`/`Native.CancelNew/CancelTrigger/CancelDrop` (Task 2) are the single shared primitive every later cancellation-touching site (Task 3's timeout, Task 9's regression tests) builds on by name.
- **Sequencing:** Task 1 (panic boundary) is first because every subsequent task's debugging depends on failures faulting cleanly instead of hanging. Task 2 (cancellation) is second because Task 3's timeout and several review-finding fixes assume it exists. Task 0 must run first of all since every later task's tests connect via its (already-written, just uncommitted) profile-resolution code.
- **Review Focus:** each of the five items has its owning task's Step spelled out above (panic test in Task 1 Step 6, cancellation-actually-stops-native-work proof in Task 2, concurrent stress test in Task 7 Step 2/5, binary+null header cases in Task 5 Step 1, stored-offset-waits-not-replays test in Task 4 Step 7).
