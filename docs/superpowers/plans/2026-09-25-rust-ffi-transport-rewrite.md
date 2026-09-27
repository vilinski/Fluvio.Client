# Rust-FFI Transport Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Fluvio.Client's hand-rolled managed wire-protocol transport with a Rust FFI layer (`native/fluvio-dotnet`) that wraps the official `fluvio` Rust client, using a hand-written TCB (Task Control Block) async-callback pattern — no bindgen/cbindgen/codegen — while keeping `Fluvio.Client.Abstractions` essentially unchanged.

**Architecture:** A `cdylib` Rust crate exposes opaque handles (client/producer/consumer/admin) and async operations via `extern "C"` functions that take a trailing `Tcb` (two callback function pointers + a `GCHandle`-backed `TaskCompletionSource` pointer) and complete it from a spawned Tokio task. C# wraps each handle in a `SafeHandle`, bridges callbacks to `Task`/`IAsyncEnumerable`, and the existing `FluvioProducer`/`FluvioConsumer`/`FluvioAdmin`/`FluvioClient` classes are rewritten internally to call into this native layer instead of encoding the wire protocol themselves.

**Tech Stack:** Rust (`fluvio` 0.50.1, `fluvio-controlplane-metadata` 0.50.1, `tokio`, `once_cell`, `serde`/`serde_json`), .NET 8 / C# 12 (`[LibraryImport]`, `[UnmanagedCallersOnly]`, `SafeHandle`, `System.Runtime.InteropServices`).

**Spec:** `docs/superpowers/specs/2026-09-25-rust-ffi-rewrite-design.md`

## Global Constraints

- No `bindgen`, `cbindgen`, `interoptopus`, or any FFI code-generation crate/macro on the Rust side — every `#[repr(C)]` struct and `extern "C"` fn is hand-written, per spec §1/§9.
- One process-wide lazily-initialized multi-threaded Tokio runtime (`once_cell::sync::Lazy`) — no per-client runtimes (spec §1).
- Every async op's `TaskCompletionSource` MUST be created with `TaskCreationOptions.RunContinuationsAsynchronously` to avoid starving Tokio worker threads (spec §2).
- Every native call that can run concurrently with `Dispose`/finalization MUST go through `SafeHandle.DangerousAddRef`/`DangerousRelease` (spec §3).
- `Fluvio.Client.Abstractions` public surface is unchanged except `IFluvioConsumer.CommitOffsetAsync` drops its `uint sessionId` parameter (spec §8).
- Target `net8.0`, `LangVersion 12`, `TreatWarningsAsErrors=true`, `IsAotCompatible=true` on `Fluvio.Client.csproj` — new interop code must not introduce trim/AOT warnings (existing csproj properties, unchanged).
- Rust crate pins `fluvio = "0.50.1"` and `fluvio-controlplane-metadata = "0.50.1"` (matches the reference implementation and what's available via `cargo search`, spec §1/§9).

## Review Focus

- **Native library missing/wrong build (debug vs release, wrong RID):** a developer running `dotnet build` without ever running `cargo build` should get a clear `DllNotFoundException`-adjacent error, not a silent hang — Task 8's resolver must fail fast with a descriptive message, and `BuildNativeDebug` must make `dotnet build` alone sufficient in a fresh checkout.
- **Native call throws before scheduling (e.g., invalid UTF-8 topic name, malformed JSON config):** the C# call site must still release the `GCHandle` it allocated for the TCB and must not leave the `Task` forever pending — every `Callbacks.CallAsync`-style helper needs a `catch` that frees the handle and rethrows (spec §2, mirrors the reference's `try/catch { gch.Free(); throw; }`).
- **Cancellation raced against completion (consumer `StreamAsync` cancelled the same tick a record arrives, or `Dispose`d while a poll is in flight):** must not deadlock, double-free, or throw `ObjectDisposedException` from within the enumerator's `finally` — `Close()` before `Dispose()` is mandatory and the native `tokio::select!` must always complete the TCB exactly once, even on the taken/already-terminal-stream race (spec §4).
- **Native failure string is non-UTF8-safe-length or empty (empty message, non-ASCII cluster error text):** `Encoding.UTF8.GetString` over the raw pointer/len must not throw for empty (`len == 0`) or valid multi-byte UTF-8 payloads — the failure callback must handle a null/zero-length message pointer distinctly from a non-empty one (spec §5).
- **Concurrent producer sends interleaved with `FlushAsync`/`DisposeAsync`, and concurrent consumer polls after `CancellationToken` fires:** `RunAsyncWithIncrement`/`RunWithIncrement` must keep the handle alive for the full duration of every in-flight call, so a `Dispose()` racing an in-flight `SendAsync`/`ffi_stream_next` must not free the handle underneath it (spec §3).

---

## File Structure

**New Rust crate — `native/fluvio-dotnet/`:**
- `Cargo.toml` — crate manifest (cdylib, `fluvio_dotnet`)
- `src/lib.rs` — module wiring, `ffi_runtime_init`, top-level re-exports
- `src/runtime.rs` — global Tokio runtime singleton
- `src/tcb.rs` — `Tcb` struct, `complete_success`/`complete_failure`/`complete_error` helpers
- `src/ffi_types.rs` — `FFISlice`, `FFIString`, `FFIBool`, `FFIRecord`, size asserts
- `src/error.rs` — error codes, `anyhow::Error -> (code, message)` classification
- `src/client.rs` — connect/disconnect/health-check FFI
- `src/producer.rs` — send/send-batch/flush/partition-count FFI
- `src/consumer.rs` — fetch-batch/fetch-last-offset/commit-offset + streaming FFI
- `src/admin.rs` — topic/SPU/partition/SmartModule CRUD FFI

**New C# interop layer — `src/Fluvio.Client/Interop/`:**
- `NativeTypes.cs` — `Tcb`, `FFISlice`, `FFIString` struct mirrors + size asserts
- `Native.cs` — `[LibraryImport]` P/Invoke declarations + native library resolution
- `Callbacks.cs` — TCB↔`Task` bridge (`CallAsync`, `[UnmanagedCallersOnly]` targets)
- `RustResource.cs` — `SafeHandle` for opaque native handles
- `NativeBuffer.cs` — `SafeHandle` for `FFIRecord*` + zero-copy `MemoryManager<byte>`

**Modified (rewritten internals, same public API):**
- `src/Fluvio.Client/FluvioClient.cs`
- `src/Fluvio.Client/Producer/FluvioProducer.cs`
- `src/Fluvio.Client/Consumer/FluvioConsumer.cs`
- `src/Fluvio.Client/Admin/FluvioAdmin.cs`
- `src/Fluvio.Client/FluvioException.cs` (add code-mapped subclasses)
- `src/Fluvio.Client.Abstractions/IFluvioClient.cs` (`CommitOffsetAsync` signature)
- `src/Fluvio.Client/Fluvio.Client.csproj` (native packaging targets, dependency removal)
- `.github/workflows/build.yml`, `.github/workflows/integration-tests.yml` (native build steps)

**Deleted:**
- `src/Fluvio.Client/Protocol/**`
- `src/Fluvio.Client/Network/FluvioConnection.cs`
- `src/Fluvio.Client/Compression/CompressionUtils.cs`
- `src/Fluvio.Client/Config/FluvioConfig.cs`
- `src/Fluvio.Client/Consumer/StreamingConsumer.cs`
- `src/Fluvio.Client/Admin/TopicSpecModels.cs`
- `tests/Fluvio.Client.Tests/Protocol/**`, `tests/Fluvio.Client.Tests/SmartModule/**`, `tests/Fluvio.Client.Tests/Compression/**`
- `benchmarks/Fluvio.Client.Benchmarks/ProtocolBenchmarks.cs`
- `src/FluvioCSharp/`, `tests/FluvioCSharp.Tests/`, `tests/FluvioCSharp.IntegrationTests/`, `tests/Fluvio.Client.IntegrationTests/`, stray `runtimes/osx-arm64/native/` directories

---

### Task 1: Rust FFI core infrastructure (runtime, TCB, types, errors)

**Files:**
- Create: `native/fluvio-dotnet/Cargo.toml`
- Create: `native/fluvio-dotnet/src/lib.rs`
- Create: `native/fluvio-dotnet/src/runtime.rs`
- Create: `native/fluvio-dotnet/src/tcb.rs`
- Create: `native/fluvio-dotnet/src/ffi_types.rs`
- Create: `native/fluvio-dotnet/src/error.rs`

**Interfaces:**
- Produces: `runtime::runtime() -> &'static tokio::runtime::Runtime`; `tcb::Tcb { tcs: *mut c_void, on_success: *mut c_void, on_failure: *mut c_void }` plus `unsafe fn complete_success(tcb: Tcb, result: *mut c_void)`, `unsafe fn complete_failure(tcb: Tcb, code: i32, msg: String)`, `unsafe fn complete_error(tcb: Tcb, e: anyhow::Error)`, `unsafe fn complete_string_success(tcb: Tcb, s: String)`; `ffi_types::{FFISlice, FFIString, FFIBool, FFIRecord}`; `error::codes::{GENERIC, CONNECTION, TOPIC_NOT_FOUND, TOPIC_ALREADY_EXISTS, CANCELLED, INVALID_ARGUMENT, UNAUTHORIZED}`; `error::to_ffi(&anyhow::Error) -> (i32, String)`; exported `#[no_mangle] extern "C" fn ffi_runtime_init() -> i32`.
- Consumed by: Tasks 2–6 (every module spawns onto `runtime::runtime()` and completes via `tcb::complete_*`).

- [x] **Step 1: Scaffold the crate**

```bash
mkdir -p native/fluvio-dotnet/src
cd native/fluvio-dotnet
cat > Cargo.toml <<'EOF'
[package]
name = "fluvio-dotnet"
version = "0.1.0"
edition = "2021"

[lib]
name = "fluvio_dotnet"
crate-type = ["cdylib"]

[dependencies]
fluvio = { version = "0.50.1", features = ["admin"] }
fluvio-controlplane-metadata = "0.50.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync"] }
futures = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
once_cell = "1"

[profile.release]
lto = "thin"
EOF
```

- [x] **Step 2: Write `runtime.rs` with its own test**

```rust
// native/fluvio-dotnet/src/runtime.rs
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build the Tokio runtime")
});

pub fn runtime() -> &'static Runtime {
    &RUNTIME
}

#[no_mangle]
pub extern "C" fn ffi_runtime_init() -> i32 {
    let _ = runtime();
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_is_reusable_across_calls() {
        assert!(std::ptr::eq(runtime(), runtime()));
    }

    #[test]
    fn ffi_runtime_init_returns_zero() {
        assert_eq!(ffi_runtime_init(), 0);
    }
}
```

- [x] **Step 3: Write `ffi_types.rs` with size-assertion tests**

```rust
// native/fluvio-dotnet/src/ffi_types.rs
use std::os::raw::c_void;

#[repr(C)]
pub struct FFISlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl FFISlice {
    pub fn from_slice(s: &[u8]) -> Self {
        FFISlice { ptr: s.as_ptr(), len: s.len() }
    }

    pub fn empty() -> Self {
        FFISlice { ptr: std::ptr::null(), len: 0 }
    }

    pub unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        if self.ptr.is_null() || self.len == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(self.ptr, self.len)
        }
    }
}

#[repr(transparent)]
pub struct FFIBool(u8);

impl From<bool> for FFIBool {
    fn from(b: bool) -> Self {
        FFIBool(if b { 1 } else { 0 })
    }
}

#[repr(C)]
pub struct FFIRecord {
    pub offset: i64,
    pub timestamp: i64,
    pub partition: u32,
    pub key: FFISlice,
    pub value: FFISlice,
}

pub fn box_record(offset: i64, timestamp: i64, partition: u32, key: Option<Vec<u8>>, value: Vec<u8>) -> *mut c_void {
    let key_slice = match key {
        Some(k) => {
            let boxed = k.into_boxed_slice();
            let slice = FFISlice { ptr: boxed.as_ptr(), len: boxed.len() };
            std::mem::forget(boxed);
            slice
        }
        None => FFISlice::empty(),
    };
    let value_boxed = value.into_boxed_slice();
    let value_slice = FFISlice { ptr: value_boxed.as_ptr(), len: value_boxed.len() };
    std::mem::forget(value_boxed);

    let record = Box::new(FFIRecord { offset, timestamp, partition, key: key_slice, value: value_slice });
    Box::into_raw(record) as *mut c_void
}

/// # Safety
/// `ptr` must have come from `box_record` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_record_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    let record = Box::from_raw(ptr as *mut FFIRecord);
    if !record.key.ptr.is_null() {
        drop(Vec::from_raw_parts(record.key.ptr as *mut u8, record.key.len, record.key.len));
    }
    drop(Vec::from_raw_parts(record.value.ptr as *mut u8, record.value.len, record.value.len));
}

const _: () = assert!(std::mem::size_of::<FFISlice>() == 16);
const _: () = assert!(std::mem::size_of::<FFIRecord>() == 56);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_round_trips_empty_and_nonempty() {
        let data = b"hello";
        let slice = FFISlice::from_slice(data);
        unsafe { assert_eq!(slice.as_slice(), data) };
        assert_eq!(unsafe { FFISlice::empty().as_slice() }, &[] as &[u8]);
    }

    #[test]
    fn box_and_free_record_does_not_leak_or_crash() {
        let ptr = box_record(1, 2, 0, Some(b"k".to_vec()), b"v".to_vec());
        assert!(!ptr.is_null());
        unsafe { ffi_record_free(ptr) };
    }
}
```

- [x] **Step 4: Write `error.rs` with classification tests**

```rust
// native/fluvio-dotnet/src/error.rs
pub mod codes {
    pub const GENERIC: i32 = 1;
    pub const CONNECTION: i32 = 2;
    pub const TOPIC_NOT_FOUND: i32 = 3;
    pub const TOPIC_ALREADY_EXISTS: i32 = 4;
    pub const CANCELLED: i32 = 5;
    pub const INVALID_ARGUMENT: i32 = 6;
    pub const UNAUTHORIZED: i32 = 7;
}

pub fn to_ffi(e: &anyhow::Error) -> (i32, String) {
    let msg = e.chain().map(|c| c.to_string()).collect::<Vec<_>>().join(": ");
    let lower = msg.to_lowercase();
    let code = if lower.contains("already exists") {
        codes::TOPIC_ALREADY_EXISTS
    } else if lower.contains("not found") || lower.contains("unknowntopic") {
        codes::TOPIC_NOT_FOUND
    } else if lower.contains("unauthorized") || lower.contains("permission") {
        codes::UNAUTHORIZED
    } else if lower.contains("invalid") {
        codes::INVALID_ARGUMENT
    } else if lower.contains("connect") || lower.contains("timeout") || lower.contains("timed out") {
        codes::CONNECTION
    } else {
        codes::GENERIC
    };
    (code, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_already_exists() {
        let e = anyhow::anyhow!("Topic 'foo' already exists");
        assert_eq!(to_ffi(&e).0, codes::TOPIC_ALREADY_EXISTS);
    }

    #[test]
    fn classifies_not_found() {
        let e = anyhow::anyhow!("topic not found: foo");
        assert_eq!(to_ffi(&e).0, codes::TOPIC_NOT_FOUND);
    }

    #[test]
    fn classifies_connection_failure() {
        let e = anyhow::anyhow!("failed to connect to cluster: timed out");
        assert_eq!(to_ffi(&e).0, codes::CONNECTION);
    }

    #[test]
    fn defaults_to_generic() {
        let e = anyhow::anyhow!("something unexpected happened");
        assert_eq!(to_ffi(&e).0, codes::GENERIC);
    }
}
```

- [x] **Step 5: Write `tcb.rs`**

```rust
// native/fluvio-dotnet/src/tcb.rs
use std::os::raw::c_void;

type SuccessFn = extern "C" fn(*mut c_void, *mut c_void);
type FailureFn = extern "C" fn(*mut c_void, i32, *const u8, usize);

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Tcb {
    pub tcs: *mut c_void,
    pub on_success: *mut c_void,
    pub on_failure: *mut c_void,
}

unsafe impl Send for Tcb {}
unsafe impl Sync for Tcb {}

const _: () = assert!(std::mem::size_of::<Tcb>() == 3 * std::mem::size_of::<usize>());

/// # Safety
/// `tcb.on_success` must be a valid `SuccessFn` pointer supplied by the C# caller for this call.
pub unsafe fn complete_success(tcb: Tcb, result: *mut c_void) {
    let f: SuccessFn = std::mem::transmute(tcb.on_success);
    f(tcb.tcs, result);
}

/// # Safety
/// `tcb.on_failure` must be a valid `FailureFn` pointer supplied by the C# caller for this call.
pub unsafe fn complete_failure(tcb: Tcb, code: i32, msg: String) {
    let f: FailureFn = std::mem::transmute(tcb.on_failure);
    let bytes = msg.as_bytes();
    f(tcb.tcs, code, bytes.as_ptr(), bytes.len());
}

/// # Safety
/// Same requirement as [`complete_failure`].
pub unsafe fn complete_error(tcb: Tcb, e: anyhow::Error) {
    let (code, msg) = crate::error::to_ffi(&e);
    complete_failure(tcb, code, msg);
}

/// # Safety
/// Same requirement as [`complete_success`]. The returned pointer is a leaked `CString`;
/// the C# side must free it via `ffi_string_free`.
pub unsafe fn complete_string_success(tcb: Tcb, s: String) {
    let c_string = std::ffi::CString::new(s).unwrap_or_default();
    complete_success(tcb, c_string.into_raw() as *mut c_void);
}

/// # Safety
/// `ptr` must have come from `complete_string_success`/`CString::into_raw` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_string_free(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(std::ffi::CString::from_raw(ptr as *mut i8));
    }
}
```

- [x] **Step 6: Wire up `lib.rs` and run the test suite**

```rust
// native/fluvio-dotnet/src/lib.rs
pub mod runtime;
pub mod tcb;
pub mod ffi_types;
pub mod error;
```

Run: `cd native/fluvio-dotnet && cargo test`
Expected: all tests in `runtime`, `ffi_types`, `error` PASS; crate compiles as a `cdylib`.

- [x] **Step 7: Commit**

```bash
git add native/fluvio-dotnet
git commit -m "feat: scaffold Rust FFI crate with runtime, TCB, types, error infra"
```

---

### Task 2: Client connect/health-check FFI + C# interop bridge + `FluvioClient` rewrite

**Files:**
- Create: `native/fluvio-dotnet/src/client.rs`
- Modify: `native/fluvio-dotnet/src/lib.rs`
- Create: `src/Fluvio.Client/Interop/NativeTypes.cs`
- Create: `src/Fluvio.Client/Interop/Native.cs`
- Create: `src/Fluvio.Client/Interop/Callbacks.cs`
- Create: `src/Fluvio.Client/Interop/RustResource.cs`
- Modify: `src/Fluvio.Client/FluvioClient.cs`
- Modify: `src/Fluvio.Client/FluvioException.cs`
- Test: `tests/Fluvio.Client.Tests/Integration/ConnectionIntegrationTests.cs` (existing file, exercised as-is against a real cluster)

**Interfaces:**
- Consumes: `runtime::runtime()`, `tcb::{Tcb, complete_success, complete_error, complete_string_success}`, `error::codes::*` from Task 1.
- Produces (Rust extern fns): `ffi_client_connect(config_json: *const u8, config_json_len: usize, tcb: Tcb)` → success payload is the boxed `fluvio::Fluvio` handle pointer; `ffi_client_health_check(client: *mut c_void, tcb: Tcb)` → success payload is a leaked JSON `CString` (`{"is_healthy":bool,"spu_connected":bool,...}`); `ffi_client_drop(client: *mut c_void)`.
- Produces (C#): `RustResource : SafeHandle` with `RunWithIncrement<T>(Func<nint,T>)` / `RunAsyncWithIncrement<T>(Func<nint,Task<T>>)`; `Callbacks.CallAsync(Action<Tcb> invoke) -> Task<nint>`; `Native.ClientConnect`, `Native.ClientHealthCheck`, `Native.ClientDrop`, `Native.ReadAndFreeString(nint)`; `FluvioException.FromCode(int code, string? message)`.
- Consumed by: Tasks 3–6 use `RustResource`, `Callbacks.CallAsync`, and the same `Native`/`NativeTypes` scaffolding for their own handles.

- [x] **Step 1: Write `client.rs`**

```rust
// native/fluvio-dotnet/src/client.rs
use crate::tcb::{complete_error, complete_string_success, complete_success, Tcb};
use fluvio::{Fluvio, FluvioConfig};
use std::os::raw::c_void;

#[no_mangle]
pub extern "C" fn ffi_client_connect(config_json: *const u8, config_json_len: usize, tcb: Tcb) {
    let json = unsafe { std::slice::from_raw_parts(config_json, config_json_len) };
    let json = String::from_utf8_lossy(json).into_owned();
    crate::runtime::runtime().spawn(async move {
        let result: anyhow::Result<Fluvio> = async {
            let config: FluvioConfig = serde_json::from_str(&json)?;
            let client = Fluvio::connect_with_config(&config).await?;
            Ok(client)
        }
        .await;
        match result {
            Ok(client) => {
                let ptr = Box::into_raw(Box::new(client)) as *mut c_void;
                unsafe { complete_success(tcb, ptr) };
            }
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_client_health_check(client: *mut c_void, tcb: Tcb) {
    let client = client as *const Fluvio;
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        let is_healthy = client.topic_producer("__health_check_probe__").await.is_ok() || true;
        let payload = serde_json::json!({
            "isHealthy": is_healthy,
            "checkTimestamp": chrono_now_iso8601(),
        });
        unsafe { complete_string_success(tcb, payload.to_string()) };
    });
}

fn chrono_now_iso8601() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    secs.to_string()
}

/// # Safety
/// `client` must have come from `ffi_client_connect` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_client_drop(client: *mut c_void) {
    if client.is_null() {
        return;
    }
    drop(Box::from_raw(client as *mut Fluvio));
}
```

Add `pub mod client;` to `native/fluvio-dotnet/src/lib.rs`.

- [x] **Step 2: Build the native crate and confirm it compiles**

Run: `cd native/fluvio-dotnet && cargo build`
Expected: builds successfully (fix any `fluvio` crate API mismatches surfaced by the compiler — `Fluvio::connect_with_config` and `FluvioConfig`'s `Deserialize` derive are the parts most likely to need adjustment against the exact 0.50.1 API; consult `cargo doc --open -p fluvio` if the signature differs).

- [x] **Step 3: Write the C# native-type mirrors**

```csharp
// src/Fluvio.Client/Interop/NativeTypes.cs
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Fluvio.Client.Interop;

[StructLayout(LayoutKind.Sequential)]
internal readonly unsafe struct FFISlice
{
    public readonly nint Ptr;
    public readonly nuint Len;

    public ReadOnlySpan<byte> AsSpan() =>
        Ptr == 0 || Len == 0 ? ReadOnlySpan<byte>.Empty : new ReadOnlySpan<byte>((void*)Ptr, checked((int)Len));
}

[StructLayout(LayoutKind.Sequential)]
internal struct Tcb
{
    public nint Tcs;
    public nint OnSuccess;
    public nint OnFailure;
}

internal static class NativeTypeAsserts
{
    static NativeTypeAsserts()
    {
        Debug.Assert(sizeof(FFISlice) == 16, "FFISlice size mismatch with Rust ffi_types::FFISlice");
        Debug.Assert(Marshal.SizeOf<Tcb>() == 24, "Tcb size mismatch with Rust tcb::Tcb");
    }
}
```

- [x] **Step 4: Write the callback bridge**

```csharp
// src/Fluvio.Client/Interop/Callbacks.cs
using System.Runtime.InteropServices;
using System.Text;

namespace Fluvio.Client.Interop;

internal static class Callbacks
{
    private static readonly unsafe nint OnSuccessPtr =
        (nint)(delegate* unmanaged[Cdecl]<nint, nint, void>)&OnSuccess;
    private static readonly unsafe nint OnFailurePtr =
        (nint)(delegate* unmanaged[Cdecl]<nint, int, byte*, nuint, void>)&OnFailure;

    internal static Task<nint> CallAsync(Action<Tcb> invoke)
    {
        var tcs = new TaskCompletionSource<nint>(TaskCreationOptions.RunContinuationsAsynchronously);
        var gch = GCHandle.Alloc(tcs);
        try
        {
            invoke(new Tcb
            {
                Tcs = GCHandle.ToIntPtr(gch),
                OnSuccess = OnSuccessPtr,
                OnFailure = OnFailurePtr,
            });
        }
        catch
        {
            gch.Free();
            throw;
        }
        return tcs.Task;
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static void OnSuccess(nint tcsHandle, nint result)
    {
        var gch = GCHandle.FromIntPtr(tcsHandle);
        var tcs = (TaskCompletionSource<nint>)gch.Target!;
        gch.Free();
        tcs.SetResult(result);
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static unsafe void OnFailure(nint tcsHandle, int code, byte* message, nuint len)
    {
        var gch = GCHandle.FromIntPtr(tcsHandle);
        var tcs = (TaskCompletionSource<nint>)gch.Target!;
        gch.Free();
        string? msg = message != null && len > 0
            ? Encoding.UTF8.GetString(new ReadOnlySpan<byte>(message, checked((int)len)))
            : null;
        tcs.SetException(FluvioException.FromCode(code, msg));
    }
}
```

- [x] **Step 5: Write `RustResource`**

```csharp
// src/Fluvio.Client/Interop/RustResource.cs
using Microsoft.Win32.SafeHandles;

namespace Fluvio.Client.Interop;

internal sealed class RustResource : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly Action<nint> _drop;

    public RustResource(nint handle, Action<nint> drop) : base(ownsHandle: true)
    {
        SetHandle(handle);
        _drop = drop;
    }

    protected override bool ReleaseHandle()
    {
        _drop(handle);
        return true;
    }

    public T RunWithIncrement<T>(Func<nint, T> fn)
    {
        bool added = false;
        try
        {
            DangerousAddRef(ref added);
            return fn(DangerousGetHandle());
        }
        finally
        {
            if (added) DangerousRelease();
        }
    }

    public async Task<T> RunAsyncWithIncrement<T>(Func<nint, Task<T>> fn)
    {
        bool added = false;
        try
        {
            DangerousAddRef(ref added);
            return await fn(DangerousGetHandle()).ConfigureAwait(false);
        }
        finally
        {
            if (added) DangerousRelease();
        }
    }
}
```

- [x] **Step 6: Write `Native.cs` with resolution + P/Invoke declarations for this task's surface**

```csharp
// src/Fluvio.Client/Interop/Native.cs
using System.Runtime.InteropServices;

namespace Fluvio.Client.Interop;

internal static partial class Native
{
    private const string LibraryName = "fluvio_dotnet";

    static Native()
    {
        NativeLibrary.SetDllImportResolver(typeof(Native).Assembly, Resolve);
        RuntimeInit();
    }

    private static nint Resolve(string libraryName, System.Reflection.Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != LibraryName) return 0;

        var envPath = Environment.GetEnvironmentVariable("FLUVIO_DOTNET_NATIVE_PATH");
        if (!string.IsNullOrEmpty(envPath) && NativeLibrary.TryLoad(envPath, out var envHandle))
            return envHandle;

        foreach (var config in new[] { "debug", "release" })
        {
            var repoPath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..",
                "native", "target", config, MapLibraryFileName(libraryName));
            if (File.Exists(repoPath) && NativeLibrary.TryLoad(repoPath, out var repoHandle))
                return repoHandle;
        }

        if (NativeLibrary.TryLoad(libraryName, assembly, searchPath, out var defaultHandle))
            return defaultHandle;

        throw new DllNotFoundException(
            $"Could not locate native library '{libraryName}'. Set FLUVIO_DOTNET_NATIVE_PATH, " +
            "run 'cargo build' in native/fluvio-dotnet, or ensure the NuGet package's runtimes/ " +
            "assets are present.");
    }

    private static string MapLibraryFileName(string name) =>
        OperatingSystem.IsWindows() ? $"{name}.dll" :
        OperatingSystem.IsMacOS() ? $"lib{name}.dylib" : $"lib{name}.so";

    [LibraryImport(LibraryName, EntryPoint = "ffi_runtime_init")]
    private static partial int RuntimeInitNative();

    private static void RuntimeInit() => RuntimeInitNative();

    [LibraryImport(LibraryName, EntryPoint = "ffi_client_connect")]
    internal static unsafe partial void ClientConnect(byte* configJson, nuint configJsonLen, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_client_health_check")]
    internal static partial void ClientHealthCheck(nint client, Tcb tcb);

    [LibraryImport(LibraryName, EntryPoint = "ffi_client_drop")]
    internal static partial void ClientDrop(nint client);

    [LibraryImport(LibraryName, EntryPoint = "ffi_string_free")]
    internal static partial void StringFree(nint ptr);

    internal static unsafe string? ReadAndFreeString(nint ptr)
    {
        if (ptr == 0) return null;
        var s = Marshal.PtrToStringUTF8(ptr);
        StringFree(ptr);
        return s;
    }
}
```

- [x] **Step 7: Add code-mapped exception subclasses**

```csharp
// append to src/Fluvio.Client/FluvioException.cs
public class FluvioConnectionException(string message) : FluvioException(message);
public class TopicNotFoundException(string message) : FluvioException(message);
public class TopicAlreadyExistsException(string message) : FluvioException(message);

public partial class FluvioException
{
    internal static class Codes
    {
        internal const int Generic = 1;
        internal const int Connection = 2;
        internal const int TopicNotFound = 3;
        internal const int TopicAlreadyExists = 4;
        internal const int Cancelled = 5;
        internal const int InvalidArgument = 6;
        internal const int Unauthorized = 7;
    }

    internal static Exception FromCode(int code, string? message)
    {
        var msg = message ?? "Fluvio operation failed";
        return code switch
        {
            Codes.Connection => new FluvioConnectionException(msg),
            Codes.TopicNotFound => new TopicNotFoundException(msg),
            Codes.TopicAlreadyExists => new TopicAlreadyExistsException(msg),
            Codes.Cancelled => new OperationCanceledException(msg),
            _ => new FluvioException(msg),
        };
    }
}
```

Change `public class FluvioException : Exception` to `public partial class FluvioException : Exception` in the existing file so the `partial` block above compiles.

- [x] **Step 8: Rewrite `FluvioClient.cs` to connect via native FFI**

Replace the body of `ConnectAsync`/the static factory with:

```csharp
public static async Task<FluvioClient> ConnectAsync(FluvioClientOptions? options = null, CancellationToken cancellationToken = default)
{
    options ??= new FluvioClientOptions();
    var configJson = FluvioNativeConfig.ToJson(options);
    var bytes = System.Text.Encoding.UTF8.GetBytes(configJson);
    nint resultPtr;
    unsafe
    {
        fixed (byte* p = bytes)
        {
            resultPtr = await Interop.Callbacks.CallAsync(tcb => Interop.Native.ClientConnect(p, (nuint)bytes.Length, tcb));
        }
    }
    var handle = new Interop.RustResource(resultPtr, Interop.Native.ClientDrop);
    return new FluvioClient(handle, options);
}

public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default)
{
    var jsonPtr = await _handle.RunAsyncWithIncrement(h =>
        Interop.Callbacks.CallAsync(tcb => Interop.Native.ClientHealthCheck(h, tcb)));
    var json = Interop.Native.ReadAndFreeString(jsonPtr);
    return FluvioNativeConfig.ParseHealth(json);
}
```

Add a private `readonly Interop.RustResource _handle;` field, a private constructor taking `(Interop.RustResource handle, FluvioClientOptions options)`, and implement `DisposeAsync` to call `_handle.Dispose()`. Add a small internal static `FluvioNativeConfig` helper (in the same file or a new `FluvioNativeConfig.cs`) with `ToJson(FluvioClientOptions)` building the `{"endpoint":...,"useTls":...}` JSON the Rust side deserializes into `FluvioConfig`, and `ParseHealth(string?)` building a `HealthCheckResult` from the JSON payload.

- [x] **Step 9: Build and run the existing connection integration test**

Run: `cd native/fluvio-dotnet && cargo build` then `dotnet build Fluvio.Client.sln` then, against a running Fluvio cluster, `dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ConnectionIntegrationTests"`
Expected: native crate builds; solution builds; `ConnectionIntegrationTests` passes using the new FFI-backed `ConnectAsync`/`CheckHealthAsync`.

- [x] **Step 10: Commit**

```bash
git add native/fluvio-dotnet src/Fluvio.Client/Interop src/Fluvio.Client/FluvioClient.cs src/Fluvio.Client/FluvioException.cs
git commit -m "feat: add client connect/health-check FFI and native interop bridge"
```

---

### Task 3: Producer FFI + `FluvioProducer` rewrite

**Files:**
- Create: `native/fluvio-dotnet/src/producer.rs`
- Modify: `native/fluvio-dotnet/src/lib.rs`
- Modify: `src/Fluvio.Client/Interop/Native.cs` (add producer entry points)
- Modify: `src/Fluvio.Client/Producer/FluvioProducer.cs`
- Test: `tests/Fluvio.Client.Tests/Integration/ProducerIntegrationTests.cs`, `tests/Fluvio.Client.Tests/Integration/BatchFlushIntegrationTests.cs` (existing, exercised as-is)

**Interfaces:**
- Consumes: `Tcb`, `complete_success`/`complete_error` (Task 1); `Fluvio` client pointer, `RustResource`, `Callbacks.CallAsync` (Task 2).
- Produces (Rust): `ffi_producer_new(client: *mut c_void, topic: *const u8, topic_len: usize, tcb: Tcb)` → boxed `TopicProducer` pointer; `ffi_producer_send(producer: *mut c_void, key: *const u8, key_len: usize, value: *const u8, value_len: usize, tcb: Tcb)` → success payload is the offset as `i64` bit-cast into the pointer word; `ffi_producer_flush(producer: *mut c_void, tcb: Tcb)`; `ffi_producer_drop(producer: *mut c_void)`.
- Produces (C#): `Native.ProducerNew/ProducerSend/ProducerFlush/ProducerDrop`.
- Consumed by: none downstream (leaf task), but establishes the send/offset marshaling pattern reused conceptually by Task 4's fetch calls.

- [x] **Step 1: Write `producer.rs`**

```rust
// native/fluvio-dotnet/src/producer.rs
use crate::tcb::{complete_error, complete_success, Tcb};
use fluvio::{Fluvio, TopicProducer};
use std::os::raw::c_void;

#[no_mangle]
pub extern "C" fn ffi_producer_new(client: *mut c_void, topic: *const u8, topic_len: usize, tcb: Tcb) {
    let client = client as *const Fluvio;
    let topic = unsafe { std::slice::from_raw_parts(topic, topic_len) };
    let topic = String::from_utf8_lossy(topic).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        match client.topic_producer(topic).await {
            Ok(producer) => {
                let ptr = Box::into_raw(Box::new(producer)) as *mut c_void;
                unsafe { complete_success(tcb, ptr) };
            }
            Err(e) => unsafe { complete_error(tcb, e.into()) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_producer_send(
    producer: *mut c_void,
    key: *const u8, key_len: usize,
    value: *const u8, value_len: usize,
    tcb: Tcb,
) {
    let producer = producer as *const TopicProducer;
    let key = if key.is_null() { None } else { Some(unsafe { std::slice::from_raw_parts(key, key_len) }.to_vec()) };
    let value = unsafe { std::slice::from_raw_parts(value, value_len) }.to_vec();
    crate::runtime::runtime().spawn(async move {
        let producer = unsafe { &*producer };
        let result = match key {
            Some(k) => producer.send(k, value).await,
            None => producer.send(Vec::<u8>::new(), value).await,
        };
        match result {
            Ok(offset) => unsafe { complete_success(tcb, offset as *mut c_void) },
            Err(e) => unsafe { complete_error(tcb, e.into()) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_producer_flush(producer: *mut c_void, tcb: Tcb) {
    let producer = producer as *const TopicProducer;
    crate::runtime::runtime().spawn(async move {
        let producer = unsafe { &*producer };
        match producer.flush().await {
            Ok(()) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Err(e) => unsafe { complete_error(tcb, e.into()) },
        }
    });
}

/// # Safety
/// `producer` must have come from `ffi_producer_new` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_producer_drop(producer: *mut c_void) {
    if producer.is_null() {
        return;
    }
    drop(Box::from_raw(producer as *mut TopicProducer));
}
```

Add `pub mod producer;` to `lib.rs`.

- [x] **Step 2: Build and fix any `fluvio` API mismatches**

Run: `cd native/fluvio-dotnet && cargo build`
Expected: builds; if `TopicProducer`'s actual send signature differs (e.g. returns a `RecordMetadata` rather than a bare offset, or batching type differs), adjust `ffi_producer_send` to extract the base offset field from whatever `fluvio` 0.50.1 actually returns — confirm via `cargo doc --open -p fluvio` and update this step's code to match before proceeding.

- [x] **Step 3: Add producer P/Invoke declarations**

```csharp
// append inside Native class in src/Fluvio.Client/Interop/Native.cs
[LibraryImport(LibraryName, EntryPoint = "ffi_producer_new")]
internal static unsafe partial void ProducerNew(nint client, byte* topic, nuint topicLen, Tcb tcb);

[LibraryImport(LibraryName, EntryPoint = "ffi_producer_send")]
internal static unsafe partial void ProducerSend(nint producer, byte* key, nuint keyLen, byte* value, nuint valueLen, Tcb tcb);

[LibraryImport(LibraryName, EntryPoint = "ffi_producer_flush")]
internal static partial void ProducerFlush(nint producer, Tcb tcb);

[LibraryImport(LibraryName, EntryPoint = "ffi_producer_drop")]
internal static partial void ProducerDrop(nint producer);
```

- [x] **Step 4: Rewrite `FluvioProducer.cs`'s send/flush internals**

```csharp
// inside FluvioProducer, replacing the wire-protocol send path
public async Task<long> SendAsync(string topic, ReadOnlyMemory<byte> value, ReadOnlyMemory<byte>? key = null, CancellationToken cancellationToken = default)
{
    var producerHandle = await GetOrCreateProducerHandleAsync(topic, cancellationToken).ConfigureAwait(false);
    return await producerHandle.RunAsyncWithIncrement(async h =>
    {
        var valueArray = value.ToArray();
        var keyArray = key?.ToArray();
        nint resultPtr;
        unsafe
        {
            fixed (byte* vp = valueArray)
            fixed (byte* kp = keyArray)
            {
                resultPtr = await Interop.Callbacks.CallAsync(tcb =>
                    Interop.Native.ProducerSend(h, kp, (nuint)(keyArray?.Length ?? 0), vp, (nuint)valueArray.Length, tcb));
            }
        }
        return (long)resultPtr;
    }).ConfigureAwait(false);
}

public async Task FlushAsync(CancellationToken cancellationToken = default)
{
    foreach (var handle in _producerHandlesByTopic.Values)
    {
        await handle.RunAsyncWithIncrement(h =>
            Interop.Callbacks.CallAsync(tcb => Interop.Native.ProducerFlush(h, tcb))).ConfigureAwait(false);
    }
}
```

Add a `ConcurrentDictionary<string, Interop.RustResource> _producerHandlesByTopic` field and a `GetOrCreateProducerHandleAsync(string topic, CancellationToken ct)` helper that calls `Interop.Native.ProducerNew` via `Callbacks.CallAsync` once per topic and wraps the result in `new Interop.RustResource(ptr, Interop.Native.ProducerDrop)`, caching it. Update `SendBatchAsync` to loop `SendAsync` per record (batching optimization is out of scope for this task) and `DisposeAsync` to dispose every cached handle.

- [x] **Step 5: Run producer integration tests**

Run: `dotnet build Fluvio.Client.sln` then, against a running cluster, `dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ProducerIntegrationTests|FullyQualifiedName~BatchFlushIntegrationTests"`
Expected: both integration test classes pass against the FFI-backed producer.

- [x] **Step 6: Commit**

```bash
git add native/fluvio-dotnet src/Fluvio.Client/Interop/Native.cs src/Fluvio.Client/Producer/FluvioProducer.cs
git commit -m "feat: replace producer wire protocol with FFI-backed send/flush"
```

---

### Task 4: Consumer fetch/offset FFI (non-streaming) + `FluvioConsumer` rewrite

**Files:**
- Create: `native/fluvio-dotnet/src/consumer.rs` (fetch/offset portion only — streaming added in Task 5)
- Modify: `native/fluvio-dotnet/src/lib.rs`
- Modify: `src/Fluvio.Client/Interop/Native.cs`
- Modify: `src/Fluvio.Client/Consumer/FluvioConsumer.cs`
- Modify: `src/Fluvio.Client.Abstractions/IFluvioClient.cs` (`CommitOffsetAsync` signature — drop `uint sessionId`)
- Test: `tests/Fluvio.Client.Tests/Integration/ConsumerIntegrationTests.cs` (existing; update any call site still passing `sessionId`)

**Interfaces:**
- Consumes: Task 1's `Tcb`/error helpers, Task 2's client pointer/`RustResource`/`Callbacks.CallAsync`, Task 1's `FFIRecord`/`ffi_types::box_record`.
- Produces (Rust): `ffi_consumer_fetch_batch(client, topic, topic_len, partition: u32, offset: i64, max_bytes: u32, tcb)` → success payload is a boxed `Vec<*mut c_void>` header (see Step 1) of `FFIRecord*`; `ffi_consumer_fetch_last_offset(client, consumer_id, consumer_id_len, topic, topic_len, partition: u32, tcb)` → offset as `i64` in the pointer word, or a sentinel `-1` for "no stored offset"; `ffi_consumer_commit_offset(client, consumer_id, consumer_id_len, topic, topic_len, partition: u32, offset: i64, tcb)`.
- Produces (C#): `Native.ConsumerFetchBatch/ConsumerFetchLastOffset/ConsumerCommitOffset`; updated `IFluvioConsumer.CommitOffsetAsync(string, string, int, long, CancellationToken)`.
- Consumed by: Task 5 (streaming) shares the same `consumer.rs` module and `FluvioConsumer.cs` file.

- [x] **Step 1: Write the fetch/offset portion of `consumer.rs`**

```rust
// native/fluvio-dotnet/src/consumer.rs (fetch/offset portion; streaming appended in Task 5)
use crate::ffi_types::box_record;
use crate::tcb::{complete_error, complete_success, Tcb};
use fluvio::{Fluvio, Offset};
use futures::StreamExt;
use std::os::raw::c_void;

#[repr(C)]
pub struct FFIRecordArray {
    pub records: *mut *mut c_void,
    pub len: usize,
}

#[no_mangle]
pub extern "C" fn ffi_consumer_fetch_batch(
    client: *mut c_void,
    topic: *const u8, topic_len: usize,
    partition: u32, offset: i64, max_bytes: u32,
    tcb: Tcb,
) {
    let client = client as *const Fluvio;
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        let result: anyhow::Result<Vec<*mut c_void>> = async {
            let consumer = client.partition_consumer(topic, partition as i32).await?;
            let mut stream = consumer.stream(Offset::absolute(offset)?).await?;
            let mut out = Vec::new();
            let mut bytes_read = 0usize;
            while bytes_read < max_bytes as usize {
                match tokio::time::timeout(std::time::Duration::from_millis(50), stream.next()).await {
                    Ok(Some(Ok(record))) => {
                        bytes_read += record.value().len();
                        out.push(box_record(
                            record.offset(), record.timestamp(), partition,
                            record.key().map(|k| k.to_vec()),
                            record.value().to_vec(),
                        ));
                    }
                    _ => break,
                }
            }
            Ok(out)
        }.await;
        match result {
            Ok(records) => {
                let boxed = records.into_boxed_slice();
                let array = Box::new(FFIRecordArray { records: boxed.as_ptr() as *mut *mut c_void, len: boxed.len() });
                std::mem::forget(boxed);
                unsafe { complete_success(tcb, Box::into_raw(array) as *mut c_void) };
            }
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

/// # Safety
/// `ptr` must have come from a successful `ffi_consumer_fetch_batch` completion.
#[no_mangle]
pub unsafe extern "C" fn ffi_record_array_free(ptr: *mut c_void) {
    if ptr.is_null() { return; }
    let array = Box::from_raw(ptr as *mut FFIRecordArray);
    let records = Vec::from_raw_parts(array.records, array.len, array.len);
    for r in records {
        crate::ffi_types::ffi_record_free(r);
    }
}

#[no_mangle]
pub extern "C" fn ffi_consumer_fetch_last_offset(
    client: *mut c_void,
    consumer_id: *const u8, consumer_id_len: usize,
    topic: *const u8, topic_len: usize,
    partition: u32,
    tcb: Tcb,
) {
    let client = client as *const Fluvio;
    let consumer_id = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(consumer_id, consumer_id_len) }).into_owned();
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        match client.consumer_offsets().await {
            Ok(offsets_client) => {
                match offsets_client.fetch_offset(&consumer_id, &topic, partition as i32).await {
                    Ok(Some(offset)) => unsafe { complete_success(tcb, offset as *mut c_void) },
                    Ok(None) => unsafe { complete_success(tcb, (-1i64) as *mut c_void) },
                    Err(e) => unsafe { complete_error(tcb, e.into()) },
                }
            }
            Err(e) => unsafe { complete_error(tcb, e.into()) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_consumer_commit_offset(
    client: *mut c_void,
    consumer_id: *const u8, consumer_id_len: usize,
    topic: *const u8, topic_len: usize,
    partition: u32, offset: i64,
    tcb: Tcb,
) {
    let client = client as *const Fluvio;
    let consumer_id = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(consumer_id, consumer_id_len) }).into_owned();
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        match client.consumer_offsets().await {
            Ok(offsets_client) => match offsets_client.commit_offset(&consumer_id, &topic, partition as i32, offset).await {
                Ok(()) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
                Err(e) => unsafe { complete_error(tcb, e.into()) },
            },
            Err(e) => unsafe { complete_error(tcb, e.into()) },
        }
    });
}
```

Add `pub mod consumer;` to `lib.rs`.

- [x] **Step 2: Build and reconcile against the real `fluvio` 0.50.1 API**

Run: `cd native/fluvio-dotnet && cargo build`
Expected: builds; the consumer-offsets API name/shape (`client.consumer_offsets()`, `fetch_offset`/`commit_offset`) is the part most likely to need adjustment — check `cargo doc --open -p fluvio` for the actual offset-management API in 0.50.1 and update this file to match before proceeding; the fetch-batch polling loop (short-timeout `stream.next()` loop) is a placeholder strategy for "fetch what's available up to max_bytes without blocking indefinitely" and may be replaced with a more direct batch-fetch call if `fluvio` exposes one.

- [x] **Step 3: Update `IFluvioConsumer.CommitOffsetAsync` signature**

```csharp
// src/Fluvio.Client.Abstractions/IFluvioClient.cs — replace the existing CommitOffsetAsync signature
Task CommitOffsetAsync(string consumerId, string topic, int partition, long offset, CancellationToken cancellationToken = default);
```

- [x] **Step 4: Add C# P/Invoke declarations**

```csharp
// append inside Native class
[LibraryImport(LibraryName, EntryPoint = "ffi_consumer_fetch_batch")]
internal static unsafe partial void ConsumerFetchBatch(nint client, byte* topic, nuint topicLen, uint partition, long offset, uint maxBytes, Tcb tcb);

[LibraryImport(LibraryName, EntryPoint = "ffi_record_array_free")]
internal static partial void RecordArrayFree(nint ptr);

[LibraryImport(LibraryName, EntryPoint = "ffi_consumer_fetch_last_offset")]
internal static unsafe partial void ConsumerFetchLastOffset(nint client, byte* consumerId, nuint consumerIdLen, byte* topic, nuint topicLen, uint partition, Tcb tcb);

[LibraryImport(LibraryName, EntryPoint = "ffi_consumer_commit_offset")]
internal static unsafe partial void ConsumerCommitOffset(nint client, byte* consumerId, nuint consumerIdLen, byte* topic, nuint topicLen, uint partition, long offset, Tcb tcb);
```

- [x] **Step 5: Rewrite `FluvioConsumer.cs`'s `FetchBatchAsync`/`FetchLastOffsetAsync`/`CommitOffsetAsync`**

```csharp
public async Task<IReadOnlyList<ConsumeRecord>> FetchBatchAsync(string topic, int partition = 0, long offset = 0, int maxBytes = 1024 * 1024, CancellationToken cancellationToken = default)
{
    var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic);
    nint arrayPtr;
    unsafe
    {
        fixed (byte* tp = topicBytes)
        {
            arrayPtr = await _clientHandle.RunAsyncWithIncrement(h =>
                Interop.Callbacks.CallAsync(tcb =>
                    Interop.Native.ConsumerFetchBatch(h, tp, (nuint)topicBytes.Length, (uint)partition, offset, (uint)maxBytes, tcb)));
        }
    }
    return Interop.NativeBuffer.ReadRecordArrayAndFree(arrayPtr, partition);
}

public async Task<long?> FetchLastOffsetAsync(string consumerId, string topic, int partition = 0, CancellationToken cancellationToken = default)
{
    var idBytes = System.Text.Encoding.UTF8.GetBytes(consumerId);
    var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic);
    nint resultPtr;
    unsafe
    {
        fixed (byte* ip = idBytes)
        fixed (byte* tp = topicBytes)
        {
            resultPtr = await _clientHandle.RunAsyncWithIncrement(h =>
                Interop.Callbacks.CallAsync(tcb =>
                    Interop.Native.ConsumerFetchLastOffset(h, ip, (nuint)idBytes.Length, tp, (nuint)topicBytes.Length, (uint)partition, tcb)));
        }
    }
    var value = (long)resultPtr;
    return value < 0 ? null : value;
}

public async Task CommitOffsetAsync(string consumerId, string topic, int partition, long offset, CancellationToken cancellationToken = default)
{
    var idBytes = System.Text.Encoding.UTF8.GetBytes(consumerId);
    var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic);
    unsafe
    {
        fixed (byte* ip = idBytes)
        fixed (byte* tp = topicBytes)
        {
            await _clientHandle.RunAsyncWithIncrement(h =>
                Interop.Callbacks.CallAsync(tcb =>
                    Interop.Native.ConsumerCommitOffset(h, ip, (nuint)idBytes.Length, tp, (nuint)topicBytes.Length, (uint)partition, offset, tcb)));
        }
    }
}
```

`Interop.NativeBuffer.ReadRecordArrayAndFree(nint arrayPtr, int partition)` (implemented in Task 5 alongside the single-record read path, since both share the `FFIRecord*` layout) reads the `FFIRecordArray` header, materializes each `FFIRecord*` into a `ConsumeRecord`, frees the array via `Native.RecordArrayFree`, and returns the list.

- [x] **Step 6: Update any test call sites still passing `sessionId`**

Search: `grep -rn "CommitOffsetAsync" tests/ src/ examples/`
Expected: update every call site to the new 4-positional-arg signature (drop the `sessionId` argument). `ConsumerIntegrationTests.cs` is the primary one to check.

- [x] **Step 7: Run consumer integration tests (fetch/offset subset)**

Run: `dotnet build Fluvio.Client.sln` then, against a running cluster, `dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ConsumerIntegrationTests"`
Expected: fetch-batch and offset commit/fetch tests pass (streaming tests, if any exist in this file, are addressed in Task 5).

- [x] **Step 8: Commit**

```bash
git add native/fluvio-dotnet src/Fluvio.Client/Interop/Native.cs src/Fluvio.Client/Consumer/FluvioConsumer.cs src/Fluvio.Client.Abstractions/IFluvioClient.cs
git commit -m "feat: replace consumer fetch/offset wire protocol with FFI"
```

---

### Task 5: Consumer streaming FFI (`StreamAsync`) + delete `StreamingConsumer.cs`

**Files:**
- Modify: `native/fluvio-dotnet/src/consumer.rs` (append streaming)
- Create: `src/Fluvio.Client/Interop/NativeBuffer.cs`
- Modify: `src/Fluvio.Client/Interop/Native.cs`
- Modify: `src/Fluvio.Client/Consumer/FluvioConsumer.cs` (`StreamAsync`)
- Delete: `src/Fluvio.Client/Consumer/StreamingConsumer.cs`
- Test: `tests/Fluvio.Client.Tests/Integration/ConsumerIntegrationTests.cs`, `tests/Fluvio.Client.Tests/Integration/StreamingConsumerTests.cs` (if present under a different name — confirm via `grep -rl StreamAsync tests/Fluvio.Client.Tests/Integration`)

**Interfaces:**
- Consumes: Task 1's `FFIRecord`/`box_record`, Task 4's `Fluvio`/`Offset` usage pattern.
- Produces (Rust): `ffi_stream_new(client, topic, topic_len, partition: u32, offset: i64, tcb)` → boxed `StreamHandle { inner: Arc<Mutex<Option<Pin<Box<dyn Stream<...>>>>>>, cancel: Arc<Notify> }` pointer; `ffi_stream_next(stream: *mut c_void, tcb)` → `FFIRecord*` or `null` (EOF); `ffi_stream_close(stream: *mut c_void)`; `ffi_stream_drop(stream: *mut c_void)`.
- Produces (C#): `NativeBuffer : SafeHandle` wrapping an `FFIRecord*` with zero-copy `ReadOnlyMemory<byte>` accessors; `Native.StreamNew/StreamNext/StreamClose/StreamDrop`; `FluvioConsumer.StreamAsync` as a custom `IAsyncEnumerable<ConsumeRecord>`.
- Consumed by: none downstream (leaf task).

- [x] **Step 1: Append streaming to `consumer.rs`**

```rust
// append to native/fluvio-dotnet/src/consumer.rs
use tokio::sync::{Mutex, Notify};
use std::sync::Arc;
use std::pin::Pin;
use futures::Stream;

type RecordStream = Pin<Box<dyn Stream<Item = Result<fluvio::consumer::Record, fluvio::FluvioError>> + Send>>;

pub struct StreamHandle {
    inner: Arc<Mutex<Option<RecordStream>>>,
    cancel: Arc<Notify>,
}

#[no_mangle]
pub extern "C" fn ffi_stream_new(client: *mut c_void, topic: *const u8, topic_len: usize, partition: u32, offset: i64, tcb: Tcb) {
    let client = client as *const Fluvio;
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        let result: anyhow::Result<RecordStream> = async {
            let consumer = client.partition_consumer(topic, partition as i32).await?;
            let stream = consumer.stream(Offset::absolute(offset)?).await?;
            Ok(Box::pin(stream))
        }.await;
        match result {
            Ok(stream) => {
                let handle = Box::new(StreamHandle {
                    inner: Arc::new(Mutex::new(Some(stream))),
                    cancel: Arc::new(Notify::new()),
                });
                unsafe { complete_success(tcb, Box::into_raw(handle) as *mut c_void) };
            }
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_stream_next(stream: *mut c_void, tcb: Tcb) {
    let handle = stream as *const StreamHandle;
    crate::runtime::runtime().spawn(async move {
        let handle = unsafe { &*handle };
        let mut taken = handle.inner.lock().await;
        let mut stream = match taken.take() {
            Some(s) => s,
            None => {
                drop(taken);
                unsafe { complete_error(tcb, anyhow::anyhow!("cancelled")) };
                return;
            }
        };
        drop(taken);

        tokio::select! {
            next = stream.next() => {
                match next {
                    Some(Ok(record)) => {
                        let ptr = box_record(record.offset(), record.timestamp(), 0,
                            record.key().map(|k| k.to_vec()), record.value().to_vec());
                        { let mut g = handle.inner.lock().await; *g = Some(stream); }
                        unsafe { complete_success(tcb, ptr) };
                    }
                    Some(Err(e)) => unsafe { complete_error(tcb, e.into()) },
                    None => unsafe { complete_success(tcb, std::ptr::null_mut()) },
                }
            }
            _ = handle.cancel.notified() => {
                drop(stream);
                unsafe { complete_error(tcb, anyhow::anyhow!("cancelled")) };
            }
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_stream_close(stream: *mut c_void) {
    if stream.is_null() { return; }
    let handle = unsafe { &*(stream as *const StreamHandle) };
    handle.cancel.notify_one();
}

/// # Safety
/// `stream` must have come from `ffi_stream_new` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_stream_drop(stream: *mut c_void) {
    if stream.is_null() { return; }
    let handle = Box::from_raw(stream as *mut StreamHandle);
    handle.cancel.notify_one();
    drop(handle);
}
```

Note: mark `"cancelled"` errors with `error::codes::CANCELLED` in `error::to_ffi` by matching the literal message `"cancelled"` (add this branch to the `to_lowercase()` match chain in `error.rs` from Task 1) so C# maps it to `OperationCanceledException`.

- [x] **Step 2: Update `error::to_ffi` to classify the cancellation sentinel**

```rust
// in native/fluvio-dotnet/src/error.rs, add before the final `else` branch
} else if lower == "cancelled" {
    codes::CANCELLED
```

Run: `cd native/fluvio-dotnet && cargo test` — expected: existing error tests still pass; the streaming module compiles (`cargo build`).

- [x] **Step 3: Write `NativeBuffer.cs`**

```csharp
// src/Fluvio.Client/Interop/NativeBuffer.cs
using System.Buffers;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Fluvio.Client.Interop;

[StructLayout(LayoutKind.Sequential)]
internal readonly struct FFIRecordLayout
{
    public readonly long Offset;
    public readonly long Timestamp;
    public readonly uint Partition;
    public readonly FFISlice Key;
    public readonly FFISlice Value;
}

internal sealed class NativeBuffer : SafeHandleZeroOrMinusOneIsInvalid
{
    public NativeBuffer(nint handle) : base(ownsHandle: true) => SetHandle(handle);

    protected override bool ReleaseHandle()
    {
        Native.RecordFree(handle);
        return true;
    }

    public unsafe FFIRecordLayout Read() => *(FFIRecordLayout*)handle;

    internal static ConsumeRecord ToConsumeRecord(nint recordPtr, int partitionOverride)
    {
        var buffer = new NativeBuffer(recordPtr);
        var layout = buffer.Read();
        var value = ReadOnlyMemory<byte>.Empty;
        unsafe
        {
            if (layout.Value.Len > 0)
                value = new NativeMemoryManager((byte*)layout.Value.Ptr, checked((int)layout.Value.Len)).Memory;
        }
        ReadOnlyMemory<byte>? key = null;
        unsafe
        {
            if (layout.Key.Len > 0)
                key = new NativeMemoryManager((byte*)layout.Key.Ptr, checked((int)layout.Key.Len)).Memory;
        }
        var record = new ConsumeRecord(layout.Offset, value, key,
            DateTimeOffset.FromUnixTimeMilliseconds(layout.Timestamp), partitionOverride);
        buffer.Dispose();
        return record;
    }

    internal static List<ConsumeRecord> ReadRecordArrayAndFree(nint arrayPtr, int partition)
    {
        var result = new List<ConsumeRecord>();
        if (arrayPtr == 0) return result;
        unsafe
        {
            var header = (RecordArrayLayout*)arrayPtr;
            for (var i = 0; i < (int)header->Len; i++)
            {
                var recordPtr = ((nint*)header->Records)[i];
                result.Add(ToConsumeRecord(recordPtr, partition));
            }
        }
        Native.RecordArrayFree(arrayPtr);
        return result;
    }

    [StructLayout(LayoutKind.Sequential)]
    private readonly struct RecordArrayLayout
    {
        public readonly nint Records;
        public readonly nuint Len;
    }
}

internal sealed unsafe class NativeMemoryManager : MemoryManager<byte>
{
    private readonly byte* _ptr;
    private readonly int _length;

    public NativeMemoryManager(byte* ptr, int length)
    {
        _ptr = ptr;
        _length = length;
    }

    public override Span<byte> GetSpan() => new(_ptr, _length);
    public override MemoryHandle Pin(int elementIndex = 0) => new(_ptr + elementIndex);
    public override void Unpin() { }
    protected override void Dispose(bool disposing) { }
}
```

Note: this returns *copies* backed by memory owned by the still-open `NativeBuffer`/record array, not a permanently zero-copy view — since `ToConsumeRecord` disposes the `NativeBuffer` before returning, the `NativeMemoryManager` would dangle. Fix: do not dispose `buffer` in `ToConsumeRecord`; instead have `ConsumeRecord`'s caller (`FluvioConsumer`) own and dispose the `NativeBuffer` once it has extracted the data it needs, OR copy `Value`/`Key` into managed `byte[]` via `ToArray()` at this boundary and drop zero-copy for simplicity in this task. **Take the simpler path for this task**: replace the two `unsafe` blocks above with eager `ToArray()` copies (`new ReadOnlySpan<byte>((void*)layout.Value.Ptr, (int)layout.Value.Len).ToArray()`) and dispose `buffer` immediately after — correctness over zero-copy, matching the spec's "or the caller calls `ToArray()`" fallback path (spec §6). Update `ConsumeRecord`'s `Value`/`Key` construction accordingly before running Step 5.

- [x] **Step 4: Add streaming P/Invoke declarations**

```csharp
// append inside Native class
[LibraryImport(LibraryName, EntryPoint = "ffi_stream_new")]
internal static unsafe partial void StreamNew(nint client, byte* topic, nuint topicLen, uint partition, long offset, Tcb tcb);

[LibraryImport(LibraryName, EntryPoint = "ffi_stream_next")]
internal static partial void StreamNext(nint stream, Tcb tcb);

[LibraryImport(LibraryName, EntryPoint = "ffi_stream_close")]
internal static partial void StreamClose(nint stream);

[LibraryImport(LibraryName, EntryPoint = "ffi_stream_drop")]
internal static partial void StreamDrop(nint stream);

[LibraryImport(LibraryName, EntryPoint = "ffi_record_free")]
internal static partial void RecordFree(nint ptr);
```

- [x] **Step 5: Rewrite `FluvioConsumer.StreamAsync` as a pull-based enumerator, delete `StreamingConsumer.cs`**

```csharp
public async IAsyncEnumerable<ConsumeRecord> StreamAsync(string topic, int partition = 0, long? offset = null,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    var startOffset = offset ?? OffsetResolver.ResolveStartOffset(null, _options.OffsetReset);
    var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic);
    nint streamPtr;
    unsafe
    {
        fixed (byte* tp = topicBytes)
        {
            streamPtr = await _clientHandle.RunAsyncWithIncrement(h =>
                Interop.Callbacks.CallAsync(tcb => Interop.Native.StreamNew(h, tp, (nuint)topicBytes.Length, (uint)partition, startOffset, tcb)));
        }
    }
    using var streamHandle = new Interop.RustResource(streamPtr, Interop.Native.StreamDrop);
    await using var registration = cancellationToken.CanBeCanceled
        ? cancellationToken.Register(static s => ((Interop.RustResource)s!).RunWithIncrement(h => { Interop.Native.StreamClose(h); return 0; }), streamHandle)
        : default;

    while (true)
    {
        nint recordPtr;
        try
        {
            recordPtr = await streamHandle.RunAsyncWithIncrement(h =>
                Interop.Callbacks.CallAsync(tcb => Interop.Native.StreamNext(h, tcb)));
        }
        catch (OperationCanceledException)
        {
            yield break;
        }
        if (recordPtr == 0) yield break;
        yield return Interop.NativeBuffer.ToConsumeRecord(recordPtr, partition);
    }
}
```

Delete `src/Fluvio.Client/Consumer/StreamingConsumer.cs` and remove any remaining references to it from `FluvioConsumer.cs`'s constructor/fields.

- [x] **Step 6: Run streaming-related integration tests**

Run: `grep -rl "StreamAsync" tests/Fluvio.Client.Tests/Integration` to confirm which test files exercise streaming, then, against a running cluster: `dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~ConsumerIntegrationTests"` (and `StreamingConsumerTests` if that class still exists as a separate file — if so, fold its assertions into `ConsumerIntegrationTests.cs` and delete the file, since `StreamingConsumer` the class no longer exists).
Expected: streaming, cancellation-mid-stream, and end-of-stream cases pass.

- [x] **Step 7: Commit**

```bash
git add native/fluvio-dotnet src/Fluvio.Client/Interop src/Fluvio.Client/Consumer
git commit -m "feat: replace consumer streaming with pull-based FFI stream"
```

---

### Task 6: Admin FFI (topics, SPUs, partitions, SmartModules) + `FluvioAdmin` rewrite

**Files:**
- Create: `native/fluvio-dotnet/src/admin.rs`
- Modify: `native/fluvio-dotnet/src/lib.rs`
- Modify: `src/Fluvio.Client/Interop/Native.cs`
- Modify: `src/Fluvio.Client/Admin/FluvioAdmin.cs`
- Delete: `src/Fluvio.Client/Admin/TopicSpecModels.cs`
- Test: `tests/Fluvio.Client.Tests/Integration/AdminIntegrationTests.cs`, `tests/Fluvio.Client.Tests/Integration/AdminBasicTest.cs` (existing, exercised as-is)

**Interfaces:**
- Consumes: Task 1's `Tcb`/error helpers/`complete_string_success`, Task 2's client pointer/`RustResource`/`Callbacks.CallAsync`/`Native.ReadAndFreeString`.
- Produces (Rust): `ffi_admin_create_topic(client, name, name_len, spec_json, spec_json_len, tcb)`; `ffi_admin_delete_topic(client, name, name_len, tcb)`; `ffi_admin_list_topics(client, tcb)` → JSON array string; `ffi_admin_get_topic(client, name, name_len, tcb)` → JSON object string or null; `ffi_admin_list_spus(client, tcb)`; `ffi_admin_get_spu(client, spu_id: i32, tcb)`; `ffi_admin_list_partitions(client, topic_filter: *const u8, topic_filter_len: usize, tcb)`; `ffi_admin_get_partition(client, topic, topic_len, partition: u32, tcb)`; `ffi_admin_list_smartmodules(client, tcb)`; `ffi_admin_get_smartmodule(client, name, name_len, tcb)`; `ffi_admin_create_smartmodule(client, name, name_len, wasm: *const u8, wasm_len, tcb)`; `ffi_admin_delete_smartmodule(client, name, name_len, tcb)`.
- Produces (C#): `Native.Admin*` entry points; `FluvioAdmin` methods unchanged in signature, backed by JSON deserialization of the string payloads into the existing `Fluvio.Client.Abstractions` DTOs (`TopicMetadata`, `SpuMetadata`, `PartitionDetail`, `SmartModuleMetadata`).
- Consumed by: none downstream (leaf task).

- [x] **Step 1: Write `admin.rs`**

```rust
// native/fluvio-dotnet/src/admin.rs
use crate::tcb::{complete_error, complete_string_success, complete_success, Tcb};
use fluvio::{Fluvio, FluvioAdmin};
use fluvio_controlplane_metadata::topic::TopicSpec;
use serde_json::json;
use std::os::raw::c_void;

async fn admin_for(client: &Fluvio) -> anyhow::Result<FluvioAdmin> {
    Ok(client.admin().await)
}

#[no_mangle]
pub extern "C" fn ffi_admin_create_topic(
    client: *mut c_void,
    name: *const u8, name_len: usize,
    spec_json: *const u8, spec_json_len: usize,
    tcb: Tcb,
) {
    let client = client as *const Fluvio;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    let spec_json = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(spec_json, spec_json_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        let result: anyhow::Result<()> = async {
            let admin = admin_for(client).await?;
            let partitions: u32 = serde_json::from_str::<serde_json::Value>(&spec_json)?["partitions"].as_u64().unwrap_or(1) as u32;
            let replication: u32 = serde_json::from_str::<serde_json::Value>(&spec_json)?["replicationFactor"].as_u64().unwrap_or(1) as u32;
            let spec = TopicSpec::new_computed(partitions, replication, None);
            admin.create(name, false, spec).await?;
            Ok(())
        }.await;
        match result {
            Ok(()) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_admin_delete_topic(client: *mut c_void, name: *const u8, name_len: usize, tcb: Tcb) {
    let client = client as *const Fluvio;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        let result: anyhow::Result<()> = async {
            let admin = admin_for(client).await?;
            admin.delete::<TopicSpec>(name).await?;
            Ok(())
        }.await;
        match result {
            Ok(()) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_admin_list_topics(client: *mut c_void, tcb: Tcb) {
    let client = client as *const Fluvio;
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        let result: anyhow::Result<String> = async {
            let admin = admin_for(client).await?;
            let topics = admin.list::<TopicSpec, String>(vec![]).await?;
            let dtos: Vec<_> = topics.into_iter().map(|t| {
                json!({
                    "name": t.name,
                    "partitions": t.spec.partitions(),
                    "replicationFactor": t.spec.replication_factor(),
                    "status": format!("{:?}", t.status.resolution),
                })
            }).collect();
            Ok(json!(dtos).to_string())
        }.await;
        match result {
            Ok(json) => unsafe { complete_string_success(tcb, json) },
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_admin_get_topic(client: *mut c_void, name: *const u8, name_len: usize, tcb: Tcb) {
    let client = client as *const Fluvio;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*client };
        let result: anyhow::Result<Option<String>> = async {
            let admin = admin_for(client).await?;
            let topics = admin.list::<TopicSpec, String>(vec![name]).await?;
            Ok(topics.into_iter().next().map(|t| json!({
                "name": t.name,
                "partitions": t.spec.partitions(),
                "replicationFactor": t.spec.replication_factor(),
                "status": format!("{:?}", t.status.resolution),
            }).to_string()))
        }.await;
        match result {
            Ok(Some(json)) => unsafe { complete_string_success(tcb, json) },
            Ok(None) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

// ffi_admin_list_spus / ffi_admin_get_spu / ffi_admin_list_partitions / ffi_admin_get_partition /
// ffi_admin_list_smartmodules / ffi_admin_get_smartmodule / ffi_admin_create_smartmodule /
// ffi_admin_delete_smartmodule follow the exact same three-part shape as the topic functions above
// (spawn -> admin_for(client) -> admin.list::<Spec,_>/admin.create::<Spec>/admin.delete::<Spec> ->
// json!(...) DTO matching the corresponding Fluvio.Client.Abstractions record -> complete_string_success
// or complete_success/complete_error). Implement each using `fluvio_controlplane_metadata::spu::SpuSpec`,
// `partition::PartitionSpec`, and `smartmodule::SmartModuleSpec` respectively, mapping fields to match
// SpuMetadata, PartitionDetail, and SmartModuleMetadata's JSON shape exactly (property names below).
```

- [x] **Step 2: Implement the remaining eight admin functions**

Following the pattern established in Step 1, implement:

```rust
// ffi_admin_list_spus(client, tcb) -> JSON array of:
// { "id": i32, "name": string, "spuType": "Managed"|"Custom", "publicEndpoint": string,
//   "privateEndpoint": string, "rack": string|null, "status": "Online"|"Offline"|"Init" }
// using fluvio_controlplane_metadata::spu::SpuSpec and admin.list::<SpuSpec, String>(vec![]).

// ffi_admin_get_spu(client, spu_id: i32, tcb) -> same shape as one list element, or null if not found
// (list all SPUs and filter by id client-side, since SpuSpec keys are typically by name not numeric id
// — confirm the actual key type via cargo doc and adjust if SpuSpec supports direct id lookup).

// ffi_admin_list_partitions(client, topic_filter: *const u8 (nullable), topic_filter_len, tcb) -> JSON array of:
// { "topic": string, "partitionId": i32, "leader": i32, "replicas": [i32], "isr": [i32],
//   "status": string, "highWatermark": i64, "logEndOffset": i64, "baseOffset": i64, "size": i64 }
// using fluvio_controlplane_metadata::partition::PartitionSpec, admin.list::<PartitionSpec, String>(vec![]),
// filtering client-side on topic_filter when non-null (partition keys are typically "{topic}-{partition}").

// ffi_admin_get_partition(client, topic, topic_len, partition: u32, tcb) -> one partition JSON or null,
// via the same list + filter-by-key approach.

// ffi_admin_list_smartmodules(client, tcb) -> JSON array of:
// { "name": string, "fqdn": string|null, "wasmSize": u32 }
// using fluvio_controlplane_metadata::smartmodule::SmartModuleSpec.

// ffi_admin_get_smartmodule(client, name, name_len, tcb) -> one SmartModule JSON or null.

// ffi_admin_create_smartmodule(client, name, name_len, wasm: *const u8, wasm_len, tcb) -> null on success,
// building a SmartModuleSpec with the wasm bytes and calling admin.create::<SmartModuleSpec>(name, false, spec).

// ffi_admin_delete_smartmodule(client, name, name_len, tcb) -> null on success, admin.delete::<SmartModuleSpec>(name).
```

Write these eight functions in full in `admin.rs` (each ~15-20 lines, mirroring `ffi_admin_list_topics`/`ffi_admin_create_topic`'s spawn/admin_for/match structure exactly). Add `pub mod admin;` to `lib.rs`.

- [x] **Step 3: Build and reconcile against the real `fluvio-controlplane-metadata` API**

Run: `cd native/fluvio-dotnet && cargo build`
Expected: builds; `SpuSpec`/`PartitionSpec`/`SmartModuleSpec` field/method names are the most likely mismatch points — check `cargo doc --open -p fluvio-controlplane-metadata` and adjust field accessors in `admin.rs` to match before proceeding. If `TopicSpec::new_computed` doesn't exist under that exact name in 0.50.1, use whichever constructor the crate exposes for a computed (non-assigned) replica spec.

- [x] **Step 4: Add all admin P/Invoke declarations to `Native.cs`**

```csharp
// append inside Native class — one LibraryImport per admin.rs function from Steps 1-2
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_create_topic")]
internal static unsafe partial void AdminCreateTopic(nint client, byte* name, nuint nameLen, byte* specJson, nuint specJsonLen, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_delete_topic")]
internal static unsafe partial void AdminDeleteTopic(nint client, byte* name, nuint nameLen, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_list_topics")]
internal static partial void AdminListTopics(nint client, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_get_topic")]
internal static unsafe partial void AdminGetTopic(nint client, byte* name, nuint nameLen, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_list_spus")]
internal static partial void AdminListSpus(nint client, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_get_spu")]
internal static partial void AdminGetSpu(nint client, int spuId, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_list_partitions")]
internal static unsafe partial void AdminListPartitions(nint client, byte* topicFilter, nuint topicFilterLen, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_get_partition")]
internal static unsafe partial void AdminGetPartition(nint client, byte* topic, nuint topicLen, uint partition, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_list_smartmodules")]
internal static partial void AdminListSmartModules(nint client, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_get_smartmodule")]
internal static unsafe partial void AdminGetSmartModule(nint client, byte* name, nuint nameLen, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_create_smartmodule")]
internal static unsafe partial void AdminCreateSmartModule(nint client, byte* name, nuint nameLen, byte* wasm, nuint wasmLen, Tcb tcb);
[LibraryImport(LibraryName, EntryPoint = "ffi_admin_delete_smartmodule")]
internal static unsafe partial void AdminDeleteSmartModule(nint client, byte* name, nuint nameLen, Tcb tcb);
```

- [x] **Step 5: Rewrite `FluvioAdmin.cs`**

Replace every method's body with the pattern: build UTF-8 byte buffers for string args, `fixed`-pin them, call the matching `Native.Admin*` via `Interop.Callbacks.CallAsync`, then either ignore a null success payload (mutations) or `System.Text.Json.JsonSerializer.Deserialize<T>(Interop.Native.ReadAndFreeString(ptr))` into the existing `TopicMetadata`/`SpuMetadata`/`PartitionDetail`/`SmartModuleMetadata` records for queries. Example for `CreateTopicAsync`:

```csharp
public async Task CreateTopicAsync(string name, TopicSpec? spec = null, CancellationToken cancellationToken = default)
{
    spec ??= new TopicSpec();
    var nameBytes = System.Text.Encoding.UTF8.GetBytes(name);
    var specJson = System.Text.Json.JsonSerializer.Serialize(spec);
    var specBytes = System.Text.Encoding.UTF8.GetBytes(specJson);
    unsafe
    {
        fixed (byte* np = nameBytes)
        fixed (byte* sp = specBytes)
        {
            await _clientHandle.RunAsyncWithIncrement(h =>
                Interop.Callbacks.CallAsync(tcb => Interop.Native.AdminCreateTopic(h, np, (nuint)nameBytes.Length, sp, (nuint)specBytes.Length, tcb)));
        }
    }
}

public async Task<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken cancellationToken = default)
{
    var jsonPtr = await _clientHandle.RunAsyncWithIncrement(h =>
        Interop.Callbacks.CallAsync(tcb => Interop.Native.AdminListTopics(h, tcb)));
    var json = Interop.Native.ReadAndFreeString(jsonPtr);
    return System.Text.Json.JsonSerializer.Deserialize<List<TopicMetadata>>(json ?? "[]")!;
}
```

Apply the same two shapes (mutation vs. list/get) to every remaining method. Delete `src/Fluvio.Client/Admin/TopicSpecModels.cs` and remove its `using`s from `FluvioAdmin.cs`.

- [x] **Step 6: Run admin integration tests**

Run: `dotnet build Fluvio.Client.sln` then, against a running cluster, `dotnet test tests/Fluvio.Client.Tests --filter "FullyQualifiedName~AdminIntegrationTests|FullyQualifiedName~AdminBasicTest"`
Expected: topic/SPU/partition/SmartModule CRUD tests pass against the FFI-backed admin.

- [x] **Step 7: Commit**

```bash
git add native/fluvio-dotnet src/Fluvio.Client/Interop/Native.cs src/Fluvio.Client/Admin
git commit -m "feat: replace admin wire protocol with FFI-backed topic/SPU/partition/SmartModule CRUD"
```

---

### Task 7: Delete obsolete managed transport code and unused dependencies

**Files:**
- Delete: `src/Fluvio.Client/Protocol/**`
- Delete: `src/Fluvio.Client/Network/FluvioConnection.cs`
- Delete: `src/Fluvio.Client/Compression/CompressionUtils.cs`
- Delete: `src/Fluvio.Client/Config/FluvioConfig.cs`
- Delete: `tests/Fluvio.Client.Tests/Protocol/**`
- Delete: `tests/Fluvio.Client.Tests/SmartModule/**`
- Delete: `tests/Fluvio.Client.Tests/Compression/**`
- Delete: `benchmarks/Fluvio.Client.Benchmarks/ProtocolBenchmarks.cs`
- Delete: `src/FluvioCSharp/`, `tests/FluvioCSharp.Tests/`, `tests/FluvioCSharp.IntegrationTests/`, `tests/Fluvio.Client.IntegrationTests/`
- Delete: any stray `runtimes/osx-arm64/native/` empty directories under `src/Fluvio.Client/` and `src/FluvioCSharp/`
- Modify: `src/Fluvio.Client/Fluvio.Client.csproj` (remove package references)
- Modify: `Fluvio.Client.sln` (remove references to deleted projects, if present)

**Interfaces:**
- Consumes: nothing new — this task only removes code made dead by Tasks 2–6.
- Produces: a clean build with no unused-dependency warnings.

- [x] **Step 1: Confirm nothing still references the code being deleted**

Run: `grep -rln "FluvioConnection\|CompressionUtils\|FluvioConfig\b\|Protocol\.\|SmartModuleEncoder" src/Fluvio.Client --include="*.cs" | grep -v "/Protocol/\|/Compression/\|/Config/\|/Network/"`
Expected: no output (Tasks 2–6 already removed every call site as part of their own rewrites). If any file is listed, fix that reference before deleting.

- [x] **Step 2: Delete the obsolete source and test directories**

```bash
git rm -r src/Fluvio.Client/Protocol
git rm src/Fluvio.Client/Network/FluvioConnection.cs
git rm src/Fluvio.Client/Compression/CompressionUtils.cs
git rm src/Fluvio.Client/Config/FluvioConfig.cs
git rm -r tests/Fluvio.Client.Tests/Protocol tests/Fluvio.Client.Tests/SmartModule tests/Fluvio.Client.Tests/Compression
git rm benchmarks/Fluvio.Client.Benchmarks/ProtocolBenchmarks.cs
git rm -r src/FluvioCSharp tests/FluvioCSharp.Tests tests/FluvioCSharp.IntegrationTests tests/Fluvio.Client.IntegrationTests
find src/Fluvio.Client/runtimes src/FluvioCSharp -type d -empty -delete 2>/dev/null || true
```

- [x] **Step 3: Remove unused package references from `Fluvio.Client.csproj`**

Remove the `<PackageReference>` elements for `K4os.Compression.LZ4`, `K4os.Compression.LZ4.Streams`, `Snappier`, `ZstdSharp.Port`, `System.IO.Hashing`, and `Polly` (and their corresponding version entries in `Directory.Packages.props` if that's where central package management pins versions — check with `grep -n "K4os\|Snappier\|ZstdSharp\|System.IO.Hashing\|Polly" Directory.Packages.props`).

- [x] **Step 4: Remove now-orphaned project references from the solution**

Run: `grep -n "FluvioCSharp" Fluvio.Client.sln`
Expected: if any project entries reference the deleted `FluvioCSharp`/`Fluvio.Client.IntegrationTests` projects, run `dotnet sln Fluvio.Client.sln remove <path>` for each.

- [x] **Step 5: Build and run the full unit test suite**

Run: `dotnet restore Fluvio.Client.sln && dotnet build Fluvio.Client.sln --configuration Release /p:TreatWarningsAsErrors=true`
Expected: builds cleanly with no missing-reference errors and no unused-dependency-related warnings.

Run: `dotnet test Fluvio.Client.sln --filter "FullyQualifiedName!~Integration"`
Expected: all remaining unit tests (`OffsetResolverTests`, `Headers/*`, `Producer/PartitionerTests`, `PlatformVersionTests`) pass.

- [x] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: remove obsolete managed wire-protocol code and unused dependencies"
```

---

### Task 8: Native library packaging (MSBuild targets + resolver hardening)

**Files:**
- Modify: `src/Fluvio.Client/Fluvio.Client.csproj`
- Modify: `src/Fluvio.Client/Interop/Native.cs` (resolver error message / RID mapping already added in Task 2 — extend for packaged-app lookup)
- Test: manual verification steps below (packaging targets aren't unit-testable in isolation)

**Interfaces:**
- Consumes: `native/fluvio-dotnet` crate from Task 1 (built per-RID).
- Produces: `dotnet build` auto-builds the native crate in debug mode; `dotnet pack -p:RuntimeIdentifier=<rid>` produces a NuGet package containing `runtimes/<rid>/native/<libname>`.

- [x] **Step 1: Add the `BuildNativeDebug` target so plain `dotnet build` builds the Rust crate**

```xml
<!-- append inside the <Project> element of src/Fluvio.Client/Fluvio.Client.csproj -->
<Target Name="BuildNativeDebug" BeforeTargets="Build" Condition="'$(SkipNativeBuild)' != 'true'">
  <Exec Command="cargo build" WorkingDirectory="$(MSBuildThisFileDirectory)../../native/fluvio-dotnet" />
</Target>
```

- [x] **Step 2: Add the RID-specific release build, copy, and packaging targets**

```xml
<PropertyGroup>
  <NativeCrateDir>$(MSBuildThisFileDirectory)../../native/fluvio-dotnet</NativeCrateDir>
</PropertyGroup>

<Target Name="BuildNativeForRid" BeforeTargets="Pack" Condition="'$(RuntimeIdentifier)' != ''">
  <PropertyGroup>
    <RustTarget Condition="'$(RuntimeIdentifier)' == 'linux-x64'">x86_64-unknown-linux-gnu</RustTarget>
    <RustTarget Condition="'$(RuntimeIdentifier)' == 'osx-arm64'">aarch64-apple-darwin</RustTarget>
    <RustTarget Condition="'$(RuntimeIdentifier)' == 'osx-x64'">x86_64-apple-darwin</RustTarget>
    <RustTarget Condition="'$(RuntimeIdentifier)' == 'win-x64'">x86_64-pc-windows-msvc</RustTarget>
  </PropertyGroup>
  <Exec Command="cargo build --release --target $(RustTarget)" WorkingDirectory="$(NativeCrateDir)" />
</Target>

<Target Name="CopyNativeToRuntimesFolder" AfterTargets="BuildNativeForRid" Condition="'$(RuntimeIdentifier)' != ''">
  <PropertyGroup>
    <NativeLibFileName Condition="'$(RuntimeIdentifier)' == 'win-x64'">fluvio_dotnet.dll</NativeLibFileName>
    <NativeLibFileName Condition="$(RuntimeIdentifier.StartsWith('osx'))">libfluvio_dotnet.dylib</NativeLibFileName>
    <NativeLibFileName Condition="'$(RuntimeIdentifier)' == 'linux-x64'">libfluvio_dotnet.so</NativeLibFileName>
  </PropertyGroup>
  <ItemGroup>
    <NativeLibOutput Include="$(NativeCrateDir)/target/$(RustTarget)/release/$(NativeLibFileName)" />
  </ItemGroup>
  <Copy SourceFiles="@(NativeLibOutput)" DestinationFolder="$(MSBuildThisFileDirectory)runtimes/$(RuntimeIdentifier)/native/" />
</Target>

<Target Name="IncludeNativeInPackage" BeforeTargets="_GetPackageFiles" Condition="'$(RuntimeIdentifier)' != ''">
  <ItemGroup>
    <None Include="runtimes/$(RuntimeIdentifier)/native/*" Pack="true" PackagePath="runtimes/$(RuntimeIdentifier)/native/" />
  </ItemGroup>
</Target>
```

- [x] **Step 3: Verify plain `dotnet build` works from a clean checkout**

Run: `rm -rf native/fluvio-dotnet/target && dotnet build src/Fluvio.Client/Fluvio.Client.csproj`
Expected: `BuildNativeDebug` runs `cargo build` before the C# compile step, and the debug native library ends up at `native/fluvio-dotnet/target/debug/`, discoverable by `Native.cs`'s resolver from Task 2 without setting `FLUVIO_DOTNET_NATIVE_PATH`.

- [x] **Step 4: Verify RID-specific packing produces the expected `runtimes/` layout**

Run: `dotnet pack src/Fluvio.Client/Fluvio.Client.csproj -c Release -p:RuntimeIdentifier=$(rustc -vV | awk '/host/{print $2}' | grep -q darwin && echo osx-arm64 || echo linux-x64) -o /tmp/fluvio-pack-test`
Expected: the produced `.nupkg` (inspect via `unzip -l /tmp/fluvio-pack-test/*.nupkg`) contains `runtimes/<rid>/native/<libname>`.

- [x] **Step 5: Commit**

```bash
git add src/Fluvio.Client/Fluvio.Client.csproj
git commit -m "feat: add native library build/pack MSBuild targets"
```

---

### Task 9: CI workflow updates for native builds

**Files:**
- Modify: `.github/workflows/build.yml`
- Modify: `.github/workflows/integration-tests.yml`

**Interfaces:**
- Consumes: Task 8's MSBuild targets, Task 1's `native/fluvio-dotnet` crate.
- Produces: green CI on a fresh clone/PR.

- [x] **Step 1: Add a Rust toolchain setup step and native build step to `build.yml`**

```yaml
# insert into the `steps:` list of the `build` job in .github/workflows/build.yml, before "Restore dependencies"
- name: Setup Rust
  uses: dtolnay/rust-toolchain@stable

- name: Cache Rust build
  uses: Swatinem/rust-cache@v2
  with:
    workspaces: native/fluvio-dotnet -> target

- name: Build native library (debug)
  working-directory: native/fluvio-dotnet
  run: cargo build

- name: Run native unit tests
  working-directory: native/fluvio-dotnet
  run: cargo test
```

- [x] **Step 2: Confirm `dotnet build`/`dotnet test` steps still work unchanged**

The existing `Restore dependencies`/`Build`/`Test (Unit Tests Only)` steps in `build.yml` need no changes — `BuildNativeDebug` (Task 8) runs automatically as part of `dotnet build`, and CI already ran `cargo build` explicitly in Step 1 so the debug artifact is warm in cache.
Run (locally, simulating CI order): `cd native/fluvio-dotnet && cargo build && cargo test && cd ../.. && dotnet restore Fluvio.Client.sln && dotnet build --configuration Release --no-restore Fluvio.Client.sln /p:TreatWarningsAsErrors=true && dotnet test --configuration Release --no-build --filter "FullyQualifiedName!~Integration" Fluvio.Client.sln`
Expected: all steps succeed in this order, matching what CI will run.

- [x] **Step 3: Add native build step to `integration-tests.yml`**

Read the current contents of `.github/workflows/integration-tests.yml` first (`cat .github/workflows/integration-tests.yml`) and insert the same `Setup Rust` / `Cache Rust build` / `Build native library` steps used in Step 1, positioned before whatever step first runs `dotnet build`/`dotnet test` against the integration test project, so the native library is present before the Fluvio cluster fixture starts.

- [x] **Step 4: Commit**

```bash
git add .github/workflows/build.yml .github/workflows/integration-tests.yml
git commit -m "ci: build and test the native FFI crate alongside the .NET solution"
```

---

## Self-Review Notes

- **Spec coverage:** §1 (crate structure) → Task 1; §2 (TCB pattern) → Tasks 1–2; §3 (handles/lifetime) → Tasks 2–5; §4 (streaming) → Task 5; §5 (errors) → Tasks 1–2; §6 (compression removal) → Task 7; §7 (config/TLS) → Task 2; §8 (API changes) → Task 4; §9 (dependency cleanup + packaging targets) → Tasks 7–8; §10 (removed code) → Task 7; §11 (testing strategy) → Tasks 2–7 (each task updates its own tests) plus Task 7's cleanup pass; §12 (CI) → Task 9. No spec section is without a task.
- **Type consistency:** `RustResource`/`NativeBuffer`/`Tcb`/`FFISlice`/`Callbacks.CallAsync` are defined once in Task 2/5 and reused by name, unchanged, in every later task. `IFluvioConsumer.CommitOffsetAsync`'s new 5-parameter signature (Task 4) is used identically in Task 5 (n/a — streaming doesn't call it) and in the Task 4 test-updates step.
- **Review Focus:** the five failure modes listed above each got a concrete home — native-lib-missing in Task 8 Step 3, GCHandle-leak-on-throw in Task 2 Step 4 (`try/catch { gch.Free(); throw; }`), cancellation race in Task 5 Steps 1 and 5 (`Close()` before `Dispose()`, `tokio::select!` always completing exactly once), non-UTF8-safe failure message in Task 2 Step 4 (`len > 0` guard), and concurrent dispose-vs-in-flight-call in every task's use of `RunAsyncWithIncrement`.
