// native/fluvio-dotnet/src/cancel.rs
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

pub struct CancelHandle {
    pub notify: Arc<Notify>,
    // `Notify::notify_waiters` only wakes tasks that are *already* waiting; a trigger that
    // races ahead of `race()`'s `tokio::select!` being set up (e.g. a `CancellationToken`
    // that is already cancelled when `CancellationBridge.Create` registers it, firing the
    // native trigger synchronously before the FFI call even starts) would otherwise be lost
    // and the future would run to completion unobserved. This flag makes cancellation
    // sticky so `race()` can check it before ever waiting on the `Notify`.
    pub cancelled: AtomicBool,
}

#[no_mangle]
pub extern "C" fn ffi_cancel_new() -> *mut c_void {
    Box::into_raw(Box::new(CancelHandle { notify: Arc::new(Notify::new()), cancelled: AtomicBool::new(false) })) as *mut c_void
}

/// # Safety
/// `handle` must have come from `ffi_cancel_new` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_cancel_trigger(handle: *mut c_void) {
    if handle.is_null() { return; }
    let handle = &*(handle as *const CancelHandle);
    handle.cancelled.store(true, Ordering::SeqCst);
    // `notify_one` (rather than `notify_waiters`) stores a wake-up permit when nothing is
    // currently waiting, so a `race()` call that starts *after* this trigger still observes
    // it via `notified().await` instead of missing it (each `CancelHandle` is used by exactly
    // one in-flight `race()` call, so a single stored permit is always the right semantics).
    handle.notify.notify_one();
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
/// Takes `cancel` as a plain address rather than `*mut c_void` because a raw pointer
/// is not `Send`, even though the `CancelHandle` it points to is (`Arc<Notify>`), and
/// this future is held across an `.await` inside `crate::tcb::spawn_guarded`, which
/// requires `Send + 'static`. Callers cast their `*mut c_void` to `usize` and back
/// (the same convention used for client/producer pointers elsewhere in this crate).
///
/// # Safety
/// `cancel`, if non-zero, must be an address that came from `ffi_cancel_new` and
/// remains valid (not dropped) for the duration of this call.
pub async unsafe fn race<T>(cancel: usize, fut: impl std::future::Future<Output = T>) -> Result<T, Cancelled> {
    if cancel == 0 {
        return Ok(fut.await);
    }
    let handle = &*(cancel as *const CancelHandle);
    if handle.cancelled.load(Ordering::SeqCst) {
        return Err(Cancelled);
    }
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
        let h2 = handle as usize;
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            unsafe { ffi_cancel_trigger(h2 as *mut c_void) };
        });
        let result = unsafe { race(handle as usize, futures::future::pending::<()>()).await };
        assert!(result.is_err());
        unsafe { ffi_cancel_drop(handle) };
    }

    #[tokio::test]
    async fn race_returns_ok_when_future_completes_first() {
        let handle = ffi_cancel_new();
        let result = unsafe { race(handle as usize, async { 42 }).await };
        assert!(matches!(result, Ok(42)));
        unsafe { ffi_cancel_drop(handle) };
    }

    #[tokio::test]
    async fn race_with_null_cancel_never_cancels() {
        let result = unsafe { race(0, async { 7 }).await };
        assert!(matches!(result, Ok(7)));
    }
}
