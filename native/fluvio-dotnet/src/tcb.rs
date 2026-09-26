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
