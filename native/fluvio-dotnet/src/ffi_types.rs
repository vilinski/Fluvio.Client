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
