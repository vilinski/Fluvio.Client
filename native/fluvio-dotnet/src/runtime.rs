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
