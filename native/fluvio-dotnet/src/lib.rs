// native/fluvio-dotnet/src/lib.rs
pub mod runtime;
pub mod tcb;
pub mod ffi_types;
pub mod error;
pub mod cancel;
pub mod client;
pub mod producer;
pub mod consumer;
pub mod admin;

/// Debug-only entry point for regression-testing the panic boundary from C#.
/// Never compiled into a release build.
#[cfg(debug_assertions)]
#[no_mangle]
pub extern "C" fn ffi_debug_trigger_panic(tcb: crate::tcb::Tcb) {
    crate::tcb::spawn_guarded(tcb, async { panic!("debug-triggered panic for testing") });
}
