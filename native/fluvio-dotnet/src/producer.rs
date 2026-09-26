// native/fluvio-dotnet/src/producer.rs
use crate::tcb::{complete_error, complete_failure, complete_success, Tcb};
use fluvio::{Fluvio, RecordKey, TopicProducerPool};
use std::os::raw::c_void;

#[no_mangle]
pub extern "C" fn ffi_producer_new(client: *mut c_void, topic: *const u8, topic_len: usize, cancel: *mut c_void, tcb: Tcb) {
    // Captured as a plain address (rather than the raw pointer) because raw pointers are
    // not `Send`, even though the `Fluvio` value they point to is; the pointer is only
    // ever dereferenced on the Tokio worker thread that runs this spawned task.
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let topic = unsafe { std::slice::from_raw_parts(topic, topic_len) };
    let topic = String::from_utf8_lossy(topic).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = client.topic_producer(topic);
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(producer)) => {
                let ptr = Box::into_raw(Box::new(producer)) as *mut c_void;
                unsafe { complete_success(tcb, ptr) };
            }
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_producer_send(
    producer: *mut c_void,
    key: *const u8, key_len: usize,
    value: *const u8, value_len: usize,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    let producer_addr = producer as usize;
    let cancel_addr = cancel as usize;
    let key = if key.is_null() { None } else { Some(unsafe { std::slice::from_raw_parts(key, key_len) }.to_vec()) };
    let value = if value.is_null() { Vec::new() } else { unsafe { std::slice::from_raw_parts(value, value_len) }.to_vec() };
    crate::tcb::spawn_guarded(tcb, async move {
        let producer = unsafe { &*(producer_addr as *const TopicProducerPool) };
        let work = async {
            let produce_output = match key {
                Some(k) => producer.send(k, value).await?,
                None => producer.send(RecordKey::NULL, value).await?,
            };
            let metadata = produce_output.wait().await?;
            Ok::<_, anyhow::Error>(metadata.offset())
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(offset)) => unsafe { complete_success(tcb, offset as *mut c_void) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_producer_flush(producer: *mut c_void, cancel: *mut c_void, tcb: Tcb) {
    let producer_addr = producer as usize;
    let cancel_addr = cancel as usize;
    crate::tcb::spawn_guarded(tcb, async move {
        let producer = unsafe { &*(producer_addr as *const TopicProducerPool) };
        let work = producer.flush();
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(())) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
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
    drop(Box::from_raw(producer as *mut TopicProducerPool));
}
