// native/fluvio-dotnet/src/producer.rs
//
// Custom partitioner bridging (Task 6 of the hardening plan):
// The official fluvio 0.50.1 client's `send(key, value)` always routes through whatever
// `Partitioner` was configured on the producer AT CREATION TIME (`TopicProducerConfig`); there
// is no per-send override and no way to bypass it. To let C#'s `IPartitioner` (which computes a
// partition per-call, potentially differently for every record) control routing, every producer
// created with a custom C# partitioner installs `DynamicPartitioner` below, which simply reads
// whatever partition value the CURRENT async task placed into the `EXPLICIT_PARTITION` task-local
// via `.scope(...)` around that one `send()` call. This is race-free under concurrent sends on the
// same producer because `tokio::task_local!` values are scoped per-task, not shared/global.
// Producers with no C# partitioner configured never touch this at all — `client.topic_producer()`
// keeps using fluvio's own built-in default partitioner exactly as before, zero regression risk.
use crate::tcb::{complete_error, complete_failure, complete_success, Tcb};
use fluvio::{
    Fluvio, Partitioner, PartitionId, PartitionerConfig, RecordKey, TopicProducerConfigBuilder,
    TopicProducerPool,
};
use std::os::raw::c_void;
use std::sync::Arc;

tokio::task_local! {
    static EXPLICIT_PARTITION: std::cell::Cell<PartitionId>;
}

struct DynamicPartitioner;

impl Partitioner for DynamicPartitioner {
    fn partition(&self, _config: &PartitionerConfig, _key: Option<&[u8]>, _value: &[u8]) -> PartitionId {
        EXPLICIT_PARTITION.try_with(|c| c.get()).unwrap_or(0)
    }
}

#[no_mangle]
pub extern "C" fn ffi_producer_new(
    client: *mut c_void,
    topic: *const u8, topic_len: usize,
    use_explicit_partitioning: u8,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    // Captured as a plain address (rather than the raw pointer) because raw pointers are
    // not `Send`, even though the `Fluvio` value they point to is; the pointer is only
    // ever dereferenced on the Tokio worker thread that runs this spawned task.
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let topic = unsafe { std::slice::from_raw_parts(topic, topic_len) };
    let topic = String::from_utf8_lossy(topic).into_owned();
    let use_explicit = use_explicit_partitioning != 0;
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            if use_explicit {
                let config = TopicProducerConfigBuilder::default()
                    .partitioner(Arc::new(DynamicPartitioner))
                    .build()
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                client.topic_producer_with_config(topic, config).await
            } else {
                client.topic_producer(topic).await
            }
        };
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
    partition: i64,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    let producer_addr = producer as usize;
    let cancel_addr = cancel as usize;
    let key = if key.is_null() { None } else { Some(unsafe { std::slice::from_raw_parts(key, key_len) }.to_vec()) };
    let value = if value.is_null() { Vec::new() } else { unsafe { std::slice::from_raw_parts(value, value_len) }.to_vec() };
    crate::tcb::spawn_guarded(tcb, async move {
        let producer = unsafe { &*(producer_addr as *const TopicProducerPool) };
        let send_and_wait = async {
            let produce_output = match key {
                Some(k) => producer.send(k, value).await?,
                None => producer.send(RecordKey::NULL, value).await?,
            };
            let metadata = produce_output.wait().await?;
            Ok::<_, anyhow::Error>(metadata.offset())
        };
        // partition >= 0 means the producer was created with `use_explicit_partitioning = 1`
        // (a C# IPartitioner is configured) and this value is what it computed for this record;
        // -1 means no override — the producer's own default partitioner (unchanged) decides.
        let work = async {
            if partition >= 0 {
                EXPLICIT_PARTITION
                    .scope(std::cell::Cell::new(partition as PartitionId), send_and_wait)
                    .await
            } else {
                send_and_wait.await
            }
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
