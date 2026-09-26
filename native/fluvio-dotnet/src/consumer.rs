// native/fluvio-dotnet/src/consumer.rs (fetch/offset portion; streaming appended in Task 5)
//
// Deviations from the original plan, discovered via `cargo doc -p fluvio` against the pinned
// fluvio 0.50.1:
// - There is no `client.consumer_offsets()` "offsets client" with `fetch_offset`/`commit_offset`
//   methods. `Fluvio::consumer_offsets()` instead returns the flat `Vec<ConsumerOffset>` of every
//   consumer offset currently stored in the cluster; `ffi_consumer_fetch_last_offset` filters that
//   list for the matching consumer/topic/partition.
// - There is no standalone "set consumer offset" RPC at all in this client version: the SPU wire
//   protocol's `UpdateConsumerOffsetRequest` takes a `session_id` tied to an active managed
//   StreamFetch session, and the only client-facing API for advancing it is
//   `ConsumerStream::offset_commit()` (marks the last-*seen* record's offset as committed)
//   followed by `offset_flush()` (sends it to the SPU). There is no way to commit an arbitrary
//   offset without an active stream having actually observed a record at that offset.
//   `ffi_consumer_commit_offset` opens a short-lived managed stream at the requested offset,
//   waits (bounded) for the record at that exact position, and commits+flushes once it arrives.
//   This still lets the C# signature drop `sessionId` (spec §8) since the FFI layer hides the
//   session entirely, but it does mean committing an offset with no record at that position
//   (e.g. one past the current end of the log) fails rather than succeeding silently.
// - `partition` is `fluvio_types::PartitionId` = `u32` (not `i32`), so no cast is needed at the
//   `client.consumer_with_config`/`ConsumerOffset` call sites.
// - `PartitionConsumer::stream()` (used by the original plan snippet for fetch-batch) is
//   deprecated in favor of `Fluvio::consumer_with_config`, which is used here instead; its
//   `disable_continuous(true)` + `RetryMode::Disabled` options give the "return what's
//   available without blocking indefinitely" behavior the plan's polling loop was working
//   around, so a short `tokio::time::timeout` around `stream.next()` is kept only as a
//   best-effort bound rather than the primary termination mechanism.
use crate::ffi_types::box_record;
use crate::tcb::{complete_error, complete_success, Tcb};
use fluvio::consumer::{
    ConsumerConfigExt, ConsumerOffset, ConsumerStream, OffsetManagementStrategy, RetryMode,
};
use fluvio::{Fluvio, Offset};
use futures::StreamExt;
use std::os::raw::c_void;
use std::time::Duration;

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
    // Captured as a plain address (rather than the raw pointer) because raw pointers are
    // not `Send`, even though the `Fluvio` value they point to is; the pointer is only
    // ever dereferenced on the Tokio worker thread that runs this spawned task.
    let client_addr = client as usize;
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        // `*mut c_void` is not `Send`, so the in-progress record pointers are carried across
        // await points as plain `usize` addresses and cast back to pointers only once the
        // async block (and thus the spawned future) has finished.
        let result: anyhow::Result<Vec<usize>> = async {
            let config = ConsumerConfigExt::builder()
                .topic(topic)
                .partition(partition)
                .offset_start(Offset::absolute(offset)?)
                .offset_strategy(OffsetManagementStrategy::None)
                .disable_continuous(true)
                .retry_mode(RetryMode::Disabled)
                .max_bytes(max_bytes as i32)
                .build()?;
            let mut stream = client.consumer_with_config(config).await?;
            let mut out = Vec::new();
            let mut bytes_read = 0usize;
            while bytes_read < max_bytes as usize {
                match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                    Ok(Some(Ok(record))) => {
                        bytes_read += record.value().len();
                        out.push(box_record(
                            record.offset(), record.timestamp(), partition,
                            record.key().map(|k| k.to_vec()),
                            record.value().to_vec(),
                        ) as usize);
                    }
                    Ok(Some(Err(e))) => return Err(anyhow::anyhow!("{e}")),
                    _ => break,
                }
            }
            Ok(out)
        }.await;
        match result {
            Ok(records) => {
                let records: Vec<*mut c_void> = records.into_iter().map(|p| p as *mut c_void).collect();
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
    let client_addr = client as usize;
    let consumer_id = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(consumer_id, consumer_id_len) }).into_owned();
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        match client.consumer_offsets().await {
            Ok(offsets) => {
                let found = offsets.into_iter().find(|o: &ConsumerOffset| {
                    o.consumer_id == consumer_id && o.topic == topic && o.partition == partition
                });
                match found {
                    Some(o) => unsafe { complete_success(tcb, o.offset as *mut c_void) },
                    None => unsafe { complete_success(tcb, (-1i64) as *mut c_void) },
                }
            }
            Err(e) => unsafe { complete_error(tcb, e) },
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
    let client_addr = client as usize;
    let consumer_id = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(consumer_id, consumer_id_len) }).into_owned();
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::runtime::runtime().spawn(async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let topic_for_err = topic.clone();
        let result: anyhow::Result<()> = async {
            let config = ConsumerConfigExt::builder()
                .topic(topic)
                .partition(partition)
                .offset_start(Offset::absolute(offset)?)
                .offset_strategy(OffsetManagementStrategy::Manual)
                .offset_consumer(consumer_id)
                .disable_continuous(true)
                .retry_mode(RetryMode::Disabled)
                .build()?;
            let mut stream = client.consumer_with_config(config).await?;
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
                Ok(Some(Ok(_record))) => {
                    stream.offset_commit().await.map_err(|e| anyhow::anyhow!("{e}"))?;
                    stream.offset_flush().await.map_err(|e| anyhow::anyhow!("{e}"))?;
                    Ok(())
                }
                Ok(Some(Err(e))) => Err(anyhow::anyhow!("{e}")),
                _ => Err(anyhow::anyhow!(
                    "commit_offset: no record found at offset {offset} for topic '{topic_for_err}' partition {partition} within timeout"
                )),
            }
        }.await;
        match result {
            Ok(()) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}
