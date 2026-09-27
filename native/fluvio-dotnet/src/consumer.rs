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
use crate::tcb::{complete_error, complete_failure, complete_success, Tcb};
use fluvio::consumer::{
    BoxConsumerStream, ConsumerConfigExt, ConsumerOffset, ConsumerStream, OffsetManagementStrategy,
    RetryMode,
};
use fluvio::{Fluvio, Offset};
use futures::StreamExt;
use std::os::raw::c_void;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

/// `OffsetResolver.EndOffset` on the C# side is `-1`, used as a sentinel for "start from the
/// end of the topic" (e.g. the default `OffsetResetStrategy.Latest`). `Offset::absolute` rejects
/// any negative value, so every FFI entry point that takes a caller-resolved starting offset must
/// route it through here instead of calling `Offset::absolute` directly.
fn resolve_start_offset(offset: i64) -> Offset {
    if offset < 0 {
        Offset::end()
    } else {
        Offset::absolute(offset).expect("offset already checked non-negative")
    }
}

#[repr(C)]
pub struct FFIRecordArray {
    pub records: *mut *mut c_void,
    pub len: usize,
}

/// Owns a set of `box_record`-allocated pointers until explicitly handed off. Frees every
/// still-owned pointer on drop, so a partially-filled batch is never leaked regardless of how
/// the accumulating function exits: an error return (`?`/`return Err`), a panic, OR the future
/// simply being dropped without ever resolving (which is exactly what happens to `ffi_consumer_
/// fetch_batch`'s `work` future when `crate::cancel::race` cancels it mid-loop - a plain
/// `Vec<usize>`'s own `Drop` does nothing for the records those integers point to). Call
/// `into_inner()` on the success path to take ownership out without freeing.
struct RecordPtrGuard(Vec<usize>);

impl RecordPtrGuard {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn push(&mut self, ptr: usize) {
        self.0.push(ptr);
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn into_inner(mut self) -> Vec<usize> {
        std::mem::take(&mut self.0)
    }
}

impl Drop for RecordPtrGuard {
    fn drop(&mut self) {
        for ptr in self.0.drain(..) {
            unsafe { crate::ffi_types::ffi_record_free(ptr as *mut c_void) };
        }
    }
}

#[no_mangle]
pub extern "C" fn ffi_consumer_fetch_batch(
    client: *mut c_void,
    topic: *const u8, topic_len: usize,
    partition: u32, offset: i64, max_bytes: u32,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    // Captured as a plain address (rather than the raw pointer) because raw pointers are
    // not `Send`, even though the `Fluvio` value they point to is; the pointer is only
    // ever dereferenced on the Tokio worker thread that runs this spawned task.
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        // `*mut c_void` is not `Send`, so the in-progress record pointers are carried across
        // await points as plain `usize` addresses and cast back to pointers only once the
        // async block (and thus the spawned future) has finished.
        let work = async {
            // Deliberately continuous (unlike this function's pre-Task-2 config): with
            // `disable_continuous(true)` + `RetryMode::Disabled`, `stream.next()` returned
            // `None` almost immediately once there was nothing buffered, so removing the old
            // per-record timeout would have been a no-op for the empty-topic case (it never
            // actually waited) — the `FetchBatchAsync_EmptyTopic_BlocksUntilTimeout` regression
            // test requires a real block there, bounded only by the caller's cancellation.
            let config = ConsumerConfigExt::builder()
                .topic(topic)
                .partition(partition)
                .offset_start(resolve_start_offset(offset))
                .offset_strategy(OffsetManagementStrategy::None)
                .max_bytes(max_bytes as i32)
                .build()?;
            let mut stream = client.consumer_with_config(config).await?;
            let mut out = RecordPtrGuard::new();
            let mut bytes_read = 0usize;
            loop {
                if bytes_read >= max_bytes as usize { break; }
                // The *first* record is awaited with no internal bound at all — only the
                // caller's `cancel` handle (raced around this whole `work` future below) can
                // stop that wait, which is what makes the empty-topic regression test genuine.
                // Once at least one record has arrived, a short grace-period timeout lets a
                // finite batch (fewer bytes than `max_bytes`, no further producer activity)
                // return what it already has instead of waiting forever for more data that
                // will never come; this never fights an explicit `CancellationToken`, since a
                // shorter caller-driven cancellation still wins the outer race regardless.
                let next = if out.is_empty() {
                    stream.next().await
                } else {
                    match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
                        Ok(next) => next,
                        Err(_elapsed) => break,
                    }
                };
                match next {
                    Some(Ok(record)) => {
                        bytes_read += record.value().len();
                        out.push(box_record(
                            record.offset(), record.timestamp(), partition,
                            record.key().map(|k| k.to_vec()),
                            record.value().to_vec(),
                        ) as usize);
                    }
                    Some(Err(e)) => return Err(anyhow::anyhow!("{e}")),
                    None => break,
                }
            }
            Ok::<_, anyhow::Error>(out.into_inner())
        };
        let result = unsafe { crate::cancel::race(cancel_addr, work).await };
        match result {
            Ok(Ok(records)) => {
                let records: Vec<*mut c_void> = records.into_iter().map(|p| p as *mut c_void).collect();
                let boxed = records.into_boxed_slice();
                let array = Box::new(FFIRecordArray { records: boxed.as_ptr() as *mut *mut c_void, len: boxed.len() });
                std::mem::forget(boxed);
                unsafe { complete_success(tcb, Box::into_raw(array) as *mut c_void) };
            }
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
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
    cancel: *mut c_void,
    tcb: Tcb,
) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let consumer_id = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(consumer_id, consumer_id_len) }).into_owned();
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = client.consumer_offsets();
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(offsets)) => {
                let found = offsets.into_iter().find(|o: &ConsumerOffset| {
                    o.consumer_id == consumer_id && o.topic == topic && o.partition == partition
                });
                match found {
                    Some(o) => unsafe { complete_success(tcb, o.offset as *mut c_void) },
                    None => unsafe { complete_success(tcb, (-1i64) as *mut c_void) },
                }
            }
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_consumer_commit_offset(
    client: *mut c_void,
    consumer_id: *const u8, consumer_id_len: usize,
    topic: *const u8, topic_len: usize,
    partition: u32, offset: i64,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let consumer_id = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(consumer_id, consumer_id_len) }).into_owned();
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let topic_for_err = topic.clone();
        let work = async {
            let config = ConsumerConfigExt::builder()
                .topic(topic)
                .partition(partition)
                .offset_start(resolve_start_offset(offset))
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
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(())) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

// --- Streaming (Task 5) ---
//
// Deviations from the plan's sketch, again reconciled against the real fluvio 0.50.1 API:
// - There is no bare `Pin<Box<dyn Stream<Item = Result<fluvio::consumer::Record, fluvio::FluvioError>> + Send>>`
//   to name here: `Fluvio::consumer_with_config` returns an opaque `impl ConsumerStream<Item =
//   Result<Record, ErrorCode>>`, and the crate already exports exactly the boxed alias needed to
//   store it in a struct field: `fluvio::consumer::BoxConsumerStream` (`Pin<Box<dyn ConsumerStream<
//   Item = Result<Record, ErrorCode>> + Send + 'static>>`). `RecordStream` below is that alias.
// - `PartitionConsumer`/`consumer.stream(...)` (used in the plan's snippet) is deprecated the same
//   way it was for fetch-batch in Task 4; `Fluvio::consumer_with_config` is used instead, built with
//   `OffsetManagementStrategy::None` (offset tracking for streaming is out of scope here — callers
//   use the separate fetch/commit-offset FFI from Task 4) and the default (continuous) retry mode,
//   since unlike fetch-batch this is meant to run indefinitely rather than return a bounded slice.
// - The stream item's error type is `fluvio_protocol::link::ErrorCode`, not `FluvioError`, so it is
//   converted to `anyhow::Error` via `anyhow::anyhow!("{e}")` rather than `.into()`.
// - `StreamHandle` also carries the `partition` the stream was opened against, so `ffi_stream_next`
//   can stamp real per-record partition metadata into the boxed `FFIRecord` (the plan's sketch
//   hardcoded `0`); the C# side overrides this with its own `partition` parameter regardless
//   (`NativeBuffer.ToConsumeRecord(recordPtr, partition)`), so this is a correctness nicety rather
//   than something behavior depends on.
type RecordStream = BoxConsumerStream;

pub struct StreamHandle {
    inner: Arc<Mutex<Option<RecordStream>>>,
    cancel: Arc<Notify>,
    partition: u32,
}

#[no_mangle]
pub extern "C" fn ffi_stream_new(
    client: *mut c_void,
    topic: *const u8, topic_len: usize,
    partition: u32, offset: i64,
    tcb: Tcb,
) {
    let client_addr = client as usize;
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let result: anyhow::Result<RecordStream> = async {
            let config = ConsumerConfigExt::builder()
                .topic(topic)
                .partition(partition)
                .offset_start(resolve_start_offset(offset))
                .offset_strategy(OffsetManagementStrategy::None)
                .build()?;
            let stream = client.consumer_with_config(config).await?;
            Ok(Box::pin(stream) as RecordStream)
        }
        .await;
        match result {
            Ok(stream) => {
                let handle = Box::new(StreamHandle {
                    inner: Arc::new(Mutex::new(Some(stream))),
                    cancel: Arc::new(Notify::new()),
                    partition,
                });
                unsafe { complete_success(tcb, Box::into_raw(handle) as *mut c_void) };
            }
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_stream_next(stream: *mut c_void, tcb: Tcb) {
    let handle_addr = stream as usize;
    crate::tcb::spawn_guarded(tcb, async move {
        let handle = unsafe { &*(handle_addr as *const StreamHandle) };
        let mut taken = handle.inner.lock().await;
        let mut record_stream = match taken.take() {
            Some(s) => s,
            None => {
                drop(taken);
                unsafe { complete_error(tcb, anyhow::anyhow!("cancelled")) };
                return;
            }
        };
        drop(taken);

        tokio::select! {
            next = record_stream.next() => {
                match next {
                    Some(Ok(record)) => {
                        // `*mut c_void` is not `Send`; carry it as a `usize` across the
                        // `.lock().await` below, same convention as `ffi_consumer_fetch_batch`.
                        let ptr_addr = box_record(
                            record.offset(), record.timestamp(), handle.partition,
                            record.key().map(|k| k.to_vec()), record.value().to_vec(),
                        ) as usize;
                        { let mut g = handle.inner.lock().await; *g = Some(record_stream); }
                        unsafe { complete_success(tcb, ptr_addr as *mut c_void) };
                    }
                    Some(Err(e)) => unsafe { complete_error(tcb, anyhow::anyhow!("{e}")) },
                    None => unsafe { complete_success(tcb, std::ptr::null_mut()) },
                }
            }
            _ = handle.cancel.notified() => {
                drop(record_stream);
                unsafe { complete_error(tcb, anyhow::anyhow!("cancelled")) };
            }
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_stream_close(stream: *mut c_void) {
    if stream.is_null() {
        return;
    }
    let handle = unsafe { &*(stream as *const StreamHandle) };
    handle.cancel.notify_one();
}

/// # Safety
/// `stream` must have come from `ffi_stream_new` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_stream_drop(stream: *mut c_void) {
    if stream.is_null() {
        return;
    }
    let handle = Box::from_raw(stream as *mut StreamHandle);
    handle.cancel.notify_one();
    drop(handle);
}
