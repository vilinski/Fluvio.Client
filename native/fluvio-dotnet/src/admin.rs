// native/fluvio-dotnet/src/admin.rs
use crate::tcb::{complete_error, complete_failure, complete_string_success, complete_success, Tcb};
use fluvio::metadata::objects::Metadata;
use fluvio::{Fluvio, FluvioAdmin};
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::smartmodule::{SmartModuleSpec, SmartModuleWasm};
use fluvio_controlplane_metadata::spu::{SpuSpec, SpuStatusResolution, SpuType};
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
    cancel: *mut c_void,
    tcb: Tcb,
) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    let spec_json = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(spec_json, spec_json_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            let partitions: u32 = serde_json::from_str::<serde_json::Value>(&spec_json)?["partitions"].as_u64().unwrap_or(1) as u32;
            let replication: u32 = serde_json::from_str::<serde_json::Value>(&spec_json)?["replicationFactor"].as_u64().unwrap_or(1) as u32;
            let spec = TopicSpec::new_computed(partitions, replication, None);
            admin.create(name, false, spec).await?;
            Ok::<_, anyhow::Error>(())
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

#[no_mangle]
pub extern "C" fn ffi_admin_delete_topic(client: *mut c_void, name: *const u8, name_len: usize, cancel: *mut c_void, tcb: Tcb) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            admin.delete::<TopicSpec>(name).await?;
            Ok::<_, anyhow::Error>(())
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

#[no_mangle]
pub extern "C" fn ffi_admin_list_topics(client: *mut c_void, cancel: *mut c_void, tcb: Tcb) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
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
            Ok::<_, anyhow::Error>(json!(dtos).to_string())
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(json)) => unsafe { complete_string_success(tcb, json) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_admin_get_topic(client: *mut c_void, name: *const u8, name_len: usize, cancel: *mut c_void, tcb: Tcb) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            let topics = admin.list::<TopicSpec, String>(vec![name]).await?;
            Ok::<_, anyhow::Error>(topics.into_iter().next().map(|t| json!({
                "name": t.name,
                "partitions": t.spec.partitions(),
                "replicationFactor": t.spec.replication_factor(),
                "status": format!("{:?}", t.status.resolution),
            }).to_string()))
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(Some(json))) => unsafe { complete_string_success(tcb, json) },
            Ok(Ok(None)) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_admin_list_spus(client: *mut c_void, cancel: *mut c_void, tcb: Tcb) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            let spus = admin.list::<SpuSpec, String>(vec![]).await?;
            let dtos: Vec<_> = spus.into_iter().map(spu_to_json).collect();
            Ok::<_, anyhow::Error>(json!(dtos).to_string())
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(json)) => unsafe { complete_string_success(tcb, json) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_admin_get_spu(client: *mut c_void, spu_id: i32, cancel: *mut c_void, tcb: Tcb) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            let spus = admin.list::<SpuSpec, String>(vec![]).await?;
            Ok::<_, anyhow::Error>(spus.into_iter().find(|m| m.spec.id == spu_id).map(|m| spu_to_json(m).to_string()))
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(Some(json))) => unsafe { complete_string_success(tcb, json) },
            Ok(Ok(None)) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

fn spu_to_json(m: Metadata<SpuSpec>) -> serde_json::Value {
    json!({
        "id": m.spec.id,
        "name": m.name,
        "spuType": match m.spec.spu_type {
            SpuType::Managed => "Managed",
            SpuType::Custom => "Custom",
        },
        "publicEndpoint": m.spec.public_endpoint.addr(),
        "privateEndpoint": format!("{}", m.spec.private_endpoint),
        "rack": m.spec.rack,
        "status": match m.status.resolution {
            SpuStatusResolution::Online => "Online",
            SpuStatusResolution::Offline => "Offline",
            SpuStatusResolution::Init => "Init",
        },
    })
}

#[no_mangle]
pub extern "C" fn ffi_admin_list_partitions(
    client: *mut c_void,
    topic_filter: *const u8, topic_filter_len: usize,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let topic_filter = if topic_filter.is_null() {
        None
    } else {
        Some(String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic_filter, topic_filter_len) }).into_owned())
    };
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            let partitions = admin.list::<PartitionSpec, String>(vec![]).await?;
            let dtos: Vec<_> = partitions
                .into_iter()
                .filter(|m| {
                    topic_filter.as_ref().map(|f| split_replica_key(&m.name).0 == *f).unwrap_or(true)
                })
                .map(partition_to_json)
                .collect();
            Ok::<_, anyhow::Error>(json!(dtos).to_string())
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(json)) => unsafe { complete_string_success(tcb, json) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_admin_get_partition(
    client: *mut c_void,
    topic: *const u8, topic_len: usize,
    partition: u32,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let topic = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(topic, topic_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let key = format!("{topic}-{partition}");
        let work = async {
            let admin = admin_for(client).await?;
            let partitions = admin.list::<PartitionSpec, String>(vec![]).await?;
            Ok::<_, anyhow::Error>(partitions.into_iter().find(|m| m.name == key).map(|m| partition_to_json(m).to_string()))
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(Some(json))) => unsafe { complete_string_success(tcb, json) },
            Ok(Ok(None)) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

/// `Metadata<PartitionSpec>::name` is formatted by `ReplicaKey`'s `Display` as `{topic}-{partition}`.
/// Splits it back into `(topic, partition_id)`, defaulting the partition id to `0` if the name
/// doesn't contain the expected separator (should not happen for well-formed SC responses).
fn split_replica_key(name: &str) -> (String, i32) {
    match name.rsplit_once('-') {
        Some((topic, partition)) => match partition.parse::<i32>() {
            Ok(id) => (topic.to_string(), id),
            Err(_) => (name.to_string(), 0),
        },
        None => (name.to_string(), 0),
    }
}

fn partition_to_json(m: Metadata<PartitionSpec>) -> serde_json::Value {
    let (topic, partition_id) = split_replica_key(&m.name);
    json!({
        "topic": topic,
        "partitionId": partition_id,
        "leader": m.spec.leader,
        "replicas": m.spec.replicas,
        "isr": m.status.replicas.iter().map(|r| r.spu).collect::<Vec<_>>(),
        "status": format!("{:?}", m.status.resolution),
        "highWatermark": m.status.leader.hw,
        "logEndOffset": m.status.leader.leo,
        "baseOffset": m.status.base_offset,
        "size": m.status.size,
    })
}

#[no_mangle]
pub extern "C" fn ffi_admin_list_smartmodules(client: *mut c_void, cancel: *mut c_void, tcb: Tcb) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            let modules = admin.list::<SmartModuleSpec, String>(vec![]).await?;
            let dtos: Vec<_> = modules.into_iter().map(smartmodule_to_json).collect();
            Ok::<_, anyhow::Error>(json!(dtos).to_string())
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(json)) => unsafe { complete_string_success(tcb, json) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_admin_get_smartmodule(client: *mut c_void, name: *const u8, name_len: usize, cancel: *mut c_void, tcb: Tcb) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            let modules = admin.list::<SmartModuleSpec, String>(vec![name]).await?;
            Ok::<_, anyhow::Error>(modules.into_iter().next().map(|m| smartmodule_to_json(m).to_string()))
        };
        match unsafe { crate::cancel::race(cancel_addr, work).await } {
            Ok(Ok(Some(json))) => unsafe { complete_string_success(tcb, json) },
            Ok(Ok(None)) => unsafe { complete_success(tcb, std::ptr::null_mut()) },
            Ok(Err(e)) => unsafe { complete_error(tcb, e) },
            Err(crate::cancel::Cancelled) => unsafe {
                complete_failure(tcb, crate::error::codes::CANCELLED, "cancelled".to_string())
            },
        }
    });
}

fn smartmodule_to_json(m: Metadata<SmartModuleSpec>) -> serde_json::Value {
    json!({
        "name": m.name,
        "fqdn": m.spec.meta.as_ref().map(|meta| meta.package.fqdn()),
        "wasmSize": m.spec.wasm.payload.len() as u32,
    })
}

#[no_mangle]
pub extern "C" fn ffi_admin_create_smartmodule(
    client: *mut c_void,
    name: *const u8, name_len: usize,
    wasm: *const u8, wasm_len: usize,
    cancel: *mut c_void,
    tcb: Tcb,
) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    let wasm_bytes = unsafe { std::slice::from_raw_parts(wasm, wasm_len) }.to_vec();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            let spec = SmartModuleSpec {
                meta: None,
                summary: None,
                wasm: SmartModuleWasm::from_raw_wasm_bytes(&wasm_bytes)?,
            };
            admin.create(name, false, spec).await?;
            Ok::<_, anyhow::Error>(())
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

#[no_mangle]
pub extern "C" fn ffi_admin_delete_smartmodule(client: *mut c_void, name: *const u8, name_len: usize, cancel: *mut c_void, tcb: Tcb) {
    let client_addr = client as usize;
    let cancel_addr = cancel as usize;
    let name = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(name, name_len) }).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let work = async {
            let admin = admin_for(client).await?;
            admin.delete::<SmartModuleSpec>(name).await?;
            Ok::<_, anyhow::Error>(())
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
