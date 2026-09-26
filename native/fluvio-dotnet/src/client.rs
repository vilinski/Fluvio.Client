// native/fluvio-dotnet/src/client.rs
use crate::tcb::{complete_error, complete_string_success, complete_success, Tcb};
use fluvio::config::{ConfigFile, TlsPolicy};
use fluvio::{Fluvio, FluvioConfig};
use serde::Deserialize;
use std::os::raw::c_void;
use std::time::Instant;

/// JSON shape sent from C#. `fluvio::FluvioConfig` itself does not derive a plain
/// `{endpoint, useTls}` `Deserialize` (its `tls` field is a `TlsPolicy` enum, not a bool),
/// so we deserialize into this small DTO and build the real `FluvioConfig` from it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConnectConfig {
    endpoint: Option<String>,
    profile: Option<String>,
    client_id: Option<String>,
    #[serde(default)]
    use_tls: Option<bool>,
}

#[no_mangle]
pub extern "C" fn ffi_client_connect(config_json: *const u8, config_json_len: usize, tcb: Tcb) {
    let json = unsafe { std::slice::from_raw_parts(config_json, config_json_len) };
    let json = String::from_utf8_lossy(json).into_owned();
    crate::tcb::spawn_guarded(tcb, async move {
        let result: anyhow::Result<Fluvio> = async {
            let connect_config: ConnectConfig = serde_json::from_str(&json)?;
            let mut config = if let Some(profile) = &connect_config.profile {
                ConfigFile::load(None)?
                    .config()
                    .cluster_with_profile(profile)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Fluvio profile '{profile}' not found"))?
            } else if let Some(endpoint) = &connect_config.endpoint {
                FluvioConfig::new(endpoint)
            } else {
                ConfigFile::load(None)?.config().current_cluster()?.clone()
            };
            if let Some(endpoint) = connect_config.endpoint {
                config.endpoint = endpoint;
            }
            match connect_config.use_tls {
                Some(false) => config.tls = TlsPolicy::Disabled,
                Some(true) if matches!(config.tls, TlsPolicy::Disabled) => {
                    config.tls = TlsPolicy::Anonymous;
                }
                _ => {}
            }
            config.client_id = connect_config.client_id;
            let client = Fluvio::connect_with_config(&config).await?;
            Ok(client)
        }
        .await;
        match result {
            Ok(client) => {
                let ptr = Box::into_raw(Box::new(client)) as *mut c_void;
                unsafe { complete_success(tcb, ptr) };
            }
            Err(e) => unsafe { complete_error(tcb, e) },
        }
    });
}

#[no_mangle]
pub extern "C" fn ffi_client_health_check(client: *mut c_void, tcb: Tcb) {
    // Captured as a plain address (rather than the raw pointer) because raw pointers are
    // not `Send`, even though the `Fluvio` value they point to is; the pointer is only
    // ever dereferenced on the Tokio worker thread that runs this spawned task.
    let client_addr = client as usize;
    crate::tcb::spawn_guarded(tcb, async move {
        let client = unsafe { &*(client_addr as *const Fluvio) };
        let start = Instant::now();
        let is_healthy = client.consumer_offsets().await.is_ok();
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let payload = serde_json::json!({
            "isHealthy": is_healthy,
            "elapsedMs": elapsed_ms,
        });
        unsafe { complete_string_success(tcb, payload.to_string()) };
    });
}

/// # Safety
/// `client` must have come from `ffi_client_connect` and not yet been freed.
#[no_mangle]
pub unsafe extern "C" fn ffi_client_drop(client: *mut c_void) {
    if client.is_null() {
        return;
    }
    drop(Box::from_raw(client as *mut Fluvio));
}
