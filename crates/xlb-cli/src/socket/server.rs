use std::os::unix::fs::PermissionsExt;
use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::Result;
use tokio::{net::UnixListener, sync::broadcast};
use xlb::AssetClass;

use super::{protocol::*, read_frame, write_frame};

/// Shared state owned by the serve daemon, cloned into each connection task.
#[derive(Clone)]
pub struct NodeState {
    pub node_id: String,
    pub started_at: Instant,
    pub classes: Arc<HashMap<String, AssetClass>>,
    /// Class metadata from config (for fields not yet exposed by AssetClass).
    pub class_meta: Arc<HashMap<String, ClassMeta>>,
    pub event_tx: broadcast::Sender<NodeEvent>,
    /// R423-T6: the same sink the classes observe through, kept here so
    /// `ClassStats` can report real fetch counts and a recent-fetch tail.
    /// Sharing one instance is load-bearing — a second sink would count a
    /// different set of fetches from the one the classes actually emit to.
    pub metrics: Arc<crate::metrics_sink::MetricsSink>,
}

/// Config-derived class metadata stored alongside the live AssetClass.
#[derive(Clone)]
pub struct ClassMeta {
    pub role: String,
    pub cdn_fallback: Option<String>,
    pub permanent_seeds: Vec<String>,
    pub cache_budget_bytes: u64,
}

impl NodeState {
    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn node_info(&self) -> NodeInfo {
        NodeInfo {
            node_id: self.node_id.clone(),
            uptime_secs: self.uptime_secs(),
            classes: self
                .classes
                .keys()
                .map(|name| {
                    let meta = self.class_meta.get(name);
                    ClassInfo {
                        name: name.clone(),
                        role: meta
                            .map(|m| m.role.clone())
                            .unwrap_or_else(|| "participant".into()),
                        cdn_fallback: meta.and_then(|m| m.cdn_fallback.clone()),
                        permanent_seeds: meta
                            .map(|m| m.permanent_seeds.clone())
                            .unwrap_or_default(),
                    }
                })
                .collect(),
        }
    }
}

pub async fn listen(state: NodeState, socket_path: &str) -> Result<()> {
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    // Restrict the socket to owner-only (0600) so no other local user can
    // connect to the control plane, independent of the peer-uid check below.
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))?;
    tracing::info!(socket = socket_path, "xlb control socket listening");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(state, stream).await {
                        tracing::debug!("client closed: {e}");
                    }
                });
            }
            Err(e) => tracing::error!("accept: {e}"),
        }
    }
}

async fn handle_conn(state: NodeState, mut stream: tokio::net::UnixStream) -> Result<()> {
    // Authenticate the peer by uid: the control socket only serves the daemon's
    // own user. SO_PEERCRED is kernel-supplied and cannot be spoofed.
    let our_uid = unsafe { libc::getuid() };
    match stream.peer_cred() {
        Ok(cred) if cred.uid() == our_uid => {}
        Ok(cred) => {
            tracing::warn!(
                peer_uid = cred.uid(),
                our_uid,
                "rejecting control-socket connection from foreign uid"
            );
            return Ok(());
        }
        Err(e) => {
            tracing::warn!("rejecting control-socket connection: peer_cred failed: {e}");
            return Ok(());
        }
    }

    let (mut reader, mut writer) = stream.split();

    loop {
        let cmd: Command = match read_frame(&mut reader).await {
            Ok(c) => c,
            Err(_) => break,
        };

        match cmd {
            Command::Inspect => {
                let info = state.node_info();
                write_frame(&mut writer, &Response::NodeInfo(info)).await?;
            }

            Command::ClassStats { class } => {
                let stats = build_stats(&state, &class).await;
                write_frame(&mut writer, &Response::ClassStats(stats)).await?;
            }

            Command::SubscribeEvents => {
                let mut rx = state.event_tx.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(ev) => {
                            if write_frame(&mut writer, &Response::Event(ev))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
                break;
            }

            Command::Fetch { class, hash, out } => {
                let resp = do_fetch(&state, &class, &hash, out.as_deref()).await;
                write_frame(&mut writer, &resp).await?;
            }

            Command::SetRole { class: _, role: _ } => {
                write_frame(&mut writer, &Response::Ok).await?;
            }

            Command::SetBandwidth {
                class: _,
                upload_kbps: _,
                download_kbps: _,
            } => {
                write_frame(&mut writer, &Response::Ok).await?;
            }
        }
    }

    Ok(())
}

/// How many recent rows one `ClassStats` reply carries. Small: this is the
/// "what just happened" strip on an operator card, not a log.
const RECENT_IN_REPLY: usize = 20;

async fn build_stats(state: &NodeState, class_name: &str) -> ClassStats {
    let meta = state.class_meta.get(class_name);
    let ac = state.classes.get(class_name);
    let gov_state = ac.map(|ac| {
        let gov = ac.governor();
        GovernorState {
            on_battery: gov.is_on_battery(),
            metered: gov.is_metered(),
            is_passive: gov.is_passive(),
        }
    });

    // R423-T6: the cache reading comes from the class itself, so it is the
    // same number eviction is measured against. Falls back to the configured
    // budget only when the class isn't registered (an unknown class name),
    // where every other field is a fallback too.
    let cache = match ac {
        Some(ac) => Some(ac.cache_stats().await),
        None => None,
    };

    let totals = state.metrics.totals_for(class_name);
    let recent_fetches = state
        .metrics
        .recent_for(class_name, RECENT_IN_REPLY)
        .into_iter()
        .map(|m| FetchRecord {
            timestamp_secs: m.timestamp_secs,
            class: m.class,
            hash_short: m.blake3.chars().take(12).collect(),
            bytes: m.bytes_served,
            tier: m.tier_source.clone(),
            ok: m.tier_source != "miss",
            note: None,
            peer_id: m.peer_id,
        })
        .collect();

    ClassStats {
        name: class_name.to_string(),
        role: meta
            .map(|m| m.role.clone())
            .unwrap_or_else(|| "unknown".into()),
        peer_count: 0,
        cache_bytes: cache.map(|c| c.bytes).unwrap_or(0),
        cache_entries: cache.map(|c| c.entries).unwrap_or(0),
        cache_budget_bytes: cache
            .map(|c| c.budget_bytes)
            .or_else(|| meta.map(|m| m.cache_budget_bytes))
            .unwrap_or(0),
        cache_disk_backed: cache.map(|c| c.disk_backed).unwrap_or(false),
        upload_kbps: 0.0,
        download_kbps: 0.0,
        governor: gov_state.unwrap_or(GovernorState {
            on_battery: false,
            metered: false,
            is_passive: false,
        }),
        fetches_total: totals.fetches,
        fetches_hit: totals.hits,
        bytes_served_total: totals.bytes_served,
        recent_fetches,
    }
}

async fn do_fetch(
    state: &NodeState,
    class_name: &str,
    hash_str: &str,
    out: Option<&str>,
) -> Response {
    use std::time::Instant;
    use xlb::BlakeHash;

    let Some(ac) = state.classes.get(class_name) else {
        return Response::Error {
            message: format!("unknown class: {class_name}"),
        };
    };

    let hash = match BlakeHash::from_hex(hash_str) {
        Ok(h) => h,
        Err(e) => {
            return Response::Error {
                message: e.to_string(),
            }
        }
    };

    let _ = state.event_tx.send(NodeEvent::FetchStarted {
        class: class_name.to_string(),
        hash: hash_str.to_string(),
    });

    let start = Instant::now();
    // R423-T7: `fetch_reported` instead of `fetch`, so `tier` below is the tier
    // that ACTUALLY served the blob. It used to be the constant string
    // "fetched", which is the same amount of information as no field at all —
    // and it was the field a hit-rate dashboard would have been built on.
    let (result, report) = ac.asset(hash).fetch_reported().await;
    let tier = report.tier_label().to_string();
    match result {
        Ok(bytes) => {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            let saved_to = if let Some(path) = out {
                match std::fs::write(path, &bytes[..]) {
                    Ok(()) => Some(path.to_string()),
                    Err(e) => {
                        return Response::Error {
                            message: format!("save to {path}: {e}"),
                        }
                    }
                }
            } else {
                None
            };

            let _ = state.event_tx.send(NodeEvent::FetchCompleted {
                class: class_name.to_string(),
                hash: hash_str.to_string(),
                bytes: bytes.len() as u64,
                tier: tier.clone(),
                elapsed_ms,
            });

            Response::FetchResult(FetchResult {
                class: class_name.to_string(),
                hash: hash_str.to_string(),
                bytes: bytes.len() as u64,
                tier,
                elapsed_ms,
                saved_to,
            })
        }
        Err(e) => {
            let _ = state.event_tx.send(NodeEvent::FetchFailed {
                class: class_name.to_string(),
                hash: hash_str.to_string(),
                reason: e.to_string(),
            });
            Response::Error {
                message: e.to_string(),
            }
        }
    }
}
