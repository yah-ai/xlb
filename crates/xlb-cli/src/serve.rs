use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

use anyhow::Result;
use mshr::Keypair;
use tokio::sync::broadcast;
use xlb::{AssetClass, AssetClassConfig, Discovery};

use crate::{
    config::{expand_tilde, NodeConfig},
    metrics_sink::MetricsSink,
    socket::server::{ClassMeta, NodeState},
};

pub async fn run(config_path: &str, socket_path: &str) -> Result<()> {
    let cfg = NodeConfig::load(config_path)?;

    // Override socket path from config if not overridden by CLI flag.
    // (CLI --socket takes precedence; we just use socket_path as passed.)
    let effective_socket =
        if socket_path == "/tmp/xlb-node.sock" && cfg.node.socket != "/tmp/xlb-node.sock" {
            cfg.node.socket.as_str()
        } else {
            socket_path
        };

    let kp = Keypair::load_or_create().map_err(|e| anyhow::anyhow!("keypair: {e}"))?;
    let node_id = kp.node_id().to_string();

    let mut classes: HashMap<String, AssetClass> = HashMap::new();
    let mut class_meta: HashMap<String, ClassMeta> = HashMap::new();

    // R423-T7. Built BEFORE the classes because every class's observer is a
    // handle to it — an AssetClass registered without one reports nothing, for
    // the whole life of the process, silently.
    let (event_tx, _) = broadcast::channel(256);
    // R423-T6: one sink, shared by the classes (as their observer) and by the
    // socket server (as the source for ClassStats counters). Two sinks would
    // each see half the picture.
    let metrics = Arc::new(MetricsSink::new(
        node_id.clone(),
        cfg.node.peer_class.clone(),
        event_tx.clone(),
        cfg.node.metrics_path.as_deref(),
    )?);

    for cc in &cfg.classes {
        // AssetClassConfig.name is &'static str; leak each name once at startup.
        let static_name: &'static str = Box::leak(cc.name.clone().into_boxed_str());

        let cache_dir = cc.cache_dir.as_deref().map(expand_tilde).map(PathBuf::from);

        let discovery = if !cc.discovery.lan && !cc.discovery.swarm {
            Discovery::none()
        } else if cc.discovery.swarm {
            Discovery::default().with_relays(cc.discovery.relays.clone())
        } else {
            Discovery::lan_only()
        };

        let ac = AssetClass::register(AssetClassConfig {
            name: static_name,
            permanent_seeds: cc.permanent_seeds.clone(),
            cdn_fallback: cc.cdn_fallback.clone(),
            discovery,
            cache_dir,
            cache_budget_bytes: cc.cache_budget_bytes,
            observer: Some(metrics.observer()),
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("register class {}: {e}", cc.name))?;

        class_meta.insert(
            cc.name.clone(),
            ClassMeta {
                role: cc.role.clone(),
                cdn_fallback: cc.cdn_fallback.clone(),
                permanent_seeds: cc.permanent_seeds.clone(),
                cache_budget_bytes: cc.cache_budget_bytes,
            },
        );
        classes.insert(cc.name.clone(), ac);
    }

    let state = NodeState {
        node_id: node_id.clone(),
        started_at: Instant::now(),
        classes: Arc::new(classes),
        class_meta: Arc::new(class_meta),
        event_tx,
        metrics,
    };

    let state_clone = state.clone();
    let socket_owned = effective_socket.to_string();
    tokio::spawn(async move {
        if let Err(e) = crate::socket::server::listen(state_clone, &socket_owned).await {
            tracing::error!("socket error: {e}");
        }
    });

    println!(
        "xlb node · {} · {} class(es) · socket: {}",
        &node_id[..node_id.len().min(12)],
        cfg.classes.len(),
        effective_socket
    );
    println!("Ctrl+C to stop");

    tokio::signal::ctrl_c().await?;
    println!("\nshutting down");
    Ok(())
}
