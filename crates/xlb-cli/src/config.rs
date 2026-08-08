use serde::{Deserialize, Serialize};

/// Top-level structure matching `xlb-node.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node: NodeSettings,
    pub classes: Vec<ClassConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSettings {
    #[serde(default = "default_socket")]
    pub socket: String,
    #[serde(default = "default_log")]
    pub log: String,
    /// Which COHORT of seed this node belongs to (R423-T7).
    ///
    /// R489-F3 makes every desktop install an in-process seed, so the seed
    /// population stops being "two stable boxes" and becomes two stable boxes
    /// plus a volatile desktop fleet. Without this dimension a dashboard cannot
    /// separate the two, and W160's hit-rate model cannot be validated per
    /// cohort — a swarm hit rate that mixes an always-on Hetzner node with
    /// laptops that are shut half the day describes neither.
    ///
    /// Free-form on purpose: this is a reporting label, not a protocol value,
    /// and a closed enum here would need a release to add a cohort. Convention
    /// so far: `yah-castle`, `home-server`, `desktop`.
    #[serde(default = "default_peer_class")]
    pub peer_class: String,
    /// Where to append newline-delimited [`xlb::FetchReport`] JSON.
    ///
    /// `null` — the default — means no file sink; reports still reach live
    /// `SubscribeEvents` subscribers over the control socket. NDJSON to a path
    /// is deliberately the dumbest possible sink: every collector can tail it,
    /// and it commits this crate to no vendor.
    #[serde(default)]
    pub metrics_path: Option<String>,
}

fn default_socket() -> String {
    "/tmp/xlb-node.sock".into()
}
fn default_log() -> String {
    "info".into()
}
/// Deliberately NOT "desktop" or any real cohort: a node whose config forgot to
/// say what it is must show up as unlabelled in the dashboard rather than
/// quietly inflating a cohort it was never part of.
fn default_peer_class() -> String {
    "unknown".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassConfig {
    pub name: String,
    #[serde(default)]
    pub permanent_seeds: Vec<String>,
    pub cdn_fallback: Option<String>,
    /// Local cache directory. `null` = in-memory only.
    #[serde(default)]
    pub cache_dir: Option<String>,
    #[serde(default = "default_cache_budget")]
    pub cache_budget_bytes: u64,
    /// "seed" | "participant" | "passive" — defaults to "participant".
    #[serde(default = "default_role")]
    pub role: String,
    #[serde(default)]
    pub bandwidth: BandwidthConfig,
    #[serde(default)]
    pub discovery: DiscoveryConfig,
}

fn default_cache_budget() -> u64 {
    5 * 1024 * 1024 * 1024
}
fn default_role() -> String {
    "participant".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BandwidthConfig {
    pub max_upload_kbps: Option<u32>,
    pub max_download_kbps: Option<u32>,
    #[serde(default = "default_auto")]
    pub battery_mode: String,
    #[serde(default = "default_auto")]
    pub metered_mode: String,
}

fn default_auto() -> String {
    "auto".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    #[serde(default = "yes")]
    pub lan: bool,
    #[serde(default = "yes")]
    pub swarm: bool,
    #[serde(default)]
    pub relays: Vec<String>,
}

fn yes() -> bool {
    true
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            lan: true,
            swarm: true,
            relays: vec![],
        }
    }
}

impl NodeConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read {path}: {e}"))?;
        let cfg: Self =
            serde_json::from_str(&content).map_err(|e| anyhow::anyhow!("{path}: {e}"))?;
        Ok(cfg)
    }
}

/// Expand a leading `~/` to the home directory.
pub fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return format!("{}/{rest}", home.to_string_lossy());
        }
    }
    path.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> NodeConfig {
        serde_json::from_str(json).expect("config parses")
    }

    const MINIMAL: &str = r#"{
      "node": { "socket": "/tmp/s.sock", "log": "info" },
      "classes": []
    }"#;

    #[test]
    fn a_config_predating_the_metrics_fields_still_loads() {
        // Every deployed xlb-node.json was written before R423-T7. None of them
        // may need editing to keep booting.
        let cfg = parse(MINIMAL);
        assert_eq!(cfg.node.socket, "/tmp/s.sock");
        assert!(cfg.node.metrics_path.is_none(), "file sink off by default");
    }

    #[test]
    fn an_unlabelled_node_reports_as_unknown_not_as_a_real_cohort() {
        // Defaulting to "desktop" (or any real cohort) would silently inflate
        // that cohort's readings with every misconfigured node.
        assert_eq!(parse(MINIMAL).node.peer_class, "unknown");
    }

    #[test]
    fn the_metrics_fields_round_trip_when_present() {
        let cfg = parse(
            r#"{
              "node": {
                "socket": "/var/lib/xlb/xlb.sock",
                "log": "info",
                "peer_class": "yah-castle",
                "metrics_path": "/var/lib/xlb/metrics/fetches.ndjson"
              },
              "classes": []
            }"#,
        );
        assert_eq!(cfg.node.peer_class, "yah-castle");
        assert_eq!(
            cfg.node.metrics_path.as_deref(),
            Some("/var/lib/xlb/metrics/fetches.ndjson")
        );
    }

    #[test]
    fn underscore_comment_keys_are_ignored() {
        // The reference config carries `_comment` / `_comment_metrics` prose.
        // Rejecting unknown keys would make the documentation break the daemon.
        let cfg = parse(
            r#"{
              "_comment": "prose",
              "_comment_metrics": "more prose",
              "node": { "socket": "/tmp/s.sock", "log": "info" },
              "classes": []
            }"#,
        );
        assert_eq!(cfg.node.socket, "/tmp/s.sock");
    }
}
