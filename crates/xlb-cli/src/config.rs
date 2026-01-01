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
}

fn default_socket() -> String {
    "/tmp/xlb-node.sock".into()
}
fn default_log() -> String {
    "info".into()
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
        Self { lan: true, swarm: true, relays: vec![] }
    }
}

impl NodeConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read {path}: {e}"))?;
        let cfg: Self = serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("{path}: {e}"))?;
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
