use serde::{Deserialize, Serialize};

/// Commands sent client → server over the control socket.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    Inspect,
    SubscribeEvents,
    ClassStats { class: String },
    Fetch { class: String, hash: String, out: Option<String> },
    SetRole { class: String, role: String },
    SetBandwidth { class: String, upload_kbps: Option<u32>, download_kbps: Option<u32> },
}

/// Responses and streamed events sent server → client.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    NodeInfo(NodeInfo),
    ClassStats(ClassStats),
    Event(NodeEvent),
    FetchResult(FetchResult),
    Ok,
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub uptime_secs: u64,
    pub classes: Vec<ClassInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassInfo {
    pub name: String,
    pub role: String,
    pub cdn_fallback: Option<String>,
    pub permanent_seeds: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassStats {
    pub name: String,
    pub role: String,
    pub peer_count: usize,
    pub cache_bytes: u64,
    pub cache_budget_bytes: u64,
    pub upload_kbps: f64,
    pub download_kbps: f64,
    pub governor: GovernorState,
    pub recent_fetches: Vec<FetchRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovernorState {
    pub on_battery: bool,
    pub metered: bool,
    pub is_passive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRecord {
    pub timestamp_secs: u64,
    pub class: String,
    pub hash_short: String,
    pub bytes: u64,
    pub tier: String,
    pub ok: bool,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum NodeEvent {
    FetchStarted { class: String, hash: String },
    FetchCompleted { class: String, hash: String, bytes: u64, tier: String, elapsed_ms: u64 },
    FetchFailed { class: String, hash: String, reason: String },
    PeerJoined { class: String, node_id: String },
    PeerLeft { class: String, node_id: String },
    GovernorChanged { class: String, is_passive: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResult {
    pub class: String,
    pub hash: String,
    pub bytes: u64,
    pub tier: String,
    pub elapsed_ms: u64,
    pub saved_to: Option<String>,
}
