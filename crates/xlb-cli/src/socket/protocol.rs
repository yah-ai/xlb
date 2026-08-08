use serde::{Deserialize, Serialize};

/// Commands sent client → server over the control socket.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    Inspect,
    SubscribeEvents,
    ClassStats {
        class: String,
    },
    Fetch {
        class: String,
        hash: String,
        out: Option<String>,
    },
    SetRole {
        class: String,
        role: String,
    },
    SetBandwidth {
        class: String,
        upload_kbps: Option<u32>,
        download_kbps: Option<u32>,
    },
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
    FetchStarted {
        class: String,
        hash: String,
    },
    FetchCompleted {
        class: String,
        hash: String,
        bytes: u64,
        tier: String,
        elapsed_ms: u64,
    },
    FetchFailed {
        class: String,
        hash: String,
        reason: String,
    },
    PeerJoined {
        class: String,
        node_id: String,
    },
    PeerLeft {
        class: String,
        node_id: String,
    },
    GovernorChanged {
        class: String,
        is_passive: bool,
    },
    /// R423-T7 — the per-fetch metric, one per completed fetch, hit or miss.
    ///
    /// Separate from [`NodeEvent::FetchCompleted`] rather than an extension of
    /// it, for two reasons that both bite if they are merged. `FetchCompleted`
    /// fires only for fetches driven over the control socket and only on
    /// success, so it can never carry a hit RATE; and it is an existing wire
    /// shape with consumers. This one fires for every fetch the node performs
    /// through any path, and reports misses.
    FetchMetric(FetchMetric),
}

/// One completed fetch as a metric row — the R423-T7 tuple, plus the reporting
/// node's own identity and cohort.
///
/// This is also exactly the shape written to `node.metrics_path` as NDJSON, so
/// a collector tailing the file and one subscribed to the socket parse the same
/// bytes. Keep the two in lockstep: a metric that means different things on two
/// transports is worse than one transport.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchMetric {
    /// Unix seconds when the fetch completed.
    pub timestamp_secs: u64,
    /// The reporting node's own `NodeId` — not the peer's.
    pub node_id: String,
    /// Reporting node's cohort (`yah-castle` / `home-server` / `desktop` /
    /// `unknown`), from `node.peer_class`. Without it a dashboard cannot
    /// separate volatile desktop seeds from stable ones (R489-F3).
    pub peer_class: String,
    pub class: String,
    /// Full blake3 hex — not shortened. A truncated hash cannot be joined
    /// against a release manifest, which is the main thing anyone will want to
    /// do with these rows.
    pub blake3: String,
    /// `cache` | `lan` | `swarm` | `seed` | `cdn` | `miss`.
    pub tier_source: String,
    pub bytes_served: u64,
    /// `NodeId` of the peer that served it; absent for cache, CDN and misses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    pub duration_ms: u64,
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
