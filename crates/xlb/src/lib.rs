//! # xlb — chunked, Bao-verified blob distribution
//!
//! Reads as **peer-eXchange + LAN + Bao-tree**. Mnemonic: each blob is a
//! xiaolongbao — content sealed in a verified wrapper, served from a kitchen,
//! shareable across tables.
//!
//! Multi-source concurrent fetch (local cache → LAN peer → swarm peer →
//! permanent seed → CDN edge), with per-chunk BLAKE3 Bao-tree verification so
//! chunks pulled from different sources are independently verifiable.
//!
//! **Status: xlb-4.** Core types + iroh-blobs transport (xlb-2) + HTTP CDN
//! fallback + Bao verifier glue (xlb-3) + BandwidthGovernor with
//! battery/metered auto-detection (xlb-4). Full five-tier fetch chain wired
//! per `.yah/docs/architecture/xlb.md`.

pub mod bandwidth;
pub(crate) mod cache;
pub(crate) mod source;
pub(crate) mod verify;
pub mod testing;
pub mod transport;

pub use bandwidth::BandwidthGovernor;
pub use source::{FetchProgress, FetchTier, ProgressSink};
pub(crate) use source::BlobSource;

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
};
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::cache::Cache;

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid hash: {0}")]
    InvalidHash(String),

    #[error("all sources exhausted for {0}")]
    FetchFailed(BlakeHash),

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        expected: BlakeHash,
        actual: BlakeHash,
    },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// ─── BlakeHash ────────────────────────────────────────────────────────────────

/// A BLAKE3 content-hash — the stable identity of an xlb asset.
///
/// All arithmetic in xlb is over these hashes; no integer IDs or paths are
/// canonical.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlakeHash([u8; 32]);

impl BlakeHash {
    /// Compute the BLAKE3 hash of `data`.
    pub fn hash(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_hex(s: &str) -> Result<Self> {
        let raw =
            hex::decode(s).map_err(|_| Error::InvalidHash(s.to_string()))?;
        let arr: [u8; 32] = raw
            .try_into()
            .map_err(|_| Error::InvalidHash(format!("expected 32 bytes: {s}")))?;
        Ok(Self(arr))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns `true` when `data` hashes to this value.
    pub fn verify(&self, data: &[u8]) -> bool {
        *self == Self::hash(data)
    }
}

impl std::fmt::Debug for BlakeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlakeHash({}…)", &self.to_hex()[..8])
    }
}

impl std::fmt::Display for BlakeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl std::str::FromStr for BlakeHash {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::from_hex(s)
    }
}

// ─── Policy types ─────────────────────────────────────────────────────────────

/// The role this process plays for an `AssetClass`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedRole {
    /// Fetch only; never serve bytes to remote peers.
    Passive,
    /// Fetch + seed under the class's per-tier bandwidth cap.
    Participant,
    /// Fetch + seed without caps; always-on. Used by yah-cloud permanent seeds.
    Permanent,
}

/// Coarse label used by `BandwidthPolicy` to bucket peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerTier {
    Cloud,
    Camp,
    Workstation,
    Mobile,
}

/// Upload/download bandwidth caps for one tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BwCaps {
    pub up_mbit: u32,
    pub down_mbit: u32,
}

impl BwCaps {
    /// Passive peers fetch but never upload.
    pub fn passive() -> Self {
        Self { up_mbit: 0, down_mbit: 50 }
    }
}

/// Per-class bandwidth policy: caps keyed by `PeerTier`.
///
/// ```
/// use xlb::{BandwidthPolicy, BwCaps, PeerTier};
///
/// let policy = BandwidthPolicy::default()
///     .role(PeerTier::Cloud,       BwCaps { up_mbit: 1000, down_mbit: 10_000 })
///     .role(PeerTier::Camp,         BwCaps { up_mbit: 10,   down_mbit: 100 })
///     .role(PeerTier::Workstation, BwCaps { up_mbit: 5,    down_mbit: 50 })
///     .role(PeerTier::Mobile,      BwCaps::passive());
/// ```
#[derive(Debug, Clone, Default)]
pub struct BandwidthPolicy {
    roles: HashMap<PeerTier, BwCaps>,
}

impl BandwidthPolicy {
    pub fn role(mut self, tier: PeerTier, caps: BwCaps) -> Self {
        self.roles.insert(tier, caps);
        self
    }

    pub fn caps_for(&self, tier: PeerTier) -> BwCaps {
        self.roles.get(&tier).copied().unwrap_or_else(BwCaps::passive)
    }
}

// ─── Discovery ────────────────────────────────────────────────────────────────

/// Which discovery mechanisms to enable for an `AssetClass`.
///
/// xlb-2 wires in mDNS and iroh-relay discovery; xlb-1 only has Static
/// (permanent seeds resolved via the class config).
#[derive(Debug, Clone)]
pub struct Discovery {
    /// Announce and discover peers via mDNS on the local subnet.
    pub lan: bool,
    /// Announce and discover peers through iroh relay servers.
    pub swarm: bool,
    pub relays: Vec<String>,
}

impl Default for Discovery {
    fn default() -> Self {
        Self { lan: true, swarm: true, relays: vec![] }
    }
}

impl Discovery {
    pub fn with_relays(
        mut self,
        relays: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.relays = relays.into_iter().map(Into::into).collect();
        self
    }

    pub fn lan_only() -> Self {
        Self { lan: true, swarm: false, relays: vec![] }
    }

    pub fn none() -> Self {
        Self { lan: false, swarm: false, relays: vec![] }
    }
}

// ─── AssetClassConfig ─────────────────────────────────────────────────────────

/// Configuration for an `AssetClass`.
pub struct AssetClassConfig {
    /// Stable name for this class. Used as the swarm topic and log identifier.
    pub name: &'static str,
    /// Pinned `NodeId`s of always-on seed servers (string form; resolved in xlb-2).
    pub permanent_seeds: Vec<String>,
    /// URL template for the CDN fallback; `{blake3}` is substituted per-asset.
    pub cdn_fallback: Option<String>,
    pub discovery: Discovery,
    pub bandwidth: BandwidthPolicy,
    /// Base directory for the local asset cache. `None` = in-memory only.
    pub cache_dir: Option<PathBuf>,
    /// LRU eviction budget for cached assets, in bytes.
    pub cache_budget_bytes: u64,
}

impl Default for AssetClassConfig {
    fn default() -> Self {
        Self {
            name: "default",
            permanent_seeds: vec![],
            cdn_fallback: None,
            discovery: Discovery::default(),
            bandwidth: BandwidthPolicy::default(),
            cache_dir: None,
            cache_budget_bytes: 1024 * 1024 * 1024,
        }
    }
}

// ─── AssetClass ───────────────────────────────────────────────────────────────

struct ClassInner {
    config: AssetClassConfig,
    /// `FetchTier::Cache` — in-memory when `config.cache_dir` is `None`,
    /// disk-backed with LRU eviction (gated on `config.cache_budget_bytes`)
    /// otherwise. Disk-backed entries survive process restart.
    cache: Cache,
    /// Fetch sources. Rebuilt into a `FetchChain` at first fetch; subsequent
    /// sources added via `add_source` are appended and the chain rebuilt.
    sources: RwLock<Vec<Arc<dyn BlobSource>>>,
    role: RwLock<(SeedRole, PeerTier)>,
    /// Bandwidth governor — enforces per-tier caps and auto-governors
    /// (battery/metered). Updated via `set_governor_*` and `probe_os`.
    governor: BandwidthGovernor,
}

/// A registered asset class — a per-app namespace for content-addressed blobs.
///
/// `AssetClass` is `Clone + Send + Sync`; clone it freely to share across tasks.
/// Drop the last clone to stop seeding gracefully (xlb-2+).
#[derive(Clone)]
pub struct AssetClass(Arc<ClassInner>);

impl AssetClass {
    /// Register a new asset class.
    ///
    /// When `config.cdn_fallback` is `Some`, an [`HttpFetcher`] is
    /// automatically wired in as the `FetchTier::Cdn` source. The iroh-blobs
    /// transport (tiers 2–4) is attached separately via
    /// [`BlobTransport::attach_fetcher`].
    ///
    /// A [`BandwidthGovernor`] is built from `config.bandwidth` and probed
    /// against the OS power source at startup (xlb-4).
    pub async fn register(config: AssetClassConfig) -> Result<Self> {
        // Pre-build CDN source before moving config into ClassInner.
        let cdn: Option<Arc<dyn BlobSource>> = if let Some(url) = &config.cdn_fallback {
            match transport::http::HttpFetcher::new(url.as_str()) {
                Ok(f) => Some(Arc::new(f)),
                Err(e) => {
                    tracing::warn!(url = %url, "HttpFetcher init failed: {e}");
                    None
                }
            }
        } else {
            None
        };

        let initial_sources: Vec<Arc<dyn BlobSource>> = cdn.into_iter().collect();

        // Build and probe the bandwidth governor.
        let governor = BandwidthGovernor::new(config.bandwidth.clone());
        governor.probe_os();

        // Build the cache layer. `cache_dir = None` → in-memory; `Some` →
        // disk-backed at the given path with LRU eviction gated on
        // `cache_budget_bytes`. The latter survives process restart, which
        // is the whole reason this layer exists (see W160 F3).
        let cache = Cache::new(
            config.cache_dir.as_deref(),
            config.cache_budget_bytes,
        )?;

        Ok(Self(Arc::new(ClassInner {
            config,
            cache,
            sources: RwLock::new(initial_sources),
            role: RwLock::new((SeedRole::Participant, PeerTier::Workstation)),
            governor,
        })))
    }

    /// Update this peer's role and tier for the class.
    pub async fn set_role(&self, role: SeedRole, tier: PeerTier) -> Result<()> {
        *self.0.role.write().await = (role, tier);
        Ok(())
    }

    /// Return a handle to a specific asset within this class.
    pub fn asset(&self, hash: BlakeHash) -> Asset {
        Asset { class: self.clone(), hash }
    }

    /// The configured name of this class.
    pub fn name(&self) -> &str {
        self.0.config.name
    }

    /// Borrow the configured permanent-seed list.
    ///
    /// Each entry is a hex-encoded `NodeId`; parse with
    /// [`transport::blobs::parse_seed_node_id`]. Used by
    /// [`transport::BlobTransport::attach_permanent_seeds`] to wire
    /// `FetchTier::Seed` sources at boot.
    pub(crate) fn permanent_seeds(&self) -> &[String] {
        &self.0.config.permanent_seeds
    }

    /// Borrow the bandwidth governor for this class.
    ///
    /// Use [`BandwidthGovernor::effective_caps`] to query per-tier caps
    /// (already adjusted for battery/metered state).
    pub fn governor(&self) -> &BandwidthGovernor {
        &self.0.governor
    }

    /// Notify the governor that battery state changed.
    ///
    /// Call from OS power-event hooks (e.g. `NSWorkspaceDidChangeNotification`
    /// on macOS) to keep caps updated in real time.
    pub fn set_battery(&self, on_battery: bool) {
        self.0.governor.set_battery(on_battery);
    }

    /// Notify the governor that metered-connection state changed.
    pub fn set_metered(&self, metered: bool) {
        self.0.governor.set_metered(metered);
    }

    /// Add a fetch source to the chain (e.g. a transport adapter or mock peer).
    ///
    /// Sources are tried in `FetchTier` priority order regardless of insertion
    /// order. Safe to call after the class is registered.
    pub(crate) fn add_source(&self, source: Arc<dyn BlobSource>) {
        if let Ok(mut sources) = self.0.sources.try_write() {
            sources.push(source);
        } else {
            tracing::warn!(class = self.name(), "add_source: lock contended, source dropped");
        }
    }

    /// Internal: run the fetch chain for `hash`.
    pub(crate) async fn fetch_bytes(&self, hash: BlakeHash) -> Result<Bytes> {
        self.fetch_bytes_with_progress(hash, None).await
    }

    /// Internal: run the fetch chain for `hash`, reporting byte progress.
    ///
    /// Identical to [`fetch_bytes`](Self::fetch_bytes) but threads `sink`
    /// down to the serving source. A cache hit returns without ever invoking
    /// `sink` (instant, no transfer to surface).
    pub(crate) async fn fetch_bytes_with_progress(
        &self,
        hash: BlakeHash,
        sink: Option<ProgressSink>,
    ) -> Result<Bytes> {
        // 1. Local cache — fast path. Disk-backed cache re-verifies BLAKE3
        //    on read; mem-backed is a HashMap lookup.
        if let Some(bytes) = self.0.cache.get(&hash).await {
            tracing::trace!(%hash, "cache hit");
            return Ok(bytes);
        }

        // 2. Clone source handles so we can release the lock before awaiting.
        let sources: Vec<Arc<dyn BlobSource>> =
            self.0.sources.read().await.clone();

        let chain = source::FetchChain::new(sources);
        if let Some((_tier, bytes)) = chain.fetch_with_progress(&hash, sink.as_ref()).await {
            // Cache write failures are non-fatal: the fetched bytes are
            // verified, so we can still return them. A disk-full error
            // means future restarts won't have this entry, but the current
            // request succeeds.
            if let Err(e) = self.0.cache.put(hash, bytes.clone()).await {
                tracing::warn!(%hash, "cache write failed: {e}");
            }
            return Ok(bytes);
        }

        Err(Error::FetchFailed(hash))
    }
}

// ─── Asset ────────────────────────────────────────────────────────────────────

/// A handle to a specific content-addressed blob within an `AssetClass`.
#[derive(Clone)]
pub struct Asset {
    class: AssetClass,
    hash: BlakeHash,
}

impl Asset {
    /// Fetch the blob, trying all sources in priority order and verifying BLAKE3.
    ///
    /// Subsequent calls return the cached copy without hitting any source.
    pub async fn fetch(&self) -> Result<Bytes> {
        self.class.fetch_bytes(self.hash).await
    }

    /// Fetch the blob, reporting incremental byte progress to `sink`.
    ///
    /// Identical to [`fetch`](Self::fetch) but the CDN tier emits per-chunk
    /// [`FetchProgress`] against `Content-Length`. Instant tiers (cache)
    /// return without ever invoking `sink`.
    pub async fn fetch_with_progress(&self, sink: ProgressSink) -> Result<Bytes> {
        self.class.fetch_bytes_with_progress(self.hash, Some(sink)).await
    }

    /// Returns `true` if this blob is in the local cache (in-memory or
    /// disk-backed). Disk-backed `is_cached` is a presence check against the
    /// LRU index — it does *not* re-verify BLAKE3. Use [`Asset::fetch`] when
    /// you need verified bytes.
    pub async fn is_cached(&self) -> bool {
        self.class.0.cache.contains(&self.hash).await
    }

    pub fn hash(&self) -> BlakeHash {
        self.hash
    }
}
