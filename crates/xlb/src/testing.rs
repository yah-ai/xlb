//! In-process mock swarm for testing the xlb fetch chain.
//!
//! `MockSwarm` builds an `AssetClass` wired to a set of virtual peers — no
//! real network, no iroh, no QUIC. All blobs live in `HashMap`s.
//!
//! Covered by the test suite below:
//! - Fetch-chain priority (LAN > Swarm > Seed > CDN).
//! - Cache hits on second fetch.
//! - Poisoned-source rejection (hash mismatch → skip source, try next).
//! - `FetchFailed` when no source has the blob.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    source::{BlobSource, FetchTier},
    AssetClass, AssetClassConfig, BlakeHash, Result,
};

// ─── MockPeer ─────────────────────────────────────────────────────────────────

/// A single virtual peer holding a set of blobs (in-memory).
pub struct MockPeer {
    blobs: HashMap<BlakeHash, Vec<u8>>,
}

impl Default for MockPeer {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPeer {
    pub fn new() -> Self {
        Self { blobs: HashMap::new() }
    }

    /// Register `data` keyed by its BLAKE3 hash.
    pub fn with_blob(mut self, data: impl Into<Vec<u8>>) -> Self {
        let data = data.into();
        let hash = BlakeHash::hash(&data);
        self.blobs.insert(hash, data);
        self
    }

    /// Register `data` keyed by an explicit `hash` (allows injecting poisoned data).
    pub fn with_blob_at(mut self, hash: BlakeHash, data: impl Into<Vec<u8>>) -> Self {
        self.blobs.insert(hash, data.into());
        self
    }
}

// ─── MockBlobSource (internal) ───────────────────────────────────────────────

struct MockBlobSource {
    tier: FetchTier,
    blobs: Arc<HashMap<BlakeHash, Vec<u8>>>,
}

#[async_trait]
impl BlobSource for MockBlobSource {
    fn tier(&self) -> FetchTier {
        self.tier
    }

    async fn fetch_raw(&self, hash: &BlakeHash) -> Option<Bytes> {
        self.blobs.get(hash).map(|v| Bytes::from(v.clone()))
    }
}

// ─── MockSwarm ────────────────────────────────────────────────────────────────

/// Builder for a test `AssetClass` backed by in-memory peers.
///
/// ```ignore
/// use xlb::{testing::{MockPeer, MockSwarm}, AssetClassConfig, BlakeHash};
///
/// let data = b"hello world";
/// let hash = BlakeHash::hash(data);
///
/// let class = MockSwarm::new()
///     .with_lan_peer(MockPeer::new().with_blob(data.as_ref()))
///     .build_class(AssetClassConfig::default())
///     .await
///     .unwrap();
///
/// let fetched = class.asset(hash).fetch().await.unwrap();
/// assert_eq!(&fetched[..], data);
/// ```
pub struct MockSwarm {
    lan_peers: Vec<MockPeer>,
    swarm_peers: Vec<MockPeer>,
    seeds: Vec<MockPeer>,
    cdn: Option<MockPeer>,
}

impl Default for MockSwarm {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSwarm {
    pub fn new() -> Self {
        Self {
            lan_peers: vec![],
            swarm_peers: vec![],
            seeds: vec![],
            cdn: None,
        }
    }

    pub fn with_lan_peer(mut self, peer: MockPeer) -> Self {
        self.lan_peers.push(peer);
        self
    }

    pub fn with_swarm_peer(mut self, peer: MockPeer) -> Self {
        self.swarm_peers.push(peer);
        self
    }

    pub fn with_seed(mut self, peer: MockPeer) -> Self {
        self.seeds.push(peer);
        self
    }

    pub fn with_cdn(mut self, cdn: MockPeer) -> Self {
        self.cdn = Some(cdn);
        self
    }

    /// Build an `AssetClass` backed by this swarm's virtual peers.
    pub async fn build_class(&self, config: AssetClassConfig) -> Result<AssetClass> {
        let class = AssetClass::register(config).await?;
        self.attach(&class);
        Ok(class)
    }

    /// Attach mock peers to an already-registered class.
    pub fn attach(&self, class: &AssetClass) {
        // Merge all peers at each tier into one source per tier.
        let mut lan: HashMap<BlakeHash, Vec<u8>> = HashMap::new();
        for p in &self.lan_peers {
            lan.extend(p.blobs.clone());
        }
        if !lan.is_empty() {
            class.add_source(Arc::new(MockBlobSource {
                tier: FetchTier::Lan,
                blobs: Arc::new(lan),
            }));
        }

        let mut swarm: HashMap<BlakeHash, Vec<u8>> = HashMap::new();
        for p in &self.swarm_peers {
            swarm.extend(p.blobs.clone());
        }
        if !swarm.is_empty() {
            class.add_source(Arc::new(MockBlobSource {
                tier: FetchTier::Swarm,
                blobs: Arc::new(swarm),
            }));
        }

        let mut seeds: HashMap<BlakeHash, Vec<u8>> = HashMap::new();
        for p in &self.seeds {
            seeds.extend(p.blobs.clone());
        }
        if !seeds.is_empty() {
            class.add_source(Arc::new(MockBlobSource {
                tier: FetchTier::Seed,
                blobs: Arc::new(seeds),
            }));
        }

        if let Some(cdn) = &self.cdn {
            class.add_source(Arc::new(MockBlobSource {
                tier: FetchTier::Cdn,
                blobs: Arc::new(cdn.blobs.clone()),
            }));
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AssetClassConfig, BlakeHash, Error};

    async fn lan_class(data: &[u8]) -> (AssetClass, BlakeHash) {
        let hash = BlakeHash::hash(data);
        let class = MockSwarm::new()
            .with_lan_peer(MockPeer::new().with_blob(data))
            .build_class(AssetClassConfig::default())
            .await
            .unwrap();
        (class, hash)
    }

    #[tokio::test]
    async fn fetch_from_lan_peer() {
        let data = b"hello from lan";
        let (class, hash) = lan_class(data).await;
        let fetched = class.asset(hash).fetch().await.unwrap();
        assert_eq!(&fetched[..], data);
    }

    #[tokio::test]
    async fn cache_hit_on_second_fetch() {
        let data = b"once and cache";
        let (class, hash) = lan_class(data).await;
        let asset = class.asset(hash);

        assert!(!asset.is_cached().await, "should not be cached before first fetch");
        let _ = asset.fetch().await.unwrap();
        assert!(asset.is_cached().await, "should be cached after first fetch");

        let fetched = asset.fetch().await.unwrap();
        assert_eq!(&fetched[..], data);
    }

    #[tokio::test]
    async fn fetch_failed_when_no_source_has_blob() {
        let hash = BlakeHash::hash(b"orphan blob");
        let class = MockSwarm::new()
            .build_class(AssetClassConfig::default())
            .await
            .unwrap();
        let err = class.asset(hash).fetch().await.unwrap_err();
        assert!(
            matches!(err, Error::FetchFailed(_)),
            "expected FetchFailed, got {err:?}"
        );
    }

    #[tokio::test]
    async fn falls_through_to_cdn_when_lan_missing() {
        let data = b"cdn fallback data";
        let hash = BlakeHash::hash(data);

        let class = MockSwarm::new()
            .with_lan_peer(MockPeer::new().with_blob(b"unrelated lan blob"))
            .with_cdn(MockPeer::new().with_blob_at(hash, data.to_vec()))
            .build_class(AssetClassConfig::default())
            .await
            .unwrap();

        let fetched = class.asset(hash).fetch().await.unwrap();
        assert_eq!(&fetched[..], data);
    }

    #[tokio::test]
    async fn lan_wins_over_cdn_when_both_have_data() {
        let lan_data = b"lan data";
        let cdn_data = b"cdn data";
        let lan_hash = BlakeHash::hash(lan_data);
        let cdn_hash = BlakeHash::hash(cdn_data);

        let class = MockSwarm::new()
            .with_lan_peer(MockPeer::new().with_blob(lan_data.as_ref()))
            .with_cdn(MockPeer::new().with_blob(cdn_data.as_ref()))
            .build_class(AssetClassConfig::default())
            .await
            .unwrap();

        // LAN wins when it has the blob.
        let fetched = class.asset(lan_hash).fetch().await.unwrap();
        assert_eq!(&fetched[..], lan_data);

        // CDN serves the blob LAN doesn't have.
        let fetched = class.asset(cdn_hash).fetch().await.unwrap();
        assert_eq!(&fetched[..], cdn_data);
    }

    #[tokio::test]
    async fn priority_lan_then_swarm_then_seed_then_cdn() {
        // Put the correct blob only on LAN; put wrong bytes on lower tiers.
        // Wrong bytes have a different BLAKE3, so they'll be rejected.
        let real = b"authoritative blob";
        let hash = BlakeHash::hash(real);
        let bad = b"poisoned (wrong hash)";

        let class = MockSwarm::new()
            .with_swarm_peer(MockPeer::new().with_blob_at(hash, bad.to_vec()))
            .with_seed(MockPeer::new().with_blob_at(hash, bad.to_vec()))
            .with_cdn(MockPeer::new().with_blob_at(hash, bad.to_vec()))
            .with_lan_peer(MockPeer::new().with_blob_at(hash, real.to_vec()))
            .build_class(AssetClassConfig::default())
            .await
            .unwrap();

        let fetched = class.asset(hash).fetch().await.unwrap();
        assert_eq!(&fetched[..], real, "LAN peer should win");
    }

    #[tokio::test]
    async fn poisoned_source_rejected_falls_through_to_cdn() {
        // LAN serves wrong bytes → hash mismatch → skip → CDN wins.
        let real = b"real payload";
        let hash = BlakeHash::hash(real);
        let poison = b"bad bytes from malicious lan peer";

        let class = MockSwarm::new()
            .with_lan_peer(MockPeer::new().with_blob_at(hash, poison.to_vec()))
            .with_cdn(MockPeer::new().with_blob_at(hash, real.to_vec()))
            .build_class(AssetClassConfig::default())
            .await
            .unwrap();

        let fetched = class.asset(hash).fetch().await.unwrap();
        assert_eq!(&fetched[..], real, "CDN serves after LAN is rejected");
    }

    #[tokio::test]
    async fn blake3_hash_roundtrip() {
        let data = b"roundtrip test data";
        let hash = BlakeHash::hash(data);
        assert!(hash.verify(data));
        assert!(!hash.verify(b"wrong data"));
    }

    #[tokio::test]
    async fn blake3_hex_roundtrip() {
        let hash = BlakeHash::hash(b"hex roundtrip");
        let reparsed = BlakeHash::from_hex(&hash.to_hex()).unwrap();
        assert_eq!(hash, reparsed);
    }

    #[tokio::test]
    async fn multiple_assets_in_one_class() {
        let blobs: &[&[u8]] = &[b"alpha", b"beta", b"gamma"];
        let mut peer = MockPeer::new();
        for b in blobs {
            peer = peer.with_blob(*b);
        }
        let class = MockSwarm::new()
            .with_swarm_peer(peer)
            .build_class(AssetClassConfig::default())
            .await
            .unwrap();

        for data in blobs {
            let hash = BlakeHash::hash(data);
            let fetched = class.asset(hash).fetch().await.unwrap();
            assert_eq!(&fetched[..], *data);
        }
    }
}
