use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::BlakeHash;

/// Position in the five-tier fetch-chain priority order.
///
/// Sources with lower numeric values are tried first. A verified hit at any
/// tier short-circuits the rest. A BLAKE3 mismatch at any tier logs a warning
/// and falls through to the next tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FetchTier {
    /// Local on-disk LRU cache. Free, µs latency. Always tried first.
    Cache = 0,
    /// mDNS-discovered peers on the same LAN subnet. Free, <1 ms.
    Lan = 1,
    /// iroh-relay-mediated swarm peers. Peer's bandwidth, 10–100 ms.
    Swarm = 2,
    /// Permanently-pinned seeds (NodeIds in the app binary). 50–200 ms.
    Seed = 3,
    /// HTTP/HTTPS CDN fallback. App's egress cost. Reachable behind firewalls
    /// that block QUIC/UDP — the real reason it exists.
    Cdn = 4,
}

impl FetchTier {
    pub fn label(self) -> &'static str {
        match self {
            FetchTier::Cache => "cache",
            FetchTier::Lan => "lan",
            FetchTier::Swarm => "swarm",
            FetchTier::Seed => "seed",
            FetchTier::Cdn => "cdn",
        }
    }
}

/// Any source that can serve bytes for a [`BlakeHash`].
///
/// The fetch chain calls sources in [`FetchTier`] order; each result is
/// BLAKE3-verified in [`FetchChain::fetch`] before being returned.
/// A mismatch causes the chain to skip to the next source (a poisoned peer
/// cannot corrupt a download).
#[async_trait]
pub(crate) trait BlobSource: Send + Sync {
    /// Try to serve bytes for `hash`. Returns `None` on a miss.
    ///
    /// BLAKE3 verification happens in the caller — do not verify here.
    async fn fetch_raw(&self, hash: &BlakeHash) -> Option<Bytes>;

    fn tier(&self) -> FetchTier;
}

/// Ordered list of [`BlobSource`]s. Tries each in [`FetchTier`] order.
pub(crate) struct FetchChain {
    sources: Vec<Arc<dyn BlobSource>>,
}

impl FetchChain {
    pub fn new(mut sources: Vec<Arc<dyn BlobSource>>) -> Self {
        sources.sort_by_key(|s| s.tier());
        FetchChain { sources }
    }

    /// Try each source in tier order; verify BLAKE3 on receipt.
    ///
    /// Returns `(tier, bytes)` on the first verified hit. Returns `None`
    /// if no source has the blob or all sources returned mismatching bytes.
    pub async fn fetch(&self, hash: &BlakeHash) -> Option<(FetchTier, Bytes)> {
        for source in &self.sources {
            let Some(bytes) = source.fetch_raw(hash).await else {
                continue;
            };
            if hash.verify(&bytes) {
                tracing::debug!(tier = source.tier().label(), hash = %hash, "fetch hit");
                return Some((source.tier(), bytes));
            }
            tracing::warn!(
                tier = source.tier().label(),
                hash = %hash,
                "BLAKE3 mismatch — dropping source"
            );
        }
        None
    }
}
