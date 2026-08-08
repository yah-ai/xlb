//! Per-fetch observability — which tier actually served a blob, from whom,
//! how many bytes, and how long it took (R423-T7).
//!
//! ## Why this exists
//!
//! W160 sizes the seed's egress bill on an assumed **50% swarm hit rate**. That
//! number was never measured, and until this module it was not *measurable*:
//! [`crate::source::FetchChain`] knew exactly which tier served every blob and
//! then dropped the answer on the floor one line later, at
//! `if let Some((_tier, bytes))`. The CLI's own fetch event reported
//! `tier: "fetched"` — a constant string carrying no information at all.
//!
//! So the cost envelope was theoretical, and the one experiment that would
//! falsify it could not be run. A [`FetchReport`] is that experiment's datum.
//!
//! ## Shape
//!
//! One report per completed [`crate::Asset::fetch`], hit or miss, carrying the
//! tuple R423-T7 asks for: class, blake3, tier, bytes, peer, duration.
//!
//! **A miss is a report too.** `tier: None` is the "no source had it" case, and
//! it has to be emitted rather than silently dropped: a hit-rate denominator
//! built only from successes is not a hit rate. That is the same class of
//! mistake as a monitor that renders zero machines when it could not read its
//! inventory — an absence reported as a value.
//!
//! ## What it deliberately does not measure
//!
//! `bytes_served` here is **download**-side: bytes this node pulled in. The
//! upload side — bytes this node served *to* a peer — is not visible from the
//! fetch chain at all, and reporting a download as if it were an upload would
//! make the egress reconciliation this ticket exists for silently wrong. See
//! the module docs on `FetchObserver` for where that hook has to go instead.

use std::sync::Arc;

use crate::{BlakeHash, FetchTier};

/// One completed fetch, as a metric.
///
/// Emitted through the [`FetchObserver`] registered on
/// [`crate::AssetClassConfig::observer`], and returned directly by
/// [`crate::Asset::fetch_reported`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchReport {
    /// The [`crate::AssetClass`] this fetch belonged to.
    pub class: &'static str,
    /// Content address of the blob.
    pub hash: BlakeHash,
    /// Which tier served it. `None` means **every** tier missed — the blob was
    /// not obtained. Carried as an `Option` rather than a synthetic `Miss`
    /// variant so a consumer cannot accidentally treat a miss as a fifth tier
    /// in a hit-rate denominator.
    pub tier: Option<FetchTier>,
    /// Hex `NodeId` of the peer that served the bytes, when the serving tier
    /// *is* a peer (LAN / swarm / seed). `None` for cache and CDN — neither
    /// has a NodeId, and inventing one would corrupt per-peer attribution.
    pub peer_id: Option<String>,
    /// Bytes obtained. `0` on a miss.
    pub bytes_served: u64,
    /// Wall time for the whole chain walk, not just the serving tier — a miss
    /// at LAN that falls through to CDN is *why* the fetch was slow, so
    /// charging the duration to CDN alone would hide the cost of the miss.
    pub duration_ms: u64,
}

impl FetchReport {
    /// `true` when some tier served the blob.
    pub fn is_hit(&self) -> bool {
        self.tier.is_some()
    }

    /// `true` when the bytes came from a *peer* rather than from local cache or
    /// the paid CDN egress path. This is the numerator of W160's hit-rate
    /// assumption, spelled out once here so every consumer computes it the same
    /// way.
    ///
    /// `Cache` is excluded deliberately: a cache hit costs nobody anything and
    /// counting it would inflate the swarm's apparent contribution.
    pub fn is_swarm_hit(&self) -> bool {
        matches!(
            self.tier,
            Some(FetchTier::Lan) | Some(FetchTier::Swarm) | Some(FetchTier::Seed)
        )
    }

    /// The tier label a dashboard should group by — `FetchTier::label` for a
    /// hit, `"miss"` for a total miss.
    pub fn tier_label(&self) -> &'static str {
        match self.tier {
            Some(t) => t.label(),
            None => "miss",
        }
    }
}

/// Callback invoked once per completed fetch.
///
/// Cheap to clone (`Arc`), invoked on the fetch task, so the closure must be
/// `Send + Sync` and **must not block** — a sink that does I/O should hand the
/// report to a channel and return.
///
/// ## Where the upload side has to go, and why it is not here
///
/// This observer sees only fetches *this* node performs. A seed's egress is
/// bytes it **serves**, which happens inside `iroh-blobs`' provider event
/// stream — a surface `xlb` does not currently subscribe to anywhere. Wiring it
/// is a separate change to `transport::blobs`, not a variant of this type, and
/// until it exists no reading here can be reconciled against a hosting bill.
/// Stated plainly so nobody builds an egress dashboard on the wrong number.
pub type FetchObserver = Arc<dyn Fn(&FetchReport) + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    fn report(tier: Option<FetchTier>) -> FetchReport {
        FetchReport {
            class: "test",
            hash: BlakeHash::hash(b"x"),
            tier,
            peer_id: None,
            bytes_served: 1,
            duration_ms: 0,
        }
    }

    #[test]
    fn a_miss_is_not_a_hit_and_labels_as_miss() {
        let r = report(None);
        assert!(!r.is_hit());
        assert!(!r.is_swarm_hit());
        assert_eq!(r.tier_label(), "miss");
    }

    #[test]
    fn cache_is_a_hit_but_never_a_swarm_hit() {
        // Counting cache hits toward the swarm rate would inflate exactly the
        // number W160's cost envelope rests on.
        let r = report(Some(FetchTier::Cache));
        assert!(r.is_hit());
        assert!(!r.is_swarm_hit());
        assert_eq!(r.tier_label(), "cache");
    }

    #[test]
    fn cdn_is_a_hit_but_never_a_swarm_hit() {
        // A CDN hit is the case that COSTS money — the opposite of a swarm hit.
        let r = report(Some(FetchTier::Cdn));
        assert!(r.is_hit());
        assert!(!r.is_swarm_hit());
        assert_eq!(r.tier_label(), "cdn");
    }

    #[test]
    fn the_three_peer_tiers_are_swarm_hits() {
        for t in [FetchTier::Lan, FetchTier::Swarm, FetchTier::Seed] {
            assert!(report(Some(t)).is_swarm_hit(), "{t:?} should count");
        }
    }
}
