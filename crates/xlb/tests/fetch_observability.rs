//! R423-T7 — the fetch chain reports which tier actually served a blob.
//!
//! These run the REAL chain through `MockSwarm`, not a projection of it. The
//! unit tests in `metrics.rs` prove the report's own arithmetic; these prove the
//! chain fills it in truthfully, which is the part that was missing — the tier
//! was computed and then discarded one line later, and the CLI reported the
//! constant string `"fetched"` in its place.
//!
//! W160's cost envelope rests on an assumed 50% swarm hit rate. Every assertion
//! here is about making that number measurable rather than assumed.

use std::sync::{Arc, Mutex};

use xlb::{
    testing::{MockPeer, MockSwarm},
    AssetClass, AssetClassConfig, BlakeHash, FetchReport, FetchTier,
};

/// A class plus the reports its observer captured, in order.
struct Observed {
    class: AssetClass,
    seen: Arc<Mutex<Vec<FetchReport>>>,
}

impl Observed {
    async fn build(swarm: MockSwarm) -> Self {
        let seen: Arc<Mutex<Vec<FetchReport>>> = Arc::default();
        let sink = seen.clone();
        let class = swarm
            .build_class(AssetClassConfig {
                name: "observed",
                observer: Some(Arc::new(move |r: &FetchReport| {
                    sink.lock().unwrap().push(r.clone());
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        Self { class, seen }
    }

    fn reports(&self) -> Vec<FetchReport> {
        self.seen.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn a_seed_hit_reports_tier_seed_and_the_serving_peer() {
    let data = b"served by the seed";
    let hash = BlakeHash::hash(data);
    let o = Observed::build(
        MockSwarm::new().with_seed(
            MockPeer::new()
                .with_id("seed-node-1")
                .with_blob(data.as_ref()),
        ),
    )
    .await;

    let (bytes, report) = o.class.asset(hash).fetch_reported().await;
    assert_eq!(&bytes.unwrap()[..], data);

    assert_eq!(report.tier, Some(FetchTier::Seed));
    assert_eq!(report.tier_label(), "seed");
    assert_eq!(report.peer_id.as_deref(), Some("seed-node-1"));
    assert_eq!(report.bytes_served, data.len() as u64);
    assert_eq!(report.class, "observed");
    assert_eq!(report.hash, hash);
    assert!(report.is_hit() && report.is_swarm_hit());
}

#[tokio::test]
async fn a_cdn_hit_reports_tier_cdn_with_no_peer() {
    // The tier that COSTS money. It must be distinguishable from a swarm hit,
    // and it must not carry an invented peer id.
    let data = b"paid egress";
    let hash = BlakeHash::hash(data);
    let o =
        Observed::build(MockSwarm::new().with_cdn(MockPeer::new().with_blob(data.as_ref()))).await;

    let (_, report) = o.class.asset(hash).fetch_reported().await;
    assert_eq!(report.tier, Some(FetchTier::Cdn));
    assert!(report.is_hit());
    assert!(
        !report.is_swarm_hit(),
        "a CDN hit is the opposite of a swarm hit"
    );
    assert!(report.peer_id.is_none());
}

#[tokio::test]
async fn the_report_names_the_tier_that_served_not_the_one_that_was_tried() {
    // A poisoned LAN peer is tried first, rejected on hash mismatch, and CDN
    // serves. Charging the hit to LAN would report a swarm hit for a fetch that
    // cost full CDN egress — the single most expensive way this metric could
    // lie, because it inflates exactly the ratio W160 is sized on.
    let real = b"real payload";
    let hash = BlakeHash::hash(real);
    let o = Observed::build(
        MockSwarm::new()
            .with_lan_peer(
                MockPeer::new()
                    .with_id("liar")
                    .with_blob_at(hash, b"poisoned bytes".to_vec()),
            )
            .with_cdn(MockPeer::new().with_blob_at(hash, real.to_vec())),
    )
    .await;

    let (bytes, report) = o.class.asset(hash).fetch_reported().await;
    assert_eq!(&bytes.unwrap()[..], real);
    assert_eq!(report.tier, Some(FetchTier::Cdn));
    assert!(!report.is_swarm_hit());
    assert_ne!(report.peer_id.as_deref(), Some("liar"));
}

#[tokio::test]
async fn a_total_miss_is_reported_rather_than_swallowed() {
    // The denominator. If misses are not emitted, a "hit rate" computed from
    // these rows is 100% by construction.
    let hash = BlakeHash::hash(b"nobody has this");
    let o = Observed::build(MockSwarm::new()).await;

    let (result, report) = o.class.asset(hash).fetch_reported().await;
    assert!(result.is_err());
    assert_eq!(report.tier, None);
    assert_eq!(report.tier_label(), "miss");
    assert_eq!(report.bytes_served, 0);
    assert!(!report.is_hit());
    assert_eq!(o.reports().len(), 1, "the miss reached the observer");
}

#[tokio::test]
async fn the_second_fetch_reports_cache_not_the_original_tier() {
    // Once cached, a blob costs nobody anything. Continuing to report it as a
    // seed hit would overstate the swarm's contribution on every repeat fetch.
    let data = b"fetch me twice";
    let hash = BlakeHash::hash(data);
    let o = Observed::build(
        MockSwarm::new().with_swarm_peer(MockPeer::new().with_id("p1").with_blob(data.as_ref())),
    )
    .await;

    let (_, first) = o.class.asset(hash).fetch_reported().await;
    assert_eq!(first.tier, Some(FetchTier::Swarm));

    let (_, second) = o.class.asset(hash).fetch_reported().await;
    assert_eq!(second.tier, Some(FetchTier::Cache));
    assert!(second.is_hit());
    assert!(
        !second.is_swarm_hit(),
        "a cache hit is free, not swarm-served"
    );
    assert!(second.peer_id.is_none(), "cache has no peer");
}

#[tokio::test]
async fn every_fetch_path_reaches_the_observer_exactly_once() {
    // `fetch`, `fetch_with_progress` and `fetch_reported` must all be
    // instrumented. An entry point that skips the observer produces a hit rate
    // that is quietly measured over a subset of traffic.
    let data = b"count me";
    let hash = BlakeHash::hash(data);
    let miss = BlakeHash::hash(b"absent");
    let o = Observed::build(
        MockSwarm::new().with_lan_peer(MockPeer::new().with_id("lan-1").with_blob(data.as_ref())),
    )
    .await;

    let _ = o.class.asset(hash).fetch().await.unwrap();
    let sink: xlb::ProgressSink = Arc::new(|_| {});
    let _ = o.class.asset(hash).fetch_with_progress(sink).await.unwrap();
    let _ = o.class.asset(hash).fetch_reported().await;
    let _ = o.class.asset(miss).fetch().await;

    let reports = o.reports();
    assert_eq!(
        reports.len(),
        4,
        "one report per fetch, hit or miss: {reports:?}"
    );
    assert_eq!(reports[0].tier, Some(FetchTier::Lan));
    // 1 and 2 are cache hits — the first fetch populated it.
    assert_eq!(reports[1].tier, Some(FetchTier::Cache));
    assert_eq!(reports[2].tier, Some(FetchTier::Cache));
    assert_eq!(reports[3].tier, None, "the miss is the fourth row");
}

#[tokio::test]
async fn a_class_with_no_observer_still_fetches() {
    // Instrumentation must be optional and free. This is the default shape for
    // every existing consumer.
    let data = b"uninstrumented";
    let hash = BlakeHash::hash(data);
    let class = MockSwarm::new()
        .with_lan_peer(MockPeer::new().with_blob(data.as_ref()))
        .build_class(AssetClassConfig::default())
        .await
        .unwrap();
    assert_eq!(&class.asset(hash).fetch().await.unwrap()[..], data);
}

#[tokio::test]
async fn the_hit_rate_over_a_batch_is_computable_from_the_reports() {
    // The whole point, end to end: three blobs, one on a peer and two only on
    // the CDN, must read as a 33% swarm hit rate — not as an assumption.
    let swarmed = b"on a peer";
    let paid_a = b"cdn only a";
    let paid_b = b"cdn only b";
    let o = Observed::build(
        MockSwarm::new()
            .with_swarm_peer(MockPeer::new().with_id("p1").with_blob(swarmed.as_ref()))
            .with_cdn(
                MockPeer::new()
                    .with_blob(paid_a.as_ref())
                    .with_blob(paid_b.as_ref()),
            ),
    )
    .await;

    for data in [swarmed.as_ref(), paid_a.as_ref(), paid_b.as_ref()] {
        let _ = o.class.asset(BlakeHash::hash(data)).fetch().await.unwrap();
    }

    let reports = o.reports();
    assert_eq!(reports.len(), 3);
    let swarm_hits = reports.iter().filter(|r| r.is_swarm_hit()).count();
    assert_eq!(swarm_hits, 1);

    let paid_bytes: u64 = reports
        .iter()
        .filter(|r| r.tier == Some(FetchTier::Cdn))
        .map(|r| r.bytes_served)
        .sum();
    assert_eq!(
        paid_bytes,
        (paid_a.len() + paid_b.len()) as u64,
        "billable egress is a sum over the rows, not an estimate"
    );
}
