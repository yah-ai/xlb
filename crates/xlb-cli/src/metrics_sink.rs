//! Turn `xlb::FetchReport`s into [`FetchMetric`] rows on two transports
//! (R423-T7): the control socket's broadcast channel, and an optional
//! newline-delimited JSON file.
//!
//! ## Why a file and a channel, and not a vendor client
//!
//! R423-T7 says to route metrics "into envoy-test or whatever the current
//! observability sink is". There is no `envoy-test` in this tree — the name
//! appears nowhere outside that ticket's own annotation, and `envoy` in this
//! codebase means the cloud floating-IP/VPS abstraction, which is not a metrics
//! sink at all. Rather than pick a vendor on the ticket's behalf, this emits to
//! the two sinks that commit us to nothing:
//!
//!   * the broadcast channel `xlb inspect`/`SubscribeEvents` already drains,
//!     for live watching;
//!   * NDJSON appended to `node.metrics_path`, which every collector on earth
//!     can tail, and which survives the collector being chosen later.
//!
//! When a real sink is picked, it consumes one of these two; nothing in `xlb`
//! core has to change, because core only knows about
//! [`xlb::FetchObserver`].
//!
//! ## Why writes are blocking, and why that is fine here
//!
//! [`xlb::FetchObserver`] is invoked on the fetch task and must not block. A
//! line append is a single `write(2)` on an already-open handle — microseconds,
//! and bounded — whereas the alternative (spawning a task per fetch, or an
//! unbounded channel) trades a bounded cost for an unbounded one. If the sink
//! ever becomes a network client, that inverts and it must move behind a
//! channel; the seam is here, not in core.

use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;
use xlb::{FetchObserver, FetchReport};

use crate::socket::protocol::{ClassTotals, FetchMetric, NodeEvent};

/// How many recent fetch rows the node keeps per process, across all classes.
///
/// Bounded on purpose: this is a live-inspection tail, not storage. The
/// durable record is the NDJSON file; anything that needs history reads that.
const RECENT_CAP: usize = 256;

/// Everything the emitter needs that a [`FetchReport`] does not carry: who is
/// reporting, and where to put it.
#[derive(Clone)]
pub struct MetricsSink {
    node_id: String,
    peer_class: String,
    event_tx: broadcast::Sender<NodeEvent>,
    /// `None` when no `metrics_path` is configured. The `Mutex` serialises
    /// appends from concurrent fetch tasks so two rows can never interleave
    /// mid-line — a half-written JSON row is worse than a dropped one, because
    /// it breaks the reader rather than the reading.
    file: Option<Arc<Mutex<std::fs::File>>>,
    /// R423-T6: in-process tail + cumulative counters, so `ClassStats` can
    /// answer "how many fetches, how many hits, who served them" without a
    /// collector being deployed first. `std::sync::Mutex` rather than tokio's
    /// because [`MetricsSink::emit`] is called from a synchronous
    /// [`FetchObserver`] and must not await; every critical section here is a
    /// push and a couple of adds.
    recent: Arc<Mutex<Recent>>,
}

#[derive(Default)]
struct Recent {
    /// Oldest at the front. Capped at [`RECENT_CAP`] rows total.
    rows: VecDeque<FetchMetric>,
    /// Cumulative, uncapped, keyed by class name.
    totals: HashMap<String, ClassTotals>,
}

impl MetricsSink {
    /// Open the sink. A `metrics_path` that cannot be opened is an error rather
    /// than a silent downgrade to channel-only: a node told to record metrics
    /// and quietly recording none is the failure this ticket exists to remove.
    pub fn new(
        node_id: String,
        peer_class: String,
        event_tx: broadcast::Sender<NodeEvent>,
        metrics_path: Option<&str>,
    ) -> anyhow::Result<Self> {
        let file = match metrics_path {
            None => None,
            Some(path) => {
                let expanded = crate::config::expand_tilde(path);
                if let Some(parent) = std::path::Path::new(&expanded).parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent).map_err(|e| {
                            anyhow::anyhow!(
                                "metrics_path {expanded}: creating {}: {e}",
                                parent.display()
                            )
                        })?;
                    }
                }
                let f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&expanded)
                    .map_err(|e| anyhow::anyhow!("metrics_path {expanded}: {e}"))?;
                Some(Arc::new(Mutex::new(f)))
            }
        };
        Ok(Self {
            node_id,
            peer_class,
            event_tx,
            file,
            recent: Arc::new(Mutex::new(Recent::default())),
        })
    }

    /// Cumulative counters for `class` since node start. A class that has
    /// served nothing yet reports all zeros — which is a true statement about
    /// it, unlike the hard-coded zeros this replaced.
    pub fn totals_for(&self, class: &str) -> ClassTotals {
        self.with_recent(|r| r.totals.get(class).copied().unwrap_or_default())
    }

    /// The most recent `limit` fetch rows for `class`, oldest first.
    pub fn recent_for(&self, class: &str, limit: usize) -> Vec<FetchMetric> {
        self.with_recent(|r| {
            let matching: Vec<FetchMetric> =
                r.rows.iter().filter(|m| m.class == class).cloned().collect();
            let skip = matching.len().saturating_sub(limit);
            matching[skip..].to_vec()
        })
    }

    /// A poisoned lock must not take the daemon down over telemetry — the
    /// same call this module already makes for the file handle.
    fn with_recent<T>(&self, f: impl FnOnce(&mut Recent) -> T) -> T {
        let mut guard = match self.recent.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        f(&mut guard)
    }

    /// Project a core [`FetchReport`] into the wire row.
    pub fn to_metric(&self, report: &FetchReport) -> FetchMetric {
        FetchMetric {
            timestamp_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            node_id: self.node_id.clone(),
            peer_class: self.peer_class.clone(),
            class: report.class.to_string(),
            blake3: report.hash.to_hex(),
            tier_source: report.tier_label().to_string(),
            bytes_served: report.bytes_served,
            peer_id: report.peer_id.clone(),
            duration_ms: report.duration_ms,
        }
    }

    /// Emit one report to both transports.
    pub fn emit(&self, report: &FetchReport) {
        let metric = self.to_metric(report);

        // R423-T6: record BEFORE the transports. The in-process tail is what
        // `xlb inspect` and the desktop card read, and it is the only one that
        // works with no collector and no subscriber attached.
        self.with_recent(|r| {
            let t = r.totals.entry(metric.class.clone()).or_default();
            t.fetches += 1;
            // `tier_source` is "miss" exactly when no tier served the blob —
            // see FetchReport::tier_label.
            if metric.tier_source != "miss" {
                t.hits += 1;
            }
            t.bytes_served += metric.bytes_served;

            if r.rows.len() == RECENT_CAP {
                r.rows.pop_front();
            }
            r.rows.push_back(metric.clone());
        });

        // A send with no subscribers is an Err and is entirely normal — nobody
        // is watching most of the time. Dropping it is correct; the file sink
        // is the durable one.
        let _ = self.event_tx.send(NodeEvent::FetchMetric(metric.clone()));

        let Some(file) = &self.file else { return };
        let Ok(mut line) = serde_json::to_string(&metric) else {
            tracing::warn!("could not serialise fetch metric");
            return;
        };
        line.push('\n');
        // A poisoned lock must not take the daemon down over telemetry: recover
        // the guard and keep writing. Losing the mutex's poison signal is the
        // cheaper failure here.
        let mut guard = match file.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Err(e) = guard.write_all(line.as_bytes()) {
            tracing::warn!("metrics append failed: {e}");
        }
    }

    /// The [`FetchObserver`] to hand to `AssetClassConfig::observer`.
    pub fn observer(&self) -> FetchObserver {
        let me = self.clone();
        Arc::new(move |report: &FetchReport| me.emit(report))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xlb::{BlakeHash, FetchTier};

    fn sink(path: Option<&str>) -> (MetricsSink, broadcast::Receiver<NodeEvent>) {
        let (tx, rx) = broadcast::channel(16);
        let s = MetricsSink::new("node-abc".into(), "yah-castle".into(), tx, path).unwrap();
        (s, rx)
    }

    fn report(tier: Option<FetchTier>, peer: Option<&str>, bytes: u64) -> FetchReport {
        FetchReport {
            class: "yah-cli",
            hash: BlakeHash::hash(b"payload"),
            tier,
            peer_id: peer.map(String::from),
            bytes_served: bytes,
            duration_ms: 42,
        }
    }

    #[test]
    fn a_seed_hit_carries_the_whole_r423_t7_tuple() {
        let (s, _rx) = sink(None);
        let m = s.to_metric(&report(Some(FetchTier::Seed), Some("peer-xyz"), 4096));
        assert_eq!(m.class, "yah-cli");
        assert_eq!(m.blake3, BlakeHash::hash(b"payload").to_hex());
        assert_eq!(m.tier_source, "seed");
        assert_eq!(m.bytes_served, 4096);
        assert_eq!(m.peer_id.as_deref(), Some("peer-xyz"));
        assert_eq!(m.duration_ms, 42);
        // …plus the reporting node's own identity + cohort.
        assert_eq!(m.node_id, "node-abc");
        assert_eq!(m.peer_class, "yah-castle");
    }

    #[test]
    fn a_miss_is_emitted_as_a_row_with_tier_miss() {
        // The denominator. A hit rate built only from hits is not a hit rate.
        let (s, _rx) = sink(None);
        let m = s.to_metric(&report(None, None, 0));
        assert_eq!(m.tier_source, "miss");
        assert_eq!(m.bytes_served, 0);
        assert!(m.peer_id.is_none());
    }

    #[test]
    fn a_cdn_hit_reports_no_peer_id() {
        // Attribution must not invent a peer for a tier that has none.
        let (s, _rx) = sink(None);
        let m = s.to_metric(&report(Some(FetchTier::Cdn), None, 10));
        assert_eq!(m.tier_source, "cdn");
        assert!(m.peer_id.is_none());
    }

    #[test]
    fn emitting_reaches_the_broadcast_channel() {
        let (s, mut rx) = sink(None);
        s.emit(&report(Some(FetchTier::Lan), Some("p"), 7));
        match rx.try_recv().expect("an event") {
            NodeEvent::FetchMetric(m) => {
                assert_eq!(m.tier_source, "lan");
                assert_eq!(m.bytes_served, 7);
            }
            other => panic!("wrong event: {other:?}"),
        }
    }

    #[test]
    fn the_file_sink_appends_one_json_row_per_fetch() {
        let dir = tempfile::tempdir().unwrap();
        // A nested path also proves the parent dir is created.
        let path = dir.path().join("nested/metrics.ndjson");
        let (s, _rx) = sink(Some(path.to_str().unwrap()));

        s.emit(&report(Some(FetchTier::Swarm), Some("p1"), 1));
        s.emit(&report(None, None, 0));

        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2, "one row per fetch: {body}");
        let first: FetchMetric = serde_json::from_str(lines[0]).unwrap();
        let second: FetchMetric = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(first.tier_source, "swarm");
        assert_eq!(second.tier_source, "miss");
    }

    #[test]
    fn the_file_sink_appends_rather_than_truncating_across_restarts() {
        // A daemon restart must not erase the day's readings — the egress
        // reconciliation this ticket exists for is a cumulative number.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metrics.ndjson");
        let p = path.to_str().unwrap();

        let (s1, _r1) = sink(Some(p));
        s1.emit(&report(Some(FetchTier::Seed), Some("p"), 1));
        drop(s1);

        let (s2, _r2) = sink(Some(p));
        s2.emit(&report(Some(FetchTier::Cdn), None, 2));

        assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 2);
    }

    #[test]
    fn an_unopenable_metrics_path_fails_loudly() {
        // Told to record and silently recording nothing is the exact shape
        // R423 is trying to remove from this system.
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();
        let path = blocker.join("metrics.ndjson");
        let (tx, _rx) = broadcast::channel(4);
        assert!(
            MetricsSink::new("n".into(), "c".into(), tx, path.to_str()).is_err(),
            "a metrics_path under a regular file must not open"
        );
    }

    #[test]
    fn no_metrics_path_means_channel_only_and_no_error() {
        let (s, mut rx) = sink(None);
        s.emit(&report(Some(FetchTier::Cache), None, 3));
        assert!(matches!(rx.try_recv(), Ok(NodeEvent::FetchMetric(_))));
    }

    #[test]
    fn the_ndjson_row_and_the_socket_row_are_the_same_bytes() {
        // Two transports, one meaning. If these ever diverge, a dashboard built
        // on one silently mis-reads the other.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metrics.ndjson");
        let (s, mut rx) = sink(Some(path.to_str().unwrap()));
        s.emit(&report(Some(FetchTier::Seed), Some("p9"), 128));

        let from_socket = match rx.try_recv().unwrap() {
            NodeEvent::FetchMetric(m) => m,
            other => panic!("wrong event: {other:?}"),
        };
        let from_file: FetchMetric =
            serde_json::from_str(std::fs::read_to_string(&path).unwrap().trim()).unwrap();
        assert_eq!(from_socket, from_file);
    }

    // ── in-process tail + totals (R423-T6) ─────────────────────────────────

    #[test]
    fn totals_count_fetches_hits_and_bytes_separately() {
        // The hit RATE is the point: counting only hits gives a numerator with
        // no denominator, which is what ClassStats used to report.
        let (s, _rx) = sink(None);
        s.emit(&report(Some(FetchTier::Seed), Some("p1"), 100));
        s.emit(&report(Some(FetchTier::Cache), None, 50));
        s.emit(&report(None, None, 0)); // miss

        let t = s.totals_for("yah-cli");
        assert_eq!(t.fetches, 3);
        assert_eq!(t.hits, 2);
        assert_eq!(t.bytes_served, 150);
    }

    #[test]
    fn totals_are_per_class_and_unknown_classes_read_zero() {
        let (s, _rx) = sink(None);
        s.emit(&report(Some(FetchTier::Cdn), None, 7));
        assert_eq!(s.totals_for("yah-cli").fetches, 1);
        // Not "no data" dressed up as activity — a class nobody has fetched
        // from reports zeros, and that is a true statement about it.
        assert_eq!(s.totals_for("whisper-ggml"), Default::default());
    }

    #[test]
    fn the_recent_tail_keeps_the_newest_rows_and_stays_bounded() {
        let (s, _rx) = sink(None);
        for i in 0..(RECENT_CAP + 10) {
            s.emit(&report(Some(FetchTier::Lan), Some("p"), i as u64));
        }
        // Cumulative counters are NOT capped by the tail.
        assert_eq!(s.totals_for("yah-cli").fetches, (RECENT_CAP + 10) as u64);

        let tail = s.recent_for("yah-cli", RECENT_CAP * 2);
        assert_eq!(tail.len(), RECENT_CAP, "the ring is bounded");
        assert_eq!(
            tail.last().unwrap().bytes_served,
            (RECENT_CAP + 9) as u64,
            "newest row is last"
        );
        assert_eq!(
            tail.first().unwrap().bytes_served,
            10,
            "the ten oldest rows were dropped, not the ten newest"
        );
    }

    #[test]
    fn the_recent_tail_filters_by_class_before_limiting() {
        // Limiting first and filtering second would let a busy class starve a
        // quiet one out of its own card.
        let (s, _rx) = sink(None);
        let mut other = report(Some(FetchTier::Swarm), Some("p"), 1);
        other.class = "whisper-ggml";
        for _ in 0..50 {
            s.emit(&other);
        }
        s.emit(&report(Some(FetchTier::Seed), Some("p9"), 999));

        let tail = s.recent_for("yah-cli", 20);
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].bytes_served, 999);
        assert_eq!(tail[0].peer_id.as_deref(), Some("p9"));
    }

    #[test]
    fn a_miss_lands_in_the_tail_too() {
        let (s, _rx) = sink(None);
        s.emit(&report(None, None, 0));
        let tail = s.recent_for("yah-cli", 5);
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].tier_source, "miss");
        assert!(tail[0].peer_id.is_none());
    }
}
