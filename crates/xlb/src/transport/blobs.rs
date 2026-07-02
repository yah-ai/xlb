//! iroh-blobs adapter — wires iroh-blobs on top of an `mshr::Endpoint`.
//!
//! Architecture:
//! - [`BlobTransport`] owns a [`MemStore`] and an iroh [`Router`] that accepts
//!   incoming iroh-blobs connections via [`RatedBlobsProtocol`]. This is the
//!   seeding side.
//! - [`IrohFetcher`] (private) implements [`BlobSource`] by dialling a peer and
//!   fetching a blob via the iroh-blobs protocol. This is the fetch side.
//! - Call [`BlobTransport::attach_fetcher`] to wire a fetcher into an
//!   [`AssetClass`] for any one peer at a chosen [`FetchTier`].
//! - Call [`BlobTransport::attach_permanent_seeds`] to wire every entry of
//!   `AssetClassConfig::permanent_seeds` at [`FetchTier::Seed`] in one shot —
//!   the canonical bootstrap path for apps that configure pinned seeds.
//! - Call [`BlobTransport::set_upload_cap`] to live-update the per-process
//!   upload rate cap on the serving side (default: uncapped).
//!
//! LAN (mDNS) and Swarm (pkarr / iroh-relay) discovery for tiers 1–2 live
//! in [`mshr::discovery`]; this module owns the static-seed path only.
//!
//! ## Upload rate limiting
//!
//! [`RatedBlobsProtocol`] replaces the stock `BlobsProtocol` on the accept
//! side. It intercepts each incoming QUIC connection, wraps every outgoing
//! send stream in [`RateLimitedSendStream`], and delegates to iroh-blobs'
//! public `handle_stream` for the actual protocol logic. Rate state is held
//! in a process-global [`UploadLimiter`] shared via `Arc` so
//! [`BlobTransport::set_upload_cap`] takes effect on all in-flight connections
//! immediately.

use std::{
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use bytes::Bytes;
use iroh_blobs::{
    provider::{StreamPair, handle_stream, events::EventSender},
    store::mem::MemStore,
    Hash as IrohHash,
};
use iroh::endpoint::VarInt;
use tokio::sync::Mutex;
use mshr::{Endpoint, NodeAddr, NodeId};

use crate::{
    BwCaps,
    source::{BlobSource, FetchTier},
    AssetClass, BlakeHash,
};

// ─── Hash conversion ─────────────────────────────────────────────────────────

fn to_iroh(h: &BlakeHash) -> IrohHash {
    IrohHash::from_bytes(*h.as_bytes())
}

// ─── UploadLimiter ────────────────────────────────────────────────────────────

/// Process-global upload rate limiter for the blob seeding side.
///
/// `cap_kbits == 0` means uncapped. Non-zero values are treated as the upload
/// ceiling in kbit/s (e.g. `500` = 500 kbit/s = 62.5 KB/s). Live-updatable
/// via atomic store; token-bucket state is protected by an async `Mutex`.
///
/// `seeding_enabled == false` causes [`RatedBlobsProtocol::accept`] to reject
/// all incoming connections immediately (the transport keeps listening but
/// serves no bytes). Defaults to `true`.
#[derive(Debug)]
struct UploadLimiter {
    /// Upload ceiling in kbit/s. `0` = uncapped.
    cap_kbits: AtomicU32,
    /// Whether the seeding side is accepting connections at all.
    seeding_enabled: AtomicBool,
    /// Token bucket state — protected by an async mutex so the lock is never
    /// held across a sleep (other waiters can proceed while one sleeps).
    state: Mutex<LimiterState>,
}

#[derive(Debug)]
struct LimiterState {
    /// Available bytes in the bucket (can go slightly negative under burst).
    tokens: f64,
    last_tick: Instant,
}

impl UploadLimiter {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cap_kbits: AtomicU32::new(0),
            seeding_enabled: AtomicBool::new(true),
            state: Mutex::new(LimiterState { tokens: 0.0, last_tick: Instant::now() }),
        })
    }

    /// Set the upload rate cap in kbit/s. `0` = uncapped.
    fn set_kbits(&self, cap_kbits: u32) {
        self.cap_kbits.store(cap_kbits, Ordering::SeqCst);
    }

    /// Enable or disable the seeding side entirely.
    ///
    /// When `false`, [`RatedBlobsProtocol`] closes incoming connections before
    /// serving any bytes. When `true`, the rate cap (if any) applies normally.
    fn set_enabled(&self, enabled: bool) {
        self.seeding_enabled.store(enabled, Ordering::SeqCst);
    }

    /// Acquire permission to send `n` bytes. If a rate cap is active, sleeps
    /// until the token bucket has refilled enough to cover the charge. The
    /// lock is released before sleeping so concurrent senders can proceed.
    ///
    /// Uses a leaky-bucket / pre-charge model: tokens are charged immediately
    /// and we sleep for however long it takes to refill back to zero. This
    /// handles single-request sizes larger than 1 second's worth of bytes
    /// correctly — the cap loop approach would loop infinitely in that case.
    async fn acquire(&self, n: usize) {
        let cap = self.cap_kbits.load(Ordering::Relaxed);
        if cap == 0 || n == 0 {
            return;
        }
        let bytes_per_sec = (cap as f64 * 1_000.0) / 8.0;

        let sleep = {
            let mut s = self.state.lock().await;
            let now = Instant::now();
            let elapsed = now.duration_since(s.last_tick).as_secs_f64();
            s.last_tick = now;
            // Refill based on elapsed time; cap accumulation at 2 seconds'
            // worth of burst budget so idle periods don't allow large bursts.
            s.tokens = (s.tokens + elapsed * bytes_per_sec).min(bytes_per_sec * 2.0);
            // Pre-charge: subtract the full send immediately.
            s.tokens -= n as f64;
            // If negative, sleep until we'd be back at zero.
            if s.tokens < 0.0 {
                std::time::Duration::from_secs_f64((-s.tokens) / bytes_per_sec)
            } else {
                std::time::Duration::ZERO
            }
        }; // lock released before sleep

        if !sleep.is_zero() {
            tokio::time::sleep(sleep).await;
        }
    }
}

// ─── RateLimitedSendStream ────────────────────────────────────────────────────

/// Wraps `iroh::endpoint::SendStream` and rate-limits all writes through a
/// shared [`UploadLimiter`].
struct RateLimitedSendStream {
    inner: iroh::endpoint::SendStream,
    limiter: Arc<UploadLimiter>,
}

impl iroh_blobs::util::SendStream for RateLimitedSendStream {
    async fn send_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
        self.limiter.acquire(bytes.len()).await;
        Ok(self.inner.write_chunk(bytes).await
            .map_err(|e| io::Error::other(e))?)
    }

    async fn send(&mut self, buf: &[u8]) -> io::Result<()> {
        self.limiter.acquire(buf.len()).await;
        Ok(self.inner.write_all(buf).await
            .map_err(|e| io::Error::other(e))?)
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn reset(&mut self, code: VarInt) -> io::Result<()> {
        self.inner.reset(code).map_err(|e| io::Error::other(e))
    }

    async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
        Ok(self.inner.stopped().await
            .map_err(|e| io::Error::other(e))?
            .map(|e| e.into()))
    }

    fn id(&self) -> u64 {
        self.inner.id().index()
    }
}

// ─── RatedBlobsProtocol ──────────────────────────────────────────────────────

/// iroh [`ProtocolHandler`] that wraps each outgoing send stream with
/// [`RateLimitedSendStream`] before delegating to iroh-blobs' `handle_stream`.
///
/// This replaces the stock `BlobsProtocol` on the seeding side so we can
/// interpose rate limiting without forking iroh-blobs.
#[derive(Debug)]
struct RatedBlobsProtocol {
    store: MemStore,
    limiter: Arc<UploadLimiter>,
}

impl iroh::protocol::ProtocolHandler for RatedBlobsProtocol {
    fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> impl Future<Output = Result<(), iroh::protocol::AcceptError>> + Send {
        let store: iroh_blobs::api::Store = (*self.store).clone();
        let limiter = self.limiter.clone();
        async move {
            // If seeding is disabled, close the connection immediately without
            // serving any bytes. The peer will see a QUIC connection reset.
            if !limiter.seeding_enabled.load(Ordering::Relaxed) {
                connection.close(VarInt::from_u32(0), b"seeding disabled");
                return Ok(());
            }
            let conn_id = connection.stable_id() as u64;
            let events = EventSender::DEFAULT;
            loop {
                // Re-check on each new stream request so toggling off takes
                // effect quickly (after the current in-flight stream drains).
                if !limiter.seeding_enabled.load(Ordering::Relaxed) {
                    break;
                }
                let (writer, reader) = match connection.accept_bi().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let rated_writer = RateLimitedSendStream { inner: writer, limiter: limiter.clone() };
                let pair = StreamPair::new(conn_id, reader, rated_writer, events.clone());
                let store = store.clone();
                tokio::spawn(async move {
                    let _ = handle_stream(pair, store).await;
                });
            }
            Ok(())
        }
    }
}

// ─── BlobTransport ───────────────────────────────────────────────────────────

/// iroh-blobs seeder + fetcher for one xlb process.
///
/// Wraps a [`MemStore`] and an iroh [`Router`] so this node can both serve
/// blobs (incoming iroh-blobs ALPN connections handled by the Router) and
/// fetch blobs from remote peers (via [`IrohFetcher`]).
///
/// Build one per [`AssetClass`]. The `Endpoint` should not be running
/// another accept loop — the Router's background task owns the accept side.
///
/// ## Upload rate limiting
///
/// Call [`BlobTransport::set_upload_cap`] with a [`BwCaps`] to cap outgoing
/// blob traffic. `cap.up_mbit == 0` is treated as "off" (uncapped, identical
/// to passing `None`). Updates take effect on all subsequent `write` calls
/// across every in-flight connection immediately — no restart required.
pub struct BlobTransport {
    store: MemStore,
    router: iroh::protocol::Router,
    endpoint: Endpoint,
    upload_limiter: Arc<UploadLimiter>,
}

impl BlobTransport {
    /// Build a transport from a bound `xlb-net::Endpoint`.
    ///
    /// Spawns an iroh Router accept loop (on the endpoint's iroh socket) that
    /// handles incoming iroh-blobs requests via [`RatedBlobsProtocol`].
    pub async fn new(endpoint: Endpoint) -> anyhow::Result<Self> {
        let store = MemStore::new();
        let upload_limiter = UploadLimiter::new();
        let rated_proto = RatedBlobsProtocol { store: store.clone(), limiter: upload_limiter.clone() };
        let router = iroh::protocol::Router::builder(endpoint.inner().clone())
            .accept(iroh_blobs::ALPN, rated_proto)
            .spawn();
        Ok(Self { store, router, endpoint, upload_limiter })
    }

    /// Set the upload rate cap for the seeding side (mbit/s resolution).
    ///
    /// - `Some(caps)` — enforce `caps.up_mbit` as the per-process ceiling.
    ///   `caps.up_mbit == 0` is treated the same as `None` (uncapped). Does
    ///   not change the enabled/disabled state.
    /// - `None` — remove the cap entirely.
    ///
    /// For sub-mbit precision (e.g. 500 kbit/s), use [`Self::set_seeding_cap`].
    pub fn set_upload_cap(&self, cap: Option<&BwCaps>) {
        let kbits = cap.map(|c| c.up_mbit.saturating_mul(1000)).unwrap_or(0);
        self.upload_limiter.set_kbits(kbits);
    }

    /// Configure the seeding side with kbit/s precision.
    ///
    /// - `enabled = false` — reject all incoming connections immediately.
    ///   The `kbits` parameter is ignored.
    /// - `enabled = true, kbits = 0` — accept connections with no rate cap.
    /// - `enabled = true, kbits > 0` — accept connections, capped at `kbits`
    ///   kbit/s (e.g. `500` = 500 kbit/s ≈ 62.5 KB/s).
    ///
    /// Takes effect on all subsequent stream-accept calls immediately.
    pub fn set_seeding_cap(&self, enabled: bool, kbits: u32) {
        self.upload_limiter.set_enabled(enabled);
        self.upload_limiter.set_kbits(if enabled { kbits } else { 0 });
    }

    /// Add a blob to the local store so it can be served to remote peers.
    ///
    /// Returns the [`BlakeHash`] of the data (identical to `BlakeHash::hash(&data)`).
    pub async fn add_blob(&self, data: impl Into<Bytes>) -> anyhow::Result<BlakeHash> {
        let data: Bytes = data.into();
        let xlb_hash = BlakeHash::hash(&data);
        let mut tt = self.store.add_bytes(data).temp_tag().await
            .map_err(|e| anyhow::anyhow!("iroh-blobs add_bytes: {e}"))?;
        // Leak the temp tag so the blob is never GC'd for the life of this transport.
        tt.leak();
        Ok(xlb_hash)
    }

    /// The `NodeAddr` of this transport's endpoint.
    ///
    /// Give this to remote peers so they can resolve our address when fetching.
    pub fn node_addr(&self) -> NodeAddr {
        self.endpoint.endpoint_addr()
    }

    /// Wire an iroh-blobs fetcher into `class` at `tier`.
    pub fn attach_fetcher(&self, class: &AssetClass, seeder: NodeAddr, tier: FetchTier) {
        class.add_source(Arc::new(IrohFetcher {
            tier,
            endpoint: self.endpoint.clone(),
            peer: seeder,
            local_store: self.store.clone(),
        }));
    }

    /// Wire every entry of `class`'s `permanent_seeds` config at
    /// [`FetchTier::Seed`].
    pub fn attach_permanent_seeds(&self, class: &AssetClass) -> SeedAttachReport {
        let mut report = SeedAttachReport::default();
        for (idx, raw) in class.permanent_seeds().iter().enumerate() {
            match parse_seed_node_id(raw) {
                Ok(addr) => {
                    self.attach_fetcher(class, addr, FetchTier::Seed);
                    report.attached += 1;
                }
                Err(err) => {
                    tracing::warn!(
                        class = class.name(),
                        index = idx,
                        entry = %raw,
                        "permanent_seeds: skipping malformed entry: {err}"
                    );
                    report.errors.push((idx, err));
                }
            }
        }
        report
    }

    /// Shut down the Router accept loop and wait for it to drain.
    pub async fn shutdown(self) {
        if let Err(e) = self.router.shutdown().await {
            tracing::warn!("BlobTransport::shutdown: {e}");
        }
    }
}

// ─── Permanent-seed parsing ──────────────────────────────────────────────────

/// Reasons a hex-encoded permanent-seed `NodeId` failed to parse.
#[derive(Debug, thiserror::Error)]
pub enum SeedParseError {
    #[error("not valid hex")]
    InvalidHex,
    #[error("expected 32 bytes, got {got}")]
    InvalidLength { got: usize },
    #[error("not a valid Ed25519 public key")]
    InvalidPubKey,
}

/// Parse a hex-encoded `NodeId` string into a bare [`NodeAddr`].
pub fn parse_seed_node_id(s: &str) -> Result<NodeAddr, SeedParseError> {
    let bytes = hex::decode(s.trim()).map_err(|_| SeedParseError::InvalidHex)?;
    let arr: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| SeedParseError::InvalidLength { got: bytes.len() })?;
    let node_id = NodeId::from_bytes(&arr).map_err(|_| SeedParseError::InvalidPubKey)?;
    Ok(NodeAddr::from(node_id))
}

/// Outcome of [`BlobTransport::attach_permanent_seeds`].
#[derive(Debug, Default)]
pub struct SeedAttachReport {
    pub attached: usize,
    pub errors: Vec<(usize, SeedParseError)>,
}

// ─── IrohFetcher ─────────────────────────────────────────────────────────────

struct IrohFetcher {
    tier: FetchTier,
    endpoint: Endpoint,
    peer: NodeAddr,
    local_store: MemStore,
}

#[async_trait]
impl BlobSource for IrohFetcher {
    fn tier(&self) -> FetchTier {
        self.tier
    }

    async fn fetch_raw(&self, hash: &BlakeHash) -> Option<Bytes> {
        let iroh_hash = to_iroh(hash);

        if let Ok(bytes) = self.local_store.get_bytes(iroh_hash).await {
            return Some(bytes);
        }

        let conn = self
            .endpoint
            .inner()
            .connect(self.peer.clone(), iroh_blobs::ALPN)
            .await
            .map_err(|e| tracing::warn!(%hash, "iroh-blobs connect: {e}"))
            .ok()?;

        self.local_store
            .remote()
            .fetch(conn, iroh_hash)
            .await
            .map_err(|e| tracing::warn!(%hash, "iroh-blobs fetch: {e}"))
            .ok()?;

        self.local_store
            .get_bytes(iroh_hash)
            .await
            .map_err(|e| tracing::warn!(%hash, "iroh-blobs get_bytes: {e}"))
            .ok()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use mshr::Keypair;

    async fn make_transport() -> BlobTransport {
        let kp = Keypair::generate();
        let ep = Endpoint::builder().keypair(kp).bind().await.unwrap();
        BlobTransport::new(ep).await.unwrap()
    }

    #[tokio::test]
    async fn set_upload_cap_stores_kbits() {
        let t = make_transport().await;
        assert_eq!(t.upload_limiter.cap_kbits.load(Ordering::Relaxed), 0);

        // mbit-resolution API converts to kbits internally
        t.set_upload_cap(Some(&BwCaps { up_mbit: 5, down_mbit: 50 }));
        assert_eq!(t.upload_limiter.cap_kbits.load(Ordering::Relaxed), 5_000);

        t.set_upload_cap(None);
        assert_eq!(t.upload_limiter.cap_kbits.load(Ordering::Relaxed), 0);

        t.set_upload_cap(Some(&BwCaps { up_mbit: 0, down_mbit: 10 }));
        assert_eq!(t.upload_limiter.cap_kbits.load(Ordering::Relaxed), 0);

        // kbits-resolution API (for sub-mbit values)
        t.set_seeding_cap(true, 500);
        assert_eq!(t.upload_limiter.cap_kbits.load(Ordering::Relaxed), 500);
        assert!(t.upload_limiter.seeding_enabled.load(Ordering::Relaxed));

        t.set_seeding_cap(false, 500);
        assert!(!t.upload_limiter.seeding_enabled.load(Ordering::Relaxed));

        t.shutdown().await;
    }

    #[tokio::test]
    async fn upload_limiter_uncapped_is_fast() {
        let limiter = UploadLimiter::new();
        // With cap=0 (off), acquiring any number of bytes should be instant.
        let start = tokio::time::Instant::now();
        for _ in 0..100 {
            limiter.acquire(65536).await;
        }
        // 100 × 64 KB with no cap should complete in well under 100 ms.
        assert!(start.elapsed().as_millis() < 100, "uncapped acquire should be near-instant");
    }

    #[tokio::test]
    async fn upload_limiter_cap_delays_writes() {
        let limiter = UploadLimiter::new();
        // 1000 kbits/s = 125,000 bytes/s. Acquire 250,000 bytes (≈2s worth).
        limiter.set_kbits(1000); // 1000 kbit/s = 125,000 bytes/s
        let bytes_to_send: usize = 250_000; // 2 seconds at 1000 kbit/s
        let start = tokio::time::Instant::now();
        limiter.acquire(bytes_to_send).await;
        let elapsed = start.elapsed();
        // Should take at least 1.5 seconds (generous lower bound).
        assert!(
            elapsed.as_millis() >= 1500,
            "capped acquire should be rate-limited: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn upload_limiter_cap_can_be_lifted() {
        let limiter = UploadLimiter::new();
        limiter.set_kbits(1000); // 1000 kbit/s
        limiter.set_kbits(0);    // lift cap

        let start = tokio::time::Instant::now();
        limiter.acquire(1_000_000).await; // 1 MB — instant with no cap
        assert!(
            start.elapsed().as_millis() < 500,
            "after lifting cap, acquire should be near-instant"
        );
    }

    #[tokio::test]
    async fn rate_limited_send_stream_id_delegates() {
        // Smoke test: constructing a RateLimitedSendStream and checking id()
        // requires a live endpoint. We skip the full write test here since
        // it would need a second endpoint. The upload_limiter_cap_delays_writes
        // test covers the token-bucket logic.
        let _ = make_transport().await.shutdown().await;
    }
}
