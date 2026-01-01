//! Thin wrapper around `iroh::Endpoint`. The wrapper exists so consumers
//! `use xlb_net::Endpoint` rather than `iroh::Endpoint` — one upgrade
//! point for the pre-1.0 substrate, and a transparent fork escape hatch.
//!
//! This phase (R105-F1) intentionally exposes the bare minimum API needed
//! for two endpoints in the same process to round-trip a stream:
//!
//! ```ignore
//! use xlb_net::{Endpoint, Keypair};
//!
//! let alice = Endpoint::builder()
//!     .keypair(Keypair::generate())
//!     .alpns(["yah/test/v1"])
//!     .bind().await?;
//!
//! let bob = Endpoint::builder()
//!     .keypair(Keypair::generate())
//!     .alpns(["yah/test/v1"])
//!     .bind().await?;
//!
//! let alice_addr = alice.endpoint_addr();
//! let conn = bob.connect_alpn(alice_addr, b"yah/test/v1").await?;
//! ```
//!
//! Discovery aggregation (mDNS / iroh-relay swarm / static / external
//! roster) lands in R105-F2..F3; this scaffolding deliberately leaves the
//! peer-resolution surface narrow.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use iroh::endpoint::{presets, Connection, Incoming};
use iroh::tls::CaRootsConfig;
use iroh::{RelayMap, RelayMode};

use crate::{Discovery, EndpointAddr, Error, Keypair, NodeId, Result};

/// ALPN bytes type alias — picked up from `iroh`'s convention.
pub type Alpn = Vec<u8>;

/// Pinned, boxed, send-able future. Inlined to avoid pulling `futures`
/// solely for one type alias at this phase.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Async handler for an accepted incoming connection on a particular ALPN.
///
/// Boxed for object safety; consumers usually wrap a method on a
/// per-protocol struct (e.g. `society::handle_v1`) in an `Arc`.
pub type AlpnHandler = Arc<
    dyn Fn(Connection) -> BoxFut<'static, anyhow::Result<()>> + Send + Sync + 'static,
>;

/// Builder for [`Endpoint`]. Returned by [`Endpoint::builder`].
pub struct EndpointBuilder {
    keypair: Option<Keypair>,
    alpns: Vec<Alpn>,
    discovery: Option<Discovery>,
    relay_map: Option<RelayMap>,
    insecure_skip_tls_verify: bool,
}

impl EndpointBuilder {
    fn new() -> Self {
        Self {
            keypair: None,
            alpns: Vec::new(),
            discovery: None,
            relay_map: None,
            insecure_skip_tls_verify: false,
        }
    }

    /// Attach a [`Discovery`] composition. When omitted, the endpoint
    /// binds with no address-lookup services — peers must be reachable
    /// via fully-formed `EndpointAddr`s passed to `connect_alpn`.
    pub fn discovery(mut self, d: Discovery) -> Self {
        self.discovery = Some(d);
        self
    }

    /// Use a custom relay (typically a warden-hosted [`crate::relay::Server`])
    /// for NAT-traversal proxying and QUIC address discovery. Wraps
    /// `iroh::RelayMode::Custom`.
    ///
    /// Build the [`RelayMap`] via [`crate::relay::Server::relay_map`] (when
    /// hosting locally in a test) or [`crate::relay::relay_map_for_https`]
    /// (when the URL is known out-of-band, e.g. from fleet config).
    pub fn relay_map(mut self, map: RelayMap) -> Self {
        self.relay_map = Some(map);
        self
    }

    /// Skip TLS certificate verification on relay connections. **Tests
    /// only** — required when the relay uses a self-signed cert. Mirrors
    /// `iroh::CaRootsConfig::insecure_skip_verify()`.
    pub fn insecure_skip_tls_verify(mut self, skip: bool) -> Self {
        self.insecure_skip_tls_verify = skip;
        self
    }

    /// Bind the endpoint to the given keypair. Required.
    pub fn keypair(mut self, kp: Keypair) -> Self {
        self.keypair = Some(kp);
        self
    }

    /// ALPN strings the endpoint will accept on. The accept loop dispatches
    /// to the registered handler matching the ALPN reported by the peer.
    pub fn alpns<I, S>(mut self, alpns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        self.alpns = alpns.into_iter().map(|s| s.as_ref().to_vec()).collect();
        self
    }

    /// Bind the endpoint, returning a clone-able handle.
    pub async fn bind(self) -> Result<Endpoint> {
        let keypair = self
            .keypair
            .ok_or_else(|| Error::Endpoint("EndpointBuilder: keypair() is required".into()))?;
        let alpns = self.alpns;

        // F1 uses the `Minimal` preset: it picks a TLS crypto provider but
        // does NOT install n0's DNS lookup or relay endpoints. F2/F3 layer
        // discovery on top; this phase keeps two in-process endpoints
        // self-sufficient for the round-trip test (and for any caller that
        // hands a fully-formed `EndpointAddr` out-of-band).
        let relay_mode = match self.relay_map {
            Some(map) => RelayMode::Custom(map),
            None => RelayMode::Disabled,
        };
        let mut b = iroh::Endpoint::builder(presets::Minimal)
            .secret_key(keypair.secret().clone())
            .relay_mode(relay_mode);
        if self.insecure_skip_tls_verify {
            b = b.ca_roots_config(CaRootsConfig::insecure_skip_verify());
        }
        if !alpns.is_empty() {
            b = b.alpns(alpns.clone());
        }
        if let Some(d) = self.discovery {
            b = d.apply(b);
        }

        let inner = b
            .bind()
            .await
            .map_err(|e| Error::Endpoint(format!("bind failed: {e}")))?;

        Ok(Endpoint {
            inner,
            keypair,
            registered_alpns: Arc::new(alpns),
        })
    }
}

/// Process-wide endpoint handle. `Clone + Send + Sync`; clones share the
/// underlying socket and connection pool.
#[derive(Clone)]
pub struct Endpoint {
    inner: iroh::Endpoint,
    keypair: Keypair,
    registered_alpns: Arc<Vec<Alpn>>,
}

impl Endpoint {
    /// Start a new builder. See [`EndpointBuilder`].
    pub fn builder() -> EndpointBuilder {
        EndpointBuilder::new()
    }

    /// This endpoint's `NodeId` (Ed25519 pubkey).
    pub fn node_id(&self) -> NodeId {
        self.keypair.node_id()
    }

    /// Borrow the keypair this endpoint was bound with.
    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    /// ALPNs the endpoint was registered to accept on.
    pub fn alpns(&self) -> &[Alpn] {
        &self.registered_alpns
    }

    /// Snapshot the current `EndpointAddr` (NodeId + best-known direct addrs +
    /// optional relay URL). Useful for handing to a peer in tests or for
    /// out-of-band rendezvous before the discovery layer lands (F2/F3).
    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.inner.addr()
    }

    /// Borrow the wrapped `iroh::Endpoint`. Escape hatch — prefer the
    /// methods on this wrapper where possible so the dep stays swappable.
    pub fn inner(&self) -> &iroh::Endpoint {
        &self.inner
    }

    /// Open a connection to a peer by `EndpointAddr` on the given ALPN.
    pub async fn connect_alpn(
        &self,
        peer: impl Into<EndpointAddr>,
        alpn: &[u8],
    ) -> Result<Connection> {
        self.inner
            .connect(peer, alpn)
            .await
            .map_err(|e| Error::Endpoint(format!("connect: {e}")))
    }

    /// Accept the next incoming connection (raw — no ALPN dispatch).
    /// Returns `None` once the endpoint is closed.
    pub async fn accept(&self) -> Option<Incoming> {
        self.inner.accept().await
    }

    /// Run an ALPN-dispatching accept loop. Spawns a task per connection,
    /// each invoking the handler registered for that ALPN. Connections
    /// whose ALPN has no registered handler are dropped (logged at
    /// `tracing::warn`).
    ///
    /// The loop runs until the endpoint is closed; returns `Ok(())` after
    /// a clean shutdown.
    pub async fn accept_dispatch(
        &self,
        handlers: HashMap<Alpn, AlpnHandler>,
    ) -> Result<()> {
        let handlers = Arc::new(handlers);
        while let Some(incoming) = self.inner.accept().await {
            let handlers = handlers.clone();
            tokio::spawn(async move {
                if let Err(e) = dispatch_one(incoming, handlers).await {
                    tracing::warn!(error = %e, "xlb-net accept_dispatch: connection failed");
                }
            });
        }
        Ok(())
    }

    /// Close the endpoint. Idempotent.
    pub async fn close(&self) {
        self.inner.close().await;
    }
}

async fn dispatch_one(
    incoming: Incoming,
    handlers: Arc<HashMap<Alpn, AlpnHandler>>,
) -> anyhow::Result<()> {
    // `Incoming::into_future()` (via IntoFuture) drives the handshake to
    // completion and yields a `Connection<HandshakeCompleted>` whose
    // `alpn()` we can dispatch on.
    let conn: Connection = incoming.await?;
    let alpn = conn.alpn();
    let handler = handlers.get(alpn).cloned().ok_or_else(|| {
        anyhow::anyhow!(
            "no handler registered for ALPN {:?}",
            String::from_utf8_lossy(alpn)
        )
    })?;
    handler(conn).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Two endpoints in the same process, directly addressed via
    /// `EndpointAddr`, round-trip a single bidirectional stream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn round_trip_stream() {
        const ALPN: &[u8] = b"xlb-net/test/v1";

        let alice = Endpoint::builder()
            .keypair(Keypair::generate())
            .alpns([ALPN])
            .bind()
            .await
            .expect("alice bind");

        let bob = Endpoint::builder()
            .keypair(Keypair::generate())
            .alpns([ALPN])
            .bind()
            .await
            .expect("bob bind");

        // Alice runs a tiny echo server via accept_dispatch.
        let saw_request = Arc::new(AtomicBool::new(false));
        let saw_request_h = saw_request.clone();
        let alice_handle = alice.clone();
        let server = tokio::spawn(async move {
            let mut handlers: HashMap<Alpn, AlpnHandler> = HashMap::new();
            let flag = saw_request_h.clone();
            handlers.insert(
                ALPN.to_vec(),
                Arc::new(move |conn: Connection| {
                    let flag = flag.clone();
                    Box::pin(async move {
                        let (mut send, mut recv) = conn.accept_bi().await?;
                        let buf = recv.read_to_end(1024).await?;
                        flag.store(true, Ordering::SeqCst);
                        send.write_all(&buf).await?;
                        send.finish()?;
                        // Wait for the peer to close so iroh doesn't drop
                        // unsent stream frames.
                        let _ = conn.closed().await;
                        Ok(())
                    }) as BoxFut<'static, anyhow::Result<()>>
                }),
            );
            let _ = alice_handle.accept_dispatch(handlers).await;
        });

        let alice_addr = alice.endpoint_addr();

        // Bob dials Alice on ALPN and echoes a payload.
        let conn = bob
            .connect_alpn(alice_addr, ALPN)
            .await
            .expect("bob connect");
        let (mut send, mut recv) = conn.open_bi().await.expect("open_bi");
        send.write_all(b"hello yah").await.expect("write");
        send.finish().expect("finish");
        let echoed = recv.read_to_end(1024).await.expect("read");
        assert_eq!(echoed, b"hello yah");
        conn.close(0u32.into(), b"done");

        // Allow Alice's handler to observe.
        for _ in 0..50 {
            if saw_request.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(saw_request.load(Ordering::SeqCst), "alice handler ran");

        alice.close().await;
        bob.close().await;
        server.abort();
    }

    /// Static-lane discovery: alice's `EndpointAddr` is pinned in bob's
    /// `Discovery::with_static`, so bob can dial alice using the bare
    /// `EndpointId` (no inline addrs) and the MemoryLookup resolves it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn static_lane_resolves_node_id() {
        const ALPN: &[u8] = b"xlb-net/test/static-lane/v1";

        let alice = Endpoint::builder()
            .keypair(Keypair::generate())
            .alpns([ALPN])
            .bind()
            .await
            .expect("alice bind");
        let alice_addr = alice.endpoint_addr();
        let alice_id = alice.node_id();

        let bob = Endpoint::builder()
            .keypair(Keypair::generate())
            .alpns([ALPN])
            .discovery(Discovery::new().with_static([alice_addr]))
            .bind()
            .await
            .expect("bob bind");

        let alice_handle = alice.clone();
        let server = tokio::spawn(async move {
            let mut handlers: HashMap<Alpn, AlpnHandler> = HashMap::new();
            handlers.insert(
                ALPN.to_vec(),
                Arc::new(|conn: Connection| {
                    Box::pin(async move {
                        let (mut send, mut recv) = conn.accept_bi().await?;
                        let buf = recv.read_to_end(64).await?;
                        send.write_all(&buf).await?;
                        send.finish()?;
                        let _ = conn.closed().await;
                        Ok(())
                    }) as BoxFut<'static, anyhow::Result<()>>
                }),
            );
            let _ = alice_handle.accept_dispatch(handlers).await;
        });

        // Dial by bare EndpointId — only resolvable through the static lane.
        let conn = bob
            .connect_alpn(EndpointAddr::from(alice_id), ALPN)
            .await
            .expect("static-lane connect");
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(b"static").await.unwrap();
        send.finish().unwrap();
        let echoed = recv.read_to_end(64).await.unwrap();
        assert_eq!(echoed, b"static");
        conn.close(0u32.into(), b"done");

        alice.close().await;
        bob.close().await;
        server.abort();
    }

    /// LAN-lane mDNS discovery is wired through to iroh, but real
    /// multicast in unit-test environments is unreliable (CI sandboxes,
    /// container networks, hosts with multicast disabled). We keep this
    /// test as a smoke-check that the builder compiles and binds; an
    /// end-to-end mDNS round-trip belongs in an environment-conditional
    /// integration test once the network harness exists.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lan_lane_binds() {
        let ep = Endpoint::builder()
            .keypair(Keypair::generate())
            .alpns([b"xlb-net/test/lan/v1"])
            .discovery(Discovery::new().with_lan())
            .bind()
            .await
            .expect("bind with LAN discovery");
        ep.close().await;
    }

    /// External-roster lane: a `MockPeerSource` pushes alice's
    /// `EndpointAddr` into bob's discovery pool. Bob then dials alice
    /// by bare `EndpointId` and the connect resolves through the
    /// roster's `MemoryLookup`. F2's static lane is *not* configured —
    /// only the roster — so any dial that succeeds proves the F3 path
    /// is wired end-to-end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_roster_resolves_node_id() {
        use crate::{PeerHint, PeerHintStream, PeerSource};
        use std::sync::Mutex;
        use tokio::sync::mpsc;

        const ALPN: &[u8] = b"xlb-net/test/roster/v1";

        struct MockSource {
            rx: Mutex<Option<mpsc::UnboundedReceiver<PeerHint>>>,
        }
        impl PeerSource for MockSource {
            fn subscribe(&self) -> PeerHintStream {
                let rx = self
                    .rx
                    .lock()
                    .unwrap()
                    .take()
                    .expect("subscribe called once");
                Box::pin(RxStream(rx))
            }
        }
        struct RxStream(mpsc::UnboundedReceiver<PeerHint>);
        impl futures_core::Stream for RxStream {
            type Item = PeerHint;
            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                self.0.poll_recv(cx)
            }
        }

        let (tx, rx) = mpsc::unbounded_channel::<PeerHint>();
        let source = MockSource { rx: Mutex::new(Some(rx)) };

        let alice = Endpoint::builder()
            .keypair(Keypair::generate())
            .alpns([ALPN])
            .bind()
            .await
            .expect("alice bind");
        let alice_addr = alice.endpoint_addr();
        let alice_id = alice.node_id();

        let bob = Endpoint::builder()
            .keypair(Keypair::generate())
            .alpns([ALPN])
            .discovery(Discovery::new().with_external_roster(source))
            .bind()
            .await
            .expect("bob bind");

        // Push alice's addr into bob's roster *after* bind (the pump
        // task is already subscribed). Give the pump a beat to drain.
        tx.send(PeerHint::Found(alice_addr)).expect("roster send");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let alice_handle = alice.clone();
        let server = tokio::spawn(async move {
            let mut handlers: HashMap<Alpn, AlpnHandler> = HashMap::new();
            handlers.insert(
                ALPN.to_vec(),
                Arc::new(|conn: iroh::endpoint::Connection| {
                    Box::pin(async move {
                        let (mut send, mut recv) = conn.accept_bi().await?;
                        let buf = recv.read_to_end(64).await?;
                        send.write_all(&buf).await?;
                        send.finish()?;
                        let _ = conn.closed().await;
                        Ok(())
                    }) as BoxFut<'static, anyhow::Result<()>>
                }),
            );
            let _ = alice_handle.accept_dispatch(handlers).await;
        });

        let conn = bob
            .connect_alpn(EndpointAddr::from(alice_id), ALPN)
            .await
            .expect("roster-lane connect");
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(b"roster").await.unwrap();
        send.finish().unwrap();
        let echoed = recv.read_to_end(64).await.unwrap();
        assert_eq!(echoed, b"roster");
        conn.close(0u32.into(), b"done");

        alice.close().await;
        bob.close().await;
        server.abort();
    }

    /// Swarm-lane builder smoke: configuring `with_relays(default_relays())`
    /// must not crash the bind step. Real pkarr round-trips against n0's
    /// public relay aren't appropriate for a unit test (network egress,
    /// flaky CI); end-to-end pkarr lives in an integration test against
    /// a warden-hosted relay (R105-F4).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn swarm_lane_binds() {
        use crate::default_relays;

        let ep = Endpoint::builder()
            .keypair(Keypair::generate())
            .alpns([b"xlb-net/test/swarm/v1"])
            .discovery(Discovery::new().with_relays(default_relays()))
            .bind()
            .await
            .expect("bind with swarm discovery");
        ep.close().await;
    }

    #[test]
    fn builder_requires_keypair() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let res = rt.block_on(async { Endpoint::builder().alpns([b"x"]).bind().await });
        match res {
            Err(Error::Endpoint(_)) => {}
            other => panic!("expected Error::Endpoint, got {:?}", other.err()),
        }
    }
}
