//! Discovery aggregator. Composes the four discovery lanes from
//! `xlb-net.md` (Lan / Swarm / Static / ExternalRoster) and feeds them to
//! the underlying `iroh::Endpoint::Builder` as `address_lookup` services.
//!
//! Lanes:
//!
//! - **Lan** (R105-F2): mDNS-style local-subnet discovery via the
//!   `iroh-mdns-address-lookup` crate.
//! - **Static** (R105-F2): pinned `EndpointAddr`s known up-front, backed
//!   by `iroh::address_lookup::memory::MemoryLookup`.
//! - **Swarm** (R105-F3): pkarr-relay-mediated peer discovery. Each
//!   configured pkarr relay URL gets a `PkarrPublisher` (so this endpoint
//!   advertises itself) and a `PkarrResolver` (so this endpoint can look
//!   up peers that advertised on the same relay).
//! - **ExternalRoster** (R105-F3): pluggable [`PeerSource`] feed —
//!   society's mesh roster, yubaba's raft state, anything that can stream
//!   `(node_id, addrs)` tuples. Bridged into a `MemoryLookup` updated by
//!   a background task driven by the source's stream.
//!
//! ## Why this layer exists
//!
//! Consumers of xlb-net (society, yubaba, xlb itself) shouldn't each have
//! to assemble their own discovery composition. Putting the aggregator
//! here means there's one place to standardize defaults and one place to
//! revisit if iroh's address-lookup model changes between rc.0 and 1.0.

use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::address_lookup::{PkarrPublisher, PkarrResolver, N0_DNS_PKARR_RELAY_PROD};
use iroh::endpoint::Builder as IrohBuilder;
use iroh_mdns_address_lookup::MdnsAddressLookup;
use url::Url;

use crate::{EndpointAddr, NodeId};

/// A peer-hint event emitted by an external roster source.
///
/// `Found` adds (or refreshes) addressing info for `node_id`; `Lost`
/// removes any cached info for `node_id`.
#[derive(Debug, Clone)]
pub enum PeerHint {
    /// A peer is reachable at the given addresses (either an
    /// [`EndpointAddr`] with relay URL + direct addrs, or just a bare
    /// `NodeId` if the source only knows identity).
    Found(EndpointAddr),

    /// The peer with this `NodeId` is no longer reachable through this
    /// source. Removes any cached `EndpointAddr` keyed on the id.
    Lost(NodeId),
}

/// Boxed `Stream` of [`PeerHint`]s, returned by [`PeerSource::subscribe`].
pub type PeerHintStream = Pin<Box<dyn Stream<Item = PeerHint> + Send + 'static>>;

/// External-roster feed for the discovery aggregator.
///
/// Implementors stream `(node_id, addrs)` hints from outside xlb-net —
/// society's gossip mesh, yubaba's raft state, an operator-supplied
/// hint file. `xlb-net` subscribes once at endpoint-bind time and
/// consumes updates for the life of the [`crate::Endpoint`].
///
/// xlb-net deliberately knows nothing about the source: dependency
/// direction is one-way (society/yubaba depend on xlb-net, never the
/// reverse). The trait is `Send + Sync + 'static` so a single source can
/// be shared across crate boundaries via `Arc<dyn PeerSource>`.
pub trait PeerSource: Send + Sync + 'static {
    /// Stream of peer-hint events. Called once per [`crate::Endpoint`]
    /// at bind time.
    fn subscribe(&self) -> PeerHintStream;
}

/// Default pkarr-relay URLs used when [`Discovery::with_relays`] is
/// called with the result of this function — n0's public pkarr relay.
///
/// Once yubaba cloud nodes host their own pkarr relays (R105-F4 +
/// yubaba's deploy track), this list will be `vec![<yubaba-url>,
/// <n0-fallback>]`. For now it's the n0 fallback only; production
/// callers should pass their own URLs explicitly.
pub fn default_relays() -> Vec<Url> {
    vec![N0_DNS_PKARR_RELAY_PROD
        .parse()
        .expect("static N0 pkarr relay URL parses")]
}

/// Composition of discovery lanes to layer onto an [`crate::Endpoint`].
///
/// Build with [`Discovery::new`] (or [`Discovery::default`]) and the
/// `with_*` methods, then hand to [`crate::EndpointBuilder::discovery`].
#[derive(Default, Clone)]
pub struct Discovery {
    lan: bool,
    static_seeds: Vec<EndpointAddr>,
    relay_urls: Vec<Url>,
    rosters: Vec<Arc<dyn PeerSource>>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discovery")
            .field("lan", &self.lan)
            .field("static_seeds", &self.static_seeds.len())
            .field("relay_urls", &self.relay_urls)
            .field("rosters", &self.rosters.len())
            .finish()
    }
}

impl Discovery {
    /// New, empty discovery composition. Equivalent to [`Discovery::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable the LAN (mDNS-like) lane. Discovers other yah endpoints on
    /// the same local subnet without any relay or DNS infra. Cheap and
    /// fast on shared networks; a no-op on hosts where multicast is
    /// blocked (the lookup service simply finds nothing).
    pub fn with_lan(mut self) -> Self {
        self.lan = true;
        self
    }

    /// Enable the LAN lane only if the predicate is true. Ergonomic
    /// shortcut for `if cond { d.with_lan() } else { d }`.
    pub fn with_lan_if(self, cond: bool) -> Self {
        if cond { self.with_lan() } else { self }
    }

    /// Add pinned `EndpointAddr`s the endpoint should always know about.
    /// Typical entries: yah-cloud permanent seeds, customer-camp yubaba.
    /// Replaces any previously-set static seeds; chain-call to merge.
    pub fn with_static<I>(mut self, seeds: I) -> Self
    where
        I: IntoIterator<Item = EndpointAddr>,
    {
        self.static_seeds = seeds.into_iter().collect();
        self
    }

    /// Append additional static seeds (does not replace existing).
    pub fn add_static(mut self, seed: EndpointAddr) -> Self {
        self.static_seeds.push(seed);
        self
    }

    /// Enable the **Swarm** lane via pkarr relays. Each URL gets a
    /// publisher (advertising this endpoint's `EndpointInfo`) plus a
    /// resolver (looking up other endpoints that advertised on the same
    /// relay).
    ///
    /// Pass [`default_relays`] for n0's public pkarr relay, or supply
    /// your own (typically a yubaba cloud node running an embedded
    /// relay::Server, see R105-F4). Calling with an empty iterator
    /// leaves the swarm lane disabled.
    pub fn with_relays<I>(mut self, urls: I) -> Self
    where
        I: IntoIterator<Item = Url>,
    {
        self.relay_urls = urls.into_iter().collect();
        self
    }

    /// Append a single pkarr relay URL (does not replace existing).
    pub fn add_relay(mut self, url: Url) -> Self {
        self.relay_urls.push(url);
        self
    }

    /// Attach an external roster source. xlb-net subscribes once at
    /// bind time and feeds emitted [`PeerHint`]s into a private
    /// `MemoryLookup` for the life of the endpoint.
    ///
    /// Multiple roster sources can be layered (society + yubaba, say)
    /// — each chain call appends another source.
    pub fn with_external_roster<S>(mut self, source: S) -> Self
    where
        S: PeerSource,
    {
        self.rosters.push(Arc::new(source));
        self
    }

    /// Attach an `Arc<dyn PeerSource>` directly — useful when the same
    /// source is shared across crate boundaries (e.g. society and
    /// yubaba both holding the same roster).
    pub fn with_external_roster_arc(mut self, source: Arc<dyn PeerSource>) -> Self {
        self.rosters.push(source);
        self
    }

    /// LAN lane on?
    pub fn lan_enabled(&self) -> bool {
        self.lan
    }

    /// Currently-pinned static seeds.
    pub fn static_seeds(&self) -> &[EndpointAddr] {
        &self.static_seeds
    }

    /// Configured pkarr relay URLs for the swarm lane.
    pub fn relay_urls(&self) -> &[Url] {
        &self.relay_urls
    }

    /// Number of attached external-roster sources.
    pub fn roster_count(&self) -> usize {
        self.rosters.len()
    }

    /// Apply the configured lanes to an `iroh::endpoint::Builder`. Called
    /// by [`crate::EndpointBuilder::bind`]; not normally invoked by hand.
    pub(crate) fn apply(self, mut b: IrohBuilder) -> IrohBuilder {
        if !self.static_seeds.is_empty() {
            let mem = MemoryLookup::with_provenance("xlb-net/static");
            for seed in self.static_seeds {
                mem.add_endpoint_info(seed);
            }
            b = b.address_lookup(mem);
        }
        if self.lan {
            b = b.address_lookup(MdnsAddressLookup::builder());
        }
        for url in self.relay_urls {
            // Both publisher and resolver target the same relay URL.
            // PkarrPublisherBuilder + PkarrResolverBuilder implement
            // AddressLookupBuilder, so they pick up secret_key + tls
            // config from the constructed Endpoint.
            b = b
                .address_lookup(PkarrPublisher::builder(url.clone()))
                .address_lookup(PkarrResolver::builder(url));
        }
        for source in self.rosters {
            // Each roster gets its own MemoryLookup (one provenance per
            // source) with a background task forwarding the stream.
            let mem = MemoryLookup::with_provenance("xlb-net/roster");
            spawn_roster_pump(source, mem.clone());
            b = b.address_lookup(mem);
        }
        b
    }
}

fn spawn_roster_pump(source: Arc<dyn PeerSource>, sink: MemoryLookup) {
    use std::task::Poll;

    let mut stream = source.subscribe();
    tokio::spawn(async move {
        // Hand-rolled `next()` over the boxed Stream so we don't pull
        // in `futures-util` just for one helper. `Pin<Box<S>>` derefs
        // to `S`, and `as_mut()` projects to `Pin<&mut S>` for poll.
        std::future::poll_fn(move |cx| -> Poll<()> {
            loop {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(PeerHint::Found(addr))) => {
                        sink.add_endpoint_info(addr);
                    }
                    Poll::Ready(Some(PeerHint::Lost(id))) => {
                        sink.remove_endpoint_info(id);
                    }
                    Poll::Ready(None) => return Poll::Ready(()),
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
        .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_off() {
        let d = Discovery::new();
        assert!(!d.lan_enabled());
        assert!(d.static_seeds().is_empty());
        assert!(d.relay_urls().is_empty());
        assert_eq!(d.roster_count(), 0);
    }

    #[test]
    fn with_static_collects() {
        use iroh::SecretKey;
        let id = SecretKey::generate().public();
        let d = Discovery::new().with_static([EndpointAddr::from(id)]);
        assert_eq!(d.static_seeds().len(), 1);
    }

    #[test]
    fn default_relays_parses() {
        let urls = default_relays();
        assert_eq!(urls.len(), 1);
    }

    #[test]
    fn with_relays_collects() {
        let d = Discovery::new().with_relays(default_relays());
        assert_eq!(d.relay_urls().len(), 1);
    }

    #[test]
    fn add_relay_appends() {
        let d = Discovery::new()
            .with_relays(default_relays())
            .add_relay("https://relay.example/pkarr".parse().unwrap());
        assert_eq!(d.relay_urls().len(), 2);
    }

    struct EmptyStream;
    impl Stream for EmptyStream {
        type Item = PeerHint;
        fn poll_next(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            std::task::Poll::Ready(None)
        }
    }

    struct EmptySource;
    impl PeerSource for EmptySource {
        fn subscribe(&self) -> PeerHintStream {
            Box::pin(EmptyStream)
        }
    }

    #[test]
    fn with_external_roster_collects() {
        let d = Discovery::new().with_external_roster(EmptySource);
        assert_eq!(d.roster_count(), 1);
    }
}
