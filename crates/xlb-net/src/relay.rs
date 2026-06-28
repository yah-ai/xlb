//! Embeddable iroh-relay server. Wraps `iroh_relay::server::Server` with a
//! yah-flavored builder so yubaba cloud nodes (and developer test camps)
//! can host the relay infrastructure that xlb-net's swarm/NAT-traversal
//! lanes depend on.
//!
//! ## What an iroh relay actually does
//!
//! Two services on one process, sharing one HTTPS listener:
//!
//! - **Relay (NAT-traversal proxy).** Endpoints behind symmetric NATs that
//!   can't holepunch each other tunnel their QUIC packets through the
//!   relay's WebSocket endpoint. The relay is on the data path only when
//!   direct paths fail; once holepunching succeeds, it falls out.
//! - **QUIC address-discovery (QAD) + pkarr publishing.** Endpoints learn
//!   their own observed external `(ip, port)` from the relay's QUIC
//!   service, and publish their `EndpointInfo` (NodeId → addrs) so peers
//!   can find them. This is what `Discovery::with_relays(url)` resolves
//!   against.
//!
//! Both services are TLS-terminated by the same cert; yubaba production
//! nodes use Let's Encrypt, dev/test camps use a self-signed cert.
//!
//! ## Three TLS modes
//!
//! ```ignore
//! // Production: Let's Encrypt via ACME TLS-ALPN-01 challenges.
//! let server = relay::Server::builder()
//!     .https_bind("0.0.0.0:443".parse()?)
//!     .quic_bind("0.0.0.0:7842".parse()?)
//!     .tls_letsencrypt(
//!         relay::AcmeConfig::production()
//!             .domain("relay.yah.dev")
//!             .contact("mailto:ops@yah.dev")
//!             .cache_dir("/var/lib/yah/acme"),
//!     )
//!     .start()
//!     .await?;
//!
//! // Local dev / unit tests: self-signed cert valid for localhost.
//! let server = relay::Server::builder()
//!     .https_bind("127.0.0.1:0".parse()?)
//!     .quic_bind("127.0.0.1:0".parse()?)
//!     .tls_self_signed()
//!     .start()
//!     .await?;
//!
//! // Custom: bring your own rustls::ServerConfig.
//! let server = relay::Server::builder()
//!     .https_bind("0.0.0.0:443".parse()?)
//!     .tls_manual(my_rustls_config)
//!     .start()
//!     .await?;
//! ```
//!
//! ## URL publish
//!
//! `Server::https_url()` and `Server::quic_addr()` give the operator the
//! values that need to land in the fleet's `Discovery::with_relays(...)`
//! configuration. Distribution itself is yubaba's job — see
//! `app/yah/desktop/` and the yubaba crate. xlb-net only owns the
//! "how do I host the relay" half.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use iroh_relay::server::{
    AcmeConfig as IrohAcmeConfig, CertConfig, Limits, QuicConfig, RelayConfig,
    Server as IrohServer, ServerConfig, TlsConfig,
};
use rustls::ServerConfig as RustlsServerConfig;
use url::Url;

use crate::{Error, Result};

// Re-export the typed `RelayMap`/`RelayConfig`/`RelayQuicConfig` triple so
// downstream callers (yubaba config, integration tests) can construct
// custom relay maps without taking a direct dep on `iroh-relay`.
pub use iroh_relay::{RelayConfig as ClientRelayConfig, RelayMap, RelayQuicConfig};

/// ACME (Let's Encrypt) configuration for [`ServerBuilder::tls_letsencrypt`].
///
/// Constructed via [`AcmeConfig::production`] (the real LE directory),
/// [`AcmeConfig::staging`] (LE staging — recommended for first-deploy
/// smoke), or [`AcmeConfig::custom`] (any RFC-8555 ACME directory URL —
/// useful for Pebble or step-ca in CI).
#[derive(Debug, Clone)]
pub struct AcmeConfig {
    directory_url: String,
    domains: Vec<String>,
    contacts: Vec<String>,
    cache_dir: Option<PathBuf>,
}

impl AcmeConfig {
    /// Real Let's Encrypt production directory. Subject to LE's rate
    /// limits — use [`AcmeConfig::staging`] while iterating.
    pub fn production() -> Self {
        Self::new("https://acme-v02.api.letsencrypt.org/directory")
    }

    /// Let's Encrypt staging directory. Issues untrusted but real ACME
    /// certs without burning production rate limits.
    pub fn staging() -> Self {
        Self::new("https://acme-staging-v02.api.letsencrypt.org/directory")
    }

    /// Point at any RFC-8555 ACME directory URL. Use this for Pebble or
    /// step-ca during integration tests.
    pub fn custom(directory_url: impl Into<String>) -> Self {
        Self::new(directory_url)
    }

    fn new(directory_url: impl Into<String>) -> Self {
        Self {
            directory_url: directory_url.into(),
            domains: Vec::new(),
            contacts: Vec::new(),
            cache_dir: None,
        }
    }

    /// Add a domain to the issued cert's SAN list. At least one domain is
    /// required; chain-call to add more.
    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.domains.push(domain.into());
        self
    }

    /// Add an account contact (must include the URI scheme, typically
    /// `mailto:ops@example.com`).
    pub fn contact(mut self, contact: impl Into<String>) -> Self {
        self.contacts.push(contact.into());
        self
    }

    /// Persist account key + issued cert under this directory so the
    /// server doesn't re-register from scratch on every restart. Strongly
    /// recommended for production.
    pub fn cache_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(path.into());
        self
    }
}

/// Internal: which TLS path the server should take.
enum TlsMode {
    /// Self-signed cert valid for `localhost` / `127.0.0.1` / `::1`.
    SelfSigned,
    /// ACME / Let's Encrypt.
    Acme(AcmeConfig),
    /// Caller-supplied `rustls::ServerConfig`.
    Manual(RustlsServerConfig),
}

/// Builder for [`Server`]. See module docs for examples.
pub struct ServerBuilder {
    http_bind_addr: Option<SocketAddr>,
    https_bind_addr: Option<SocketAddr>,
    quic_bind_addr: Option<SocketAddr>,
    tls: Option<TlsMode>,
    limits: Limits,
}

impl ServerBuilder {
    fn new() -> Self {
        Self {
            http_bind_addr: None,
            https_bind_addr: None,
            quic_bind_addr: None,
            tls: None,
            limits: Limits::default(),
        }
    }

    /// Bind a plain-HTTP listener at this address. iroh-relay always wants
    /// an HTTP listener for captive-portal probes and unauthenticated
    /// health checks; if TLS is configured, the main relay traffic moves
    /// to `https_bind_addr` and this socket only serves probes.
    ///
    /// If unset and TLS is enabled, defaults to `127.0.0.1:0` (OS-assigned
    /// loopback port) — production deployments should pin port 80.
    pub fn http_bind(mut self, addr: SocketAddr) -> Self {
        self.http_bind_addr = Some(addr);
        self
    }

    /// Bind the TLS-terminated HTTPS listener (carries the WebSocket
    /// relay path). Required when any `tls_*` mode is configured.
    /// Production deployments pin port 443.
    pub fn https_bind(mut self, addr: SocketAddr) -> Self {
        self.https_bind_addr = Some(addr);
        self
    }

    /// Bind the QUIC address-discovery listener. Required for endpoints
    /// to learn their observed external addresses and publish via pkarr.
    /// Production deployments pin port 7842
    /// ([`iroh_relay::defaults::DEFAULT_RELAY_QUIC_PORT`]).
    pub fn quic_bind(mut self, addr: SocketAddr) -> Self {
        self.quic_bind_addr = Some(addr);
        self
    }

    /// Use a self-signed cert valid for `localhost`, `127.0.0.1`, `::1`.
    /// Suitable for unit/integration tests; clients must opt into
    /// `Endpoint::builder().insecure_skip_tls_verify(true)` to accept it.
    pub fn tls_self_signed(mut self) -> Self {
        self.tls = Some(TlsMode::SelfSigned);
        self
    }

    /// Use Let's Encrypt (or another ACME directory) to provision certs
    /// via TLS-ALPN-01 challenges. Challenges are served on the same
    /// HTTPS port that the relay listens on (no separate :80 challenge
    /// handler needed).
    pub fn tls_letsencrypt(mut self, acme: AcmeConfig) -> Self {
        self.tls = Some(TlsMode::Acme(acme));
        self
    }

    /// Bring your own `rustls::ServerConfig`. The server config is used
    /// as-is — caller is responsible for cert chain, key, and any cert
    /// reloading. Useful when terminating in front of a corporate PKI.
    pub fn tls_manual(mut self, config: RustlsServerConfig) -> Self {
        self.tls = Some(TlsMode::Manual(config));
        self
    }

    /// Spawn the server. Returns a [`Server`] handle whose drop aborts
    /// the supervisor; call [`Server::shutdown`] for a graceful stop.
    pub async fn start(self) -> Result<Server> {
        let tls_mode = self
            .tls
            .ok_or_else(|| Error::Endpoint("relay::ServerBuilder: tls mode required".into()))?;

        // The relay always needs an HTTP listener (for captive-portal
        // probes, served plain-text). When TLS is on, main traffic moves
        // to https_bind_addr; HTTP becomes the probe-only socket.
        let http_bind_addr = self
            .http_bind_addr
            .unwrap_or_else(|| "127.0.0.1:0".parse().expect("static loopback parses"));
        let https_bind_addr = self.https_bind_addr.ok_or_else(|| {
            Error::Endpoint("relay::ServerBuilder: https_bind() is required when TLS is set".into())
        })?;

        let cert = match tls_mode {
            TlsMode::SelfSigned => {
                // iroh-relay's test-utils helper builds a `Manual` cert
                // for "localhost"/"127.0.0.1"/"::1". Reusing it keeps us
                // aligned with iroh's own test fixtures.
                let (_certs, server_config) =
                    iroh_relay::server::testing::self_signed_tls_certs_and_config();
                CertConfig::Manual { server_config }
            }
            TlsMode::Manual(server_config) => CertConfig::Manual { server_config },
            TlsMode::Acme(acme) => {
                if acme.domains.is_empty() {
                    return Err(Error::Endpoint(
                        "relay::ServerBuilder: ACME requires at least one domain".into(),
                    ));
                }
                // The relay injects an ACME-driven cert resolver into a
                // half-built rustls::ServerConfig.  We hand it the config
                // builder up to `WantsServerCert`; it adds the resolver.
                let server_config_builder = RustlsServerConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .map_err(|e| Error::Endpoint(format!("rustls protocol setup: {e}")))?
                .with_no_client_auth();

                let mut iroh_acme = IrohAcmeConfig::new(acme.directory_url)
                    .domains(acme.domains)
                    .contact(acme.contacts);
                if let Some(path) = acme.cache_dir {
                    iroh_acme = iroh_acme.cache_path(path);
                }
                CertConfig::LetsEncrypt {
                    acme_config: iroh_acme,
                    server_config_builder,
                }
            }
        };

        let tls = TlsConfig::new(https_bind_addr, cert);

        let mut relay_config = RelayConfig::new(http_bind_addr);
        relay_config.tls = Some(tls);
        relay_config.limits = self.limits;

        let quic_config = self.quic_bind_addr.map(QuicConfig::new);

        let mut server_config = ServerConfig::default();
        server_config.relay = Some(relay_config);
        server_config.quic = quic_config;

        let inner = IrohServer::spawn(server_config)
            .await
            .map_err(|e| Error::Endpoint(format!("relay spawn: {e}")))?;

        Ok(Server { inner })
    }
}

/// Running embedded iroh-relay server. `Drop` aborts the supervisor
/// task; for a graceful stop call [`Server::shutdown`].
pub struct Server {
    inner: IrohServer,
}

impl Server {
    /// Start a new builder. See module docs.
    pub fn builder() -> ServerBuilder {
        ServerBuilder::new()
    }

    /// HTTPS bind address (resolved port — useful when caller bound to
    /// `:0`). `None` if TLS isn't configured.
    pub fn https_addr(&self) -> Option<SocketAddr> {
        self.inner.https_addr()
    }

    /// Plain-HTTP bind address (probe port when TLS is on, main port
    /// otherwise).
    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.inner.http_addr()
    }

    /// QUIC bind address. `None` if `quic_bind` wasn't called.
    pub fn quic_addr(&self) -> Option<SocketAddr> {
        self.inner.quic_addr()
    }

    /// `https://<https_addr>` URL — what clients pass to
    /// `Discovery::with_relays(...)` (or to a [`RelayMap`] built via
    /// [`relay_map_for_https`]). `None` if TLS isn't configured.
    pub fn https_url(&self) -> Option<Url> {
        self.inner
            .https_addr()
            .map(|addr| Url::parse(&format!("https://{addr}")).expect("valid url"))
    }

    /// `http://<http_addr>` URL — only meaningful when TLS is *not*
    /// configured (the URL otherwise points at the probe port).
    pub fn http_url(&self) -> Option<Url> {
        self.inner
            .http_addr()
            .map(|addr| Url::parse(&format!("http://{addr}")).expect("valid url"))
    }

    /// Build a [`RelayMap`] suitable for `iroh::Endpoint::relay_mode(
    /// RelayMode::Custom(...))` from this server's HTTPS URL and QUIC
    /// port. `None` if either listener is missing.
    pub fn relay_map(&self) -> Option<RelayMap> {
        let url = self.https_url()?;
        let mut config = ClientRelayConfig::from(iroh::RelayUrl::from(url));
        if let Some(quic) = self.inner.quic_addr() {
            config.quic = Some(RelayQuicConfig::new(quic.port()));
        }
        Some(RelayMap::from(config))
    }

    /// Graceful shutdown. Waits for in-flight tasks to drain.
    pub async fn shutdown(self) -> Result<()> {
        self.inner
            .shutdown()
            .await
            .map_err(|e| Error::Endpoint(format!("relay shutdown: {e}")))
    }
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("relay::Server")
            .field("https_addr", &self.https_addr())
            .field("http_addr", &self.http_addr())
            .field("quic_addr", &self.quic_addr())
            .finish()
    }
}

/// Convenience: build a [`RelayMap`] for a known relay URL + QUIC port.
/// Mirrors what `Server::relay_map()` produces, but for the case where
/// the caller knows the URL out-of-band (production deploy, fleet config).
pub fn relay_map_for_https(url: Url, quic_port: Option<u16>) -> RelayMap {
    let mut config = ClientRelayConfig::from(iroh::RelayUrl::from(url));
    if let Some(p) = quic_port {
        config.quic = Some(RelayQuicConfig::new(p));
    }
    RelayMap::from(config)
}
