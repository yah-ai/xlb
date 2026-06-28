//! @arch:layer(core)
//! @arch:role(net)
//!
//! `xlb-net` — shared iroh-based transport substrate for the yah/xlb crate
//! family. Wraps `iroh` so consumers (`xlb`, `yubaba`, noisetable's
//! `society`) import `xlb_net::Endpoint` rather than `iroh::Endpoint`,
//! giving us a single upgrade point for the pre-1.0 substrate and a
//! transparent fork escape hatch.
//!
//! Phase 1 (R105-F1) ships:
//! - [`Keypair`]: per-machine Ed25519 secret loaded from
//!   `<data-local>/yah/identity.ed25519`.
//! - [`Endpoint`]: thin wrapper around `iroh::Endpoint`, built via
//!   [`EndpointBuilder`] with a configured keypair + ALPN list, exposing a
//!   simple [`Endpoint::accept`] loop that dispatches incoming connections
//!   by ALPN to a caller-supplied async handler.
//! - Re-exports of `iroh::NodeId` and `iroh::SecretKey` for ergonomics.
//!
//! Discovery aggregation (mDNS / swarm / static / external roster) and the
//! embeddable `relay::Server` land in subsequent sub-tickets (R105-F2..F4).

pub mod discovery;
pub mod endpoint;
pub mod keypair;
pub mod relay;

pub use discovery::{default_relays, Discovery, PeerHint, PeerHintStream, PeerSource};
pub use endpoint::{Endpoint, EndpointBuilder};
pub use iroh::endpoint::{Connection, Incoming, RecvStream, SendStream};
pub use keypair::Keypair;

// Re-export iroh's `RelayMap`/`RelayMode` so consumers can build custom
// relay configurations without taking a direct iroh dep. `RelayUrl` for
// constructing `RelayMap` from a known URL.
pub use iroh::{RelayMap, RelayMode, RelayUrl};

// iroh 1.0 renamed `NodeId`/`NodeAddr` to `EndpointId`/`EndpointAddr`. The
// xlb-net arch doc still speaks of `NodeId` (and society/yubaba's design
// notes do too), so we expose both spellings as aliases. New code should
// prefer `NodeId`/`NodeAddr` for cross-consumer consistency; either is fine
// inside the crate boundary.
pub use iroh::{EndpointAddr, EndpointId, SecretKey};
pub type NodeId = EndpointId;
pub type NodeAddr = EndpointAddr;

/// `Result` alias for fallible `xlb-net` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error for the crate. Most public APIs return [`Result`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("identity dir unavailable: no platform data-local dir")]
    NoDataDir,

    #[error("malformed keypair file: {0}")]
    Keypair(String),

    #[error("endpoint: {0}")]
    Endpoint(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
