# xlb-net

[![crates.io](https://img.shields.io/crates/v/xlb-net.svg)](https://crates.io/crates/xlb-net)
[![docs.rs](https://docs.rs/xlb-net/badge.svg)](https://docs.rs/xlb-net)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A small, opinionated wrapper around [`iroh`](https://crates.io/crates/iroh) that
gives a process **one** ALPN-multiplexed QUIC endpoint, **one** persistent
Ed25519 identity per machine, and a composed discovery layer that merges mDNS,
[iroh-relay](https://crates.io/crates/iroh-relay) swarm discovery, statically
pinned peers, and externally-injected peer hints into a single pool.

It also ships an **embeddable relay server** for projects that want to host
their own relay infrastructure rather than rely on a public one.

```toml
[dependencies]
xlb-net = "0.1"
```

## Why

`iroh` exposes everything you need to build a peer-to-peer system, but most
applications want the same handful of things on top of it:

- A stable per-machine `NodeId` that survives restarts and is shared across
  every process on that machine.
- One `Endpoint` per process that multiplexes several application protocols by
  ALPN, instead of binding multiple UDP sockets.
- A discovery layer that **combines** sources rather than picks one — LAN mDNS
  for in-room peers, an iroh-relay swarm for the wider mesh, pinned seeds for
  bootstrap, plus a hook to inject hints from your own consensus / gossip /
  control-plane code.
- An iroh relay you can run yourself, bound to your own keypair, fronted by
  Let's Encrypt or your own PKI.

`xlb-net` is the single upgrade point for those concerns. Consumers import
`xlb_net::Endpoint`, not `iroh::Endpoint` — so when iroh ships a breaking
change (it is pre-1.0 as of May 2026), the churn lives in one crate.

## Quick start

```rust,no_run
use std::collections::HashMap;
use xlb_net::{Discovery, Endpoint, Keypair};

# async fn run() -> xlb_net::Result<()> {
let keypair = Keypair::load_or_create()?;

let endpoint = Endpoint::builder(keypair, &["myapp/v1"])
    .discovery(
        Discovery::new()
            .with_lan()                           // mDNS on the local subnet
            .with_relays(xlb_net::default_relays()), // iroh public relay swarm
    )
    .bind()
    .await?;

println!("our node id is {}", endpoint.node_id());

// Accept incoming streams, dispatched by ALPN.
let mut handlers: HashMap<&'static [u8], _> = HashMap::new();
handlers.insert(b"myapp/v1".as_slice(), |conn| Box::pin(async move {
    // handle connection…
    Ok(())
}));

endpoint.accept_dispatch(handlers).await?;
# Ok(()) }
```

## What's in the box

### Per-machine identity — `Keypair`

`Keypair::load_or_create()` reads (or atomically creates on first run) an
Ed25519 secret key at the platform-appropriate `data_local_dir`:

| Platform | Path |
|---|---|
| Linux   | `~/.local/share/yah/identity.ed25519` |
| macOS   | `~/Library/Application Support/dev.yah.yah/identity.ed25519` |
| Windows | `%LocalAppData%\yah\yah\data\identity.ed25519` |

The file is written with mode `0600`, atomically (`tmp + rename`) so concurrent
processes converge on the same key. The corresponding `NodeId` is written
alongside as `identity.pub` for human inspection.

### One Endpoint, many protocols — `Endpoint`

```rust,ignore
let endpoint = Endpoint::builder(keypair, &["app/control/v1", "app/data/v1"])
    .discovery(discovery)
    .bind().await?;
```

The endpoint wraps `iroh::Endpoint` and exposes:

- `node_id() / endpoint_id()` — your `NodeId` (re-exported from iroh).
- `endpoint_addr()` — your current `EndpointAddr` snapshot.
- `connect_alpn(addr, alpn)` — outbound, ALPN-typed.
- `accept_dispatch(handlers)` — inbound accept loop that routes each incoming
  connection to the handler whose ALPN matches.

### Discovery, four lanes, merged — `Discovery`

```rust,ignore
let discovery = Discovery::new()
    .with_lan()                                    // mDNS on the local subnet
    .with_relays(xlb_net::default_relays())        // iroh-relay swarm
    .with_static(vec![pinned_addr])                // pinned NodeIds
    .with_external_roster(my_peer_source);         // anything you want
```

`PeerSource` is a tiny trait — implement it to feed peer hints from your own
consensus, gossip, mDNS+BLE bridge, raft state, etc. `xlb-net` merges those
hints into the same pool that mDNS and the relay swarm feed, with provenance
tagging.

```rust,ignore
pub trait PeerSource: Send + Sync + 'static {
    fn subscribe(&self) -> PeerHintStream;
}

pub enum PeerHint {
    Found(EndpointAddr),
    Lost(NodeId),
}
```

This is the seam that lets `xlb-net` stay agnostic of your application's
membership protocol while still benefiting from it.

### Embeddable relay — `relay::Server`

```rust,ignore
use xlb_net::relay;

let server = relay::Server::builder()
    .https_bind("0.0.0.0:443".parse()?)
    .quic_bind("0.0.0.0:7842".parse()?)
    .tls_letsencrypt(relay::AcmeConfig {
        directory: relay::AcmeDirectory::Production,
        domains: vec!["relay.example.com".into()],
        contact: vec!["mailto:ops@example.com".into()],
        cache_dir: "/var/lib/myapp/acme".into(),
    })
    .start()
    .await?;

// Hand the URL to the rest of your fleet.
let url = server.https_url().expect("https configured");
```

Three TLS modes: `tls_self_signed()` (loopback test fixtures),
`tls_letsencrypt(acme)` (TLS-ALPN-01 against any ACME directory — production,
staging, or a local Pebble), and `tls_manual(rustls_config)` for BYO PKI.

Clients point at the server with `RelayMap`:

```rust,ignore
use xlb_net::{Endpoint, RelayMap, RelayUrl};

let relay_map = RelayMap::from(RelayUrl::from(server_url.parse()?));

let endpoint = Endpoint::builder(keypair, &["myapp/v1"])
    .relay_map(relay_map)
    .bind().await?;
```

## Status

`xlb-net` follows iroh's `1.0.0-rc.0` line. The crate is `0.1.x` — the public
API is expected to stabilize alongside iroh's 1.0 release.

What's stable in `0.1`:

- `Keypair::load_or_create` path layout.
- `Endpoint::builder(keypair, alpns).discovery(…).bind()` shape.
- `Discovery::new()` + the four `with_*` lanes.
- `PeerSource` trait + `PeerHint` enum.
- `relay::Server::builder()` with the three TLS modes.

What may move in `0.2`:

- The `tls_self_signed` constructor may move behind a `test-utils` feature
  flag so production builds don't pull `rcgen` and the iroh-relay test fixtures.
- `default_relays()` may grow knobs for picking between the public iroh relays
  and a self-hosted set.

## Re-exports

`xlb-net` re-exports the iroh types you'll need at the API boundary so
consumers don't have to take a direct iroh dep:

- `xlb_net::EndpointId` (alias `NodeId`), `EndpointAddr` (alias `NodeAddr`)
- `xlb_net::SecretKey`
- `xlb_net::RelayMap`, `RelayMode`, `RelayUrl`

If iroh ships a breaking change, the version bump lives here.

## Minimum supported Rust version

Rust 1.85 (2024 edition support is not required — this crate uses edition
2021).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../../LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual-licensed as above, without any additional terms or
conditions.
