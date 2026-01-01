# xlb

[![crates.io](https://img.shields.io/crates/v/xlb.svg)](https://crates.io/crates/xlb)
[![docs.rs](https://docs.rs/xlb/badge.svg)](https://docs.rs/xlb)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**Chunked, Bao-verified blob distribution — per-app, content-addressed.**

xlb distributes large immutable blobs (ML models, container images, WASM bundles, datasets)
from an app to its install base over a tiered fetch chain, with BLAKE3 Bao-tree verification
on every chunk regardless of where it came from.

Reads as **peer-eXchange + LAN + Bao-tree**. Mnemonic: each blob is a xiaolongbao — content
sealed in a verified wrapper, served from a kitchen, shareable across tables.

## When to use xlb

xlb fits when:

- Your app ships large blobs (≥10 MB) to a known install base.
- CDN egress costs at scale are a concern.
- You want peer-assisted distribution without IPFS's complexity or BitTorrent's tracker baggage.
- You want BLAKE3 content-addressing and per-chunk verification as a first-class primitive.

xlb is **not** a general-purpose DHT. It is scoped to a single app's peer universe — your
install base, bounded and app-controlled.

## Quick start

```toml
[dependencies]
xlb = "0.1"
```

Register an asset class once at app startup:

```rust
use xlb::{AssetClass, AssetClassConfig, BandwidthPolicy, BwCaps, Discovery, PeerTier, SeedRole};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let class = AssetClass::register(AssetClassConfig {
        name: "my-app-models",
        permanent_seeds: vec![
            // Ed25519 NodeIds of your always-on seed servers.
            // These are pinned in your binary so rogue peers can't impersonate them.
            "ab12cd34ef56…".to_string(),
        ],
        cdn_fallback: Some("https://blobs.example.com/models/{blake3}".into()),
        discovery: Discovery::default(),  // LAN (mDNS) + swarm (iroh-relay) + static seeds
        bandwidth: BandwidthPolicy::default()
            .role(PeerTier::Cloud,       BwCaps { up_mbit: 1000, down_mbit: 10_000 })
            .role(PeerTier::Workstation, BwCaps { up_mbit: 5,    down_mbit: 50 })
            .role(PeerTier::Mobile,      BwCaps::passive()),
        cache_dir: dirs::cache_dir().map(|d| d.join("my-app/blobs")),
        cache_budget_bytes: 5 * 1024 * 1024 * 1024, // 5 GB LRU
    }).await?;

    // Tell xlb what role this process plays (set from your app's runtime context).
    class.set_role(SeedRole::Participant, PeerTier::Workstation).await?;

    // Fetch a blob — streams, verifies BLAKE3, caches, and starts seeding.
    let hash = "3a1fbc…c0de".parse()?;
    let bytes = class.asset(hash).fetch().await?;
    println!("fetched {} bytes", bytes.len());

    Ok(())
}
```

See [`examples/register_class.rs`](examples/register_class.rs) for a runnable version.

## Five-tier fetch chain

Given a `BlakeHash`, xlb tries sources in priority order, verifies BLAKE3 on every byte,
and falls through on failure or mismatch:

| # | Source | Typical latency | Cost |
|---|---|---|---|
| 1 | Local cache | µs | free |
| 2 | LAN peers (mDNS) | <1 ms | free |
| 3 | Swarm peers (iroh QUIC) | 10–100 ms | peer bandwidth |
| 4 | Permanent seeds (pinned NodeIds) | 50–200 ms | seed bandwidth |
| 5 | CDN edge (HTTPS Range) | 50–200 ms | egress $ |

For blobs > 10 MB, xlb fetches chunks concurrently from multiple sources — a 200 MB blob
across LAN + swarm + CDN finishes in roughly the time of the slowest single chunk, not their
sum. A poisoned chunk (hash mismatch) drops that source from the dispatch pool; other sources
continue. A malicious LAN peer cannot poison a download.

## Bandwidth policy

Each `AssetClass` carries a `BandwidthPolicy` with per-peer-tier upload/download caps.
Two auto-governors layer on top: **battery detection** and **metered-connection detection** —
when either triggers, the class drops to `Passive` (fetch only, no seeding) regardless of
configured policy.

```rust
use xlb::{BandwidthGovernor, BandwidthPolicy, BwCaps, PeerTier};

let policy = BandwidthPolicy::default()
    .role(PeerTier::Cloud,       BwCaps { up_mbit: 1000, down_mbit: 10_000 })
    .role(PeerTier::Rig,         BwCaps { up_mbit: 10,   down_mbit: 100 })
    .role(PeerTier::Workstation, BwCaps { up_mbit: 5,    down_mbit: 50 })
    .role(PeerTier::Mobile,      BwCaps::passive());

let gov = BandwidthGovernor::new(policy);
gov.probe_os();  // reads OS power state at startup; call again on power events

assert_eq!(gov.effective_caps(PeerTier::Workstation).up_mbit, 5); // on AC
gov.set_battery(true);
assert_eq!(gov.effective_caps(PeerTier::Workstation).up_mbit, 0); // on battery → passive
```

`AssetClass` exposes `governor()`, `set_battery()`, and `set_metered()` so you can wire
OS power notifications directly into the class.

## Testing with MockSwarm

xlb ships `xlb::testing::MockSwarm` — an in-process N-peer network that doesn't require
real iroh or network I/O. Use it to test your own xlb integration:

```rust
use xlb::{BlakeHash, testing::{MockPeer, MockSwarm}};

#[tokio::test]
async fn fetches_from_lan_peer() {
    let blob = b"hello world";
    let peer = MockPeer::new().with_blob(blob);

    let class = MockSwarm::new()
        .with_lan_peer(peer)
        .build_class("test-assets")
        .await;

    let bytes = class.asset(BlakeHash::hash(blob)).fetch().await.unwrap();
    assert_eq!(&bytes[..], blob);
}
```

`MockPeer::with_blob_at(data, tier)` lets you place blobs at specific tiers to verify
chain ordering and fallthrough behavior.

## Peer roles

`SeedRole` controls how this process participates in the class swarm:

| Role | Behavior | Typical context |
|---|---|---|
| `Passive` | Fetch only; never seed to remote peers | opted-out user, metered/battery |
| `Participant` | Fetch + seed under bandwidth caps | desktop install, server rig |
| `Permanent` | Fetch + seed without caps; always-on | cloud seed node, CDN-backing infra |

Set the role at startup from your app's runtime context:
```rust
class.set_role(SeedRole::Permanent, PeerTier::Cloud).await?;
```

## Substrate

- **[xlb-net](https://crates.io/crates/xlb-net)** — iroh wrapper: per-machine Ed25519 identity,
  ALPN-multiplexed `Endpoint`, and composed peer discovery (mDNS / iroh-relay / pinned / external).
  xlb depends on xlb-net; if you only need transport + identity (no blob layer), depend on xlb-net
  directly.
- **iroh-blobs** (Apache-2.0) — BLAKE3-native, Bao-tree-verified blob protocol over QUIC.
- **reqwest** with rustls (MIT/Apache-2.0, no native TLS) — CDN HTTP fallback.

## Status

**xlb-4 complete.** Core types, iroh-blobs transport, HTTP CDN fallback + Bao verifier glue,
and bandwidth governors (battery/metered auto-detection) are implemented and tested (27 unit +
2 integration tests pass on every commit).

`0.1.x` — the public API is expected to stabilize alongside iroh's 1.0 release.

**Stable in `0.1`:**
- `AssetClass::register`, `AssetClassConfig`, `Asset::fetch`, `Asset::is_cached`
- `BandwidthPolicy`, `BwCaps`, `SeedRole`, `PeerTier`
- `BlakeHash` (hash/verify/hex/`Display`/`FromStr`)
- `BandwidthGovernor` with `probe_os`, `set_battery`, `set_metered`, `effective_caps`
- `testing::{MockSwarm, MockPeer}`

**May move in `0.2`:**
- LAN + swarm discovery wiring (mDNS and iroh-relay integration land in xlb-5).
- `fetch_stream()` streaming API (currently only `fetch() -> Bytes` is stable).

## Minimum supported Rust version

Rust 1.85 (edition 2021).

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual-licensed as above,
without any additional terms or conditions.
