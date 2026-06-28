# Changelog

## [0.8.16](https://github.com/yah-ai/xlb/compare/xlb-net-v0.8.13...xlb-net-v0.8.16) - 2026-06-28

### Other

- v0.8.16 (warden→yubaba/constable→kamaji rename)
- tag release
- wip retag

## [0.1.0] - 2026-05-11

### Added

- `Keypair` — per-machine Ed25519 identity; `load_or_create()` reads or atomically creates a key at the platform data dir; persists `identity.pub` alongside for inspection
- `Endpoint` — ALPN-multiplexed QUIC endpoint wrapping `iroh::Endpoint`; builder API with `discovery()` and `bind()`; `node_id()`, `connect_alpn()`, `accept_dispatch()`
- `Discovery` — composed peer discovery: `with_lan()` (mDNS), `with_relays()` (iroh-relay swarm), `with_static()` (pinned NodeIds), `with_external_roster()` (custom `PeerSource`)
- `PeerSource` trait — implement to feed peer hints from any membership protocol into xlb-net's discovery pool
- `relay::Server` — embeddable iroh relay; builder with `https_bind()`, `quic_bind()`, `tls_self_signed()`, `tls_letsencrypt()`, `tls_manual()`
- Re-exports: `NodeId`, `NodeAddr`, `SecretKey`, `RelayMap`, `RelayMode`, `RelayUrl`
- `default_relays()` — returns the public iroh relay set
