# Changelog

## [0.1.0] - 2026-05-11

### Added

- `AssetClass` — per-app namespace for content-addressed blobs; `register()`, `asset()`, `set_role()`, `governor()`, `set_battery()`, `set_metered()`
- `Asset` — handle to a single blob; `fetch()`, `is_cached()`, `hash()`
- `BlakeHash` — BLAKE3 content-hash with `hash()`, `verify()`, `from_hex()`, `to_hex()`, `FromStr`, `Display`
- `BandwidthPolicy` — per-tier upload/download caps with builder API
- `BwCaps` — upload/download cap pair; `passive()` constructor
- `SeedRole` — `Passive` / `Participant` / `Permanent`
- `PeerTier` — `Cloud` / `Rig` / `Workstation` / `Mobile`
- `BandwidthGovernor` — wraps `BandwidthPolicy`; auto-governors for battery and metered connections; `probe_os()`, `set_battery()`, `set_metered()`, `effective_caps()`, `is_passive()`
- `Discovery` — LAN (mDNS) + swarm (iroh-relay) + static seeds; `default()`, `lan_only()`, `none()`, `with_relays()`
- Five-tier fetch chain: local cache → LAN peers → swarm peers → permanent seeds → CDN edge
- HTTP CDN fallback adapter (`reqwest` + rustls) with BLAKE3 verification on each response
- iroh-blobs transport adapter over `xlb-net::Endpoint` for tiers 2–4
- `testing::{MockSwarm, MockPeer}` — in-process N-peer test harness; no real network required
