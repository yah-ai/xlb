//! Minimal xlb setup: register an asset class and inspect bandwidth state.
//!
//!   cargo run --example register_class

use xlb::{
    AssetClass, AssetClassConfig, BandwidthPolicy, BwCaps, Discovery, PeerTier, SeedRole,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Register an asset class once at startup.
    // Hold this `AssetClass` handle in your app state for the process lifetime.
    // Drop the last clone to stop seeding gracefully.
    let class = AssetClass::register(AssetClassConfig {
        name: "my-app-assets",
        // Pin the NodeIds of your always-on seed servers.
        // Rogue peers cannot impersonate a pinned NodeId — they don't hold the key.
        permanent_seeds: vec![],
        // CDN fallback URL — xlb substitutes {blake3} with the hex hash.
        // Supports R2, S3, Cloudflare CDN, or any HTTPS URL that handles Range.
        cdn_fallback: Some("https://cdn.example.com/assets/{blake3}".into()),
        discovery: Discovery::default(), // LAN (mDNS) + swarm (iroh-relay) + static seeds
        bandwidth: BandwidthPolicy::default()
            .role(PeerTier::Cloud, BwCaps { up_mbit: 1000, down_mbit: 10_000 })
            .role(PeerTier::Camp, BwCaps { up_mbit: 10, down_mbit: 100 })
            .role(PeerTier::Workstation, BwCaps { up_mbit: 5, down_mbit: 50 })
            .role(PeerTier::Mobile, BwCaps::passive()),
        // Persistent disk cache.  Pass `None` for an in-memory-only cache.
        cache_dir: None,
        cache_budget_bytes: 1024 * 1024 * 1024, // 1 GB LRU
    })
    .await?;

    // Tell xlb what role this process plays.
    // Derive this from your app's runtime context:
    //   cloud seed node → Permanent / Cloud
    //   server camp      → Participant / Camp
    //   desktop install → Participant / Workstation
    //   mobile / metered → Passive
    class.set_role(SeedRole::Participant, PeerTier::Workstation).await?;

    // Check whether the auto-governors have forced passive mode.
    // probe_os() reads the OS power state at startup; call it again on
    // platform power-change events (e.g. NSWorkspaceDidChangeNotification).
    let gov = class.governor();
    let caps = gov.effective_caps(PeerTier::Workstation);
    let seeding = if gov.is_passive() { "paused (battery or metered)" } else { "active" };

    println!("class:     {}", class.name());
    println!("seeding:   {} — {} Mbit/s up, {} Mbit/s down", seeding, caps.up_mbit, caps.down_mbit);
    println!();
    println!("To fetch a blob once you have its BLAKE3 hash:");
    println!("  let hash: xlb::BlakeHash = \"<64-char hex>\".parse()?;");
    println!("  let bytes = class.asset(hash).fetch().await?;");
    println!();
    println!("fetch() tries: cache → LAN peers → swarm → seeds → CDN (in that order).");
    println!("Every byte is BLAKE3-verified regardless of source.");

    Ok(())
}
