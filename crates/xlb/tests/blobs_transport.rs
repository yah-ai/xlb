//! Integration test: two real iroh peers round-trip a 10 MB blob via iroh-blobs.
//!
//! This is the acceptance test for xlb-2. No mocks — two mshr Endpoints
//! connect over loopback QUIC, the iroh-blobs wire protocol transfers the blob,
//! and xlb's top-level BLAKE3 verifies integrity.

use bytes::Bytes;
use xlb::{
    transport::BlobTransport,
    AssetClass, AssetClassConfig, FetchTier,
};
use mshr::{Discovery, Endpoint, Keypair};

/// Spin two in-process iroh peers. Alice seeds a 10 MB blob; Bob fetches it
/// via iroh-blobs and xlb's fetch chain. Verifies BLAKE3 at the end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blobs_transport_round_trip() {
    // ── Alice (seeder) ────────────────────────────────────────────────────────

    let alice_ep = Endpoint::builder()
        .keypair(Keypair::generate())
        .bind()
        .await
        .expect("alice bind");

    let alice = BlobTransport::new(alice_ep.clone())
        .await
        .expect("alice transport");

    // Add a 10 MB blob to Alice's local store.
    let data: Vec<u8> = (0u8..=255).cycle().take(10 * 1024 * 1024).collect();
    let hash = alice
        .add_blob(Bytes::from(data.clone()))
        .await
        .expect("add_blob");

    // ── Bob (fetcher) ─────────────────────────────────────────────────────────

    // Static discovery: Bob pre-loads Alice's full EndpointAddr so he can
    // resolve it without mDNS or relay-mediated discovery.
    let alice_addr = alice.node_addr();

    let bob_ep = Endpoint::builder()
        .keypair(Keypair::generate())
        .discovery(Discovery::new().with_static([alice_addr.clone()]))
        .bind()
        .await
        .expect("bob bind");

    let bob = BlobTransport::new(bob_ep)
        .await
        .expect("bob transport");

    // Wire Bob's iroh fetcher into an AssetClass at the Seed tier.
    let class = AssetClass::register(AssetClassConfig::default())
        .await
        .expect("register class");
    bob.attach_fetcher(&class, alice_addr, FetchTier::Seed);

    // ── Fetch and verify ──────────────────────────────────────────────────────

    let fetched = class
        .asset(hash)
        .fetch()
        .await
        .expect("fetch failed");

    assert_eq!(fetched.len(), 10 * 1024 * 1024, "size mismatch");
    assert_eq!(&fetched[..], &data[..], "content mismatch");
    assert!(hash.verify(&fetched), "BLAKE3 root verification failed");

    // ── Graceful shutdown ─────────────────────────────────────────────────────

    alice.shutdown().await;
    bob.shutdown().await;
}

/// Verify that the xlb-level cache-hit path short-circuits on the second fetch
/// (no second iroh-blobs connection required).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blobs_transport_cache_hit_on_second_fetch() {
    let alice_ep = Endpoint::builder()
        .keypair(Keypair::generate())
        .bind()
        .await
        .expect("alice bind");
    let alice = BlobTransport::new(alice_ep.clone()).await.expect("alice");

    let data = b"cache-hit smoke test";
    let hash = alice.add_blob(Bytes::from_static(data)).await.expect("add");

    let alice_addr = alice.node_addr();
    let bob_ep = Endpoint::builder()
        .keypair(Keypair::generate())
        .discovery(Discovery::new().with_static([alice_addr.clone()]))
        .bind()
        .await
        .expect("bob bind");
    let bob = BlobTransport::new(bob_ep).await.expect("bob");

    let class = AssetClass::register(AssetClassConfig::default())
        .await
        .expect("register");
    bob.attach_fetcher(&class, alice_addr, FetchTier::Seed);

    let asset = class.asset(hash);

    // First fetch: hits iroh-blobs transport.
    assert!(!asset.is_cached().await, "should not be cached yet");
    let first = asset.fetch().await.expect("first fetch");
    assert_eq!(&first[..], data);

    // Second fetch: hits xlb's in-memory cache, no iroh-blobs connection needed.
    assert!(asset.is_cached().await, "should be cached after first fetch");
    let second = asset.fetch().await.expect("second fetch");
    assert_eq!(&second[..], data);

    alice.shutdown().await;
    bob.shutdown().await;
}
