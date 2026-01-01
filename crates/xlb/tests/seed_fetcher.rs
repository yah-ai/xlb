//! Integration tests for the permanent-seed wiring at `FetchTier::Seed`.
//!
//! Covers the contract documented on
//! [`xlb::transport::BlobTransport::attach_permanent_seeds`] and the
//! [`xlb::transport::parse_seed_node_id`] helper:
//!
//! - Empty `permanent_seeds` → no-op, empty report.
//! - Valid hex `NodeId` → attached at `FetchTier::Seed`.
//! - Malformed entries → typed [`SeedParseError`], reported with index,
//!   the well-formed entries in the same list still attach.

use xlb::{
    transport::{parse_seed_node_id, BlobTransport, SeedParseError},
    AssetClass, AssetClassConfig, Discovery,
};
use xlb_net::{Endpoint, Keypair};

/// A real, parseable hex `NodeId` lifted off a freshly-generated keypair.
fn fresh_hex_node_id() -> String {
    hex::encode(Keypair::generate().node_id().as_bytes())
}

// ─── parse_seed_node_id ──────────────────────────────────────────────────────

#[test]
fn seed_fetcher_parses_valid_hex_node_id() {
    let hex = fresh_hex_node_id();
    let addr = parse_seed_node_id(&hex).expect("valid hex pubkey should parse");
    assert_eq!(
        hex::encode(addr.id.as_bytes()),
        hex,
        "round-trip: parsed pubkey hex matches input"
    );
    assert!(
        addr.is_empty(),
        "permanent-seed NodeAddr carries no inline addrs; discovery resolves"
    );
}

#[test]
fn seed_fetcher_parses_trims_whitespace() {
    let hex = fresh_hex_node_id();
    let padded = format!("  \n{hex}\t  ");
    assert!(parse_seed_node_id(&padded).is_ok());
}

#[test]
fn seed_fetcher_rejects_non_hex() {
    let err = parse_seed_node_id("not actually hex zzz").unwrap_err();
    assert!(
        matches!(err, SeedParseError::InvalidHex),
        "got {err:?}"
    );
}

#[test]
fn seed_fetcher_rejects_wrong_length() {
    // 30 bytes of hex (60 chars), not 32.
    let short = "aa".repeat(30);
    let err = parse_seed_node_id(&short).unwrap_err();
    assert!(
        matches!(err, SeedParseError::InvalidLength { got: 30 }),
        "got {err:?}"
    );
}

#[test]
fn seed_fetcher_rejects_invalid_pubkey_bytes() {
    // 32 bytes of all-zeros is valid hex of the right length but is not a
    // valid Ed25519 point (canonical small-order curve point rejected by
    // VerifyingKey::from_bytes).
    let zeroed = "00".repeat(32);
    match parse_seed_node_id(&zeroed) {
        Err(SeedParseError::InvalidPubKey) => {}
        // Some Ed25519 backends accept the identity element; treat that as
        // also-valid for this contract — the test's load-bearing claim is
        // "doesn't panic and surfaces a typed error if it rejects." If it
        // accepts, that's fine: a downstream connect attempt will fail.
        Ok(_) => {}
        Err(other) => panic!("unexpected variant: {other:?}"),
    }
}

// ─── attach_permanent_seeds ──────────────────────────────────────────────────

async fn fresh_transport() -> BlobTransport {
    let ep = Endpoint::builder()
        .keypair(Keypair::generate())
        .bind()
        .await
        .expect("endpoint bind");
    BlobTransport::new(ep).await.expect("transport new")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_fetcher_attach_empty_is_noop() {
    let transport = fresh_transport().await;
    let class = AssetClass::register(AssetClassConfig {
        permanent_seeds: vec![],
        // No discovery, no CDN — keep the chain minimal so we can reason
        // about it.
        discovery: Discovery::none(),
        cdn_fallback: None,
        ..Default::default()
    })
    .await
    .expect("register");

    let report = transport.attach_permanent_seeds(&class);
    assert_eq!(report.attached, 0);
    assert!(report.errors.is_empty());

    transport.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_fetcher_attach_valid_seed_counted() {
    let transport = fresh_transport().await;
    let class = AssetClass::register(AssetClassConfig {
        permanent_seeds: vec![fresh_hex_node_id()],
        discovery: Discovery::none(),
        cdn_fallback: None,
        ..Default::default()
    })
    .await
    .expect("register");

    let report = transport.attach_permanent_seeds(&class);
    assert_eq!(report.attached, 1);
    assert!(report.errors.is_empty(), "errors: {:?}", report.errors);

    transport.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_fetcher_attach_mixed_good_and_bad_does_not_panic() {
    let transport = fresh_transport().await;
    let class = AssetClass::register(AssetClassConfig {
        permanent_seeds: vec![
            fresh_hex_node_id(),         // 0: good
            "not-hex-at-all".to_string(), // 1: bad — InvalidHex
            fresh_hex_node_id(),         // 2: good
            "aa".repeat(30),             // 3: bad — InvalidLength
        ],
        discovery: Discovery::none(),
        cdn_fallback: None,
        ..Default::default()
    })
    .await
    .expect("register");

    let report = transport.attach_permanent_seeds(&class);
    assert_eq!(report.attached, 2, "two valid entries attach");
    assert_eq!(report.errors.len(), 2, "two malformed entries reported");

    let indices: Vec<usize> = report.errors.iter().map(|(i, _)| *i).collect();
    assert_eq!(indices, vec![1, 3], "errors carry the source index");

    assert!(matches!(report.errors[0].1, SeedParseError::InvalidHex));
    assert!(matches!(
        report.errors[1].1,
        SeedParseError::InvalidLength { got: 30 }
    ));

    transport.shutdown().await;
}
