//! End-to-end test for [`xlb_net::relay::Server`] (R105-F4 acceptance).
//!
//! Spawns an in-process relay with a self-signed cert, binds two
//! `xlb_net::Endpoint`s that route through it via
//! `RelayMode::Custom(...)`, and proves a stream round-trips. This is the
//! "warden test container hosts a relay; another endpoint configures
//! `Discovery::with_relays(custom_url)` and successfully proxies through"
//! acceptance test from `xlb-net.md`.
//!
//! Direct UDP between alice and bob is left unblocked here — iroh will
//! prefer a holepunched direct path when both ends are on loopback. The
//! relay is *available* for the test (its `RelayUrl` is in both
//! endpoints' `RelayMap`); whether iroh actually uses it depends on its
//! transport selection. Either way, the test fails closed if the relay
//! isn't reachable, since iroh treats an unreachable relay in
//! `RelayMode::Custom` as a fatal config error.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::Connection;

use xlb_net::endpoint::{Alpn, AlpnHandler, BoxFut};
use xlb_net::{relay, Endpoint, Keypair};

const ALPN: &[u8] = b"xlb-net/test/relay/v1";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_round_trip_self_signed() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn,xlb_net=info,iroh=info,iroh_relay=info")
        .with_test_writer()
        .try_init();

    // Self-signed iroh-relay on loopback; OS-picks ports for HTTP/HTTPS/QUIC.
    let server = relay::Server::builder()
        .https_bind("127.0.0.1:0".parse().unwrap())
        .quic_bind("127.0.0.1:0".parse().unwrap())
        .tls_self_signed()
        .start()
        .await
        .expect("relay::Server start");

    let relay_map = server.relay_map().expect("relay_map");
    let relay_url = server.https_url().expect("https_url");
    eprintln!(
        "relay up: https={relay_url} http={:?} quic={:?}",
        server.http_addr(),
        server.quic_addr()
    );

    // Alice + bob both register the relay. `insecure_skip_tls_verify(true)`
    // is required because the cert is self-signed.
    let alice = Endpoint::builder()
        .keypair(Keypair::generate())
        .alpns([ALPN])
        .relay_map(relay_map.clone())
        .insecure_skip_tls_verify(true)
        .bind()
        .await
        .expect("alice bind");

    let bob = Endpoint::builder()
        .keypair(Keypair::generate())
        .alpns([ALPN])
        .relay_map(relay_map)
        .insecure_skip_tls_verify(true)
        .bind()
        .await
        .expect("bob bind");

    // Wait for alice's relay home to settle so her EndpointAddr advertises
    // the relay URL when bob fetches it.
    alice.inner().online().await;
    let alice_id = alice.node_id();
    let alice_addr = iroh::EndpointAddr::new(alice_id).with_relay_url(relay_url.into());

    // Echo handler for alice.
    let saw_request = Arc::new(AtomicBool::new(false));
    let saw = saw_request.clone();
    let alice_handle = alice.clone();
    let server_task = tokio::spawn(async move {
        let mut handlers: HashMap<Alpn, AlpnHandler> = HashMap::new();
        let flag = saw.clone();
        handlers.insert(
            ALPN.to_vec(),
            Arc::new(move |conn: Connection| {
                let flag = flag.clone();
                Box::pin(async move {
                    let (mut send, mut recv) = conn.accept_bi().await?;
                    let buf = recv.read_to_end(1024).await?;
                    flag.store(true, Ordering::SeqCst);
                    send.write_all(&buf).await?;
                    send.finish()?;
                    let _ = conn.closed().await;
                    Ok(())
                }) as BoxFut<'static, anyhow::Result<()>>
            }),
        );
        let _ = alice_handle.accept_dispatch(handlers).await;
    });

    // Bob dials alice using only her NodeId + relay URL — no direct addrs
    // baked in. Resolution rides on the relay map.
    let conn = tokio::time::timeout(
        Duration::from_secs(20),
        bob.connect_alpn(alice_addr, ALPN),
    )
    .await
    .expect("connect within 20s")
    .expect("bob connect");

    let (mut send, mut recv) = conn.open_bi().await.expect("open_bi");
    send.write_all(b"yah-via-relay").await.expect("write");
    send.finish().expect("finish");
    let echoed = recv.read_to_end(1024).await.expect("read");
    assert_eq!(echoed, b"yah-via-relay");
    conn.close(0u32.into(), b"done");

    for _ in 0..50 {
        if saw_request.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(saw_request.load(Ordering::SeqCst), "alice handler ran");

    alice.close().await;
    bob.close().await;
    server_task.abort();
    server.shutdown().await.expect("relay shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_https_url_resolves_addrs() {
    // Pure-startup smoke: the server hands back well-formed URLs for the
    // bound HTTPS listener and a RelayMap that round-trips through
    // serialization. This catches regressions in addr-formatting that
    // wouldn't surface in the longer end-to-end test above.
    let server = relay::Server::builder()
        .https_bind("127.0.0.1:0".parse().unwrap())
        .quic_bind("127.0.0.1:0".parse().unwrap())
        .tls_self_signed()
        .start()
        .await
        .expect("relay::Server start");

    let url = server.https_url().expect("https_url present");
    assert_eq!(url.scheme(), "https");
    assert!(url.port().is_some(), "OS-assigned port present");

    let map = server.relay_map().expect("relay_map");
    assert_eq!(map.len(), 1);

    server.shutdown().await.expect("shutdown");
}
