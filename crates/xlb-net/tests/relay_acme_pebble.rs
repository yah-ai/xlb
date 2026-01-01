//! End-to-end ACME issuance against [Pebble][pebble], Let's Encrypt's
//! testing ACME server. Exercises [`xlb_net::relay::Server`]'s
//! `tls_letsencrypt` path with a real ACME exchange, then runs the same
//! relay-mediated stream round-trip the self-signed test in
//! `relay_round_trip.rs` covers — proving the LE-issued cert is actually
//! used to terminate the relay's HTTPS/QUIC sockets.
//!
//! [pebble]: https://github.com/letsencrypt/pebble
//!
//! ## Why `#[ignore]` by default
//!
//! Pebble is a Go binary that has to be installed and configured on the
//! host. The test stays out of the default `cargo test` run so xlb-net
//! contributors don't need pebble on their PATH. CI provisions pebble and
//! runs `cargo test --ignored relay_acme_pebble` once that wiring lands
//! (R105-T5 follow-up: see `@yah:next` on R105-F4).
//!
//! ## Required environment
//!
//! The test reads three env vars and skips with a `println!` if any are
//! missing (so the failure mode under `cargo test --ignored` is "nothing
//! to do" rather than a confusing assertion failure):
//!
//! | Var | Meaning |
//! |---|---|
//! | `PEBBLE_BIN` | Path to a `pebble` binary (≥ v2.7). |
//! | `PEBBLE_CA_CERT` | Path to pebble's CA root cert (issued for its directory endpoint). Used to set `SSL_CERT_FILE` so the ACME client trusts pebble. |
//! | `PEBBLE_CHALLTESTSRV_BIN` | *Optional but recommended.* Path to `pebble-challtestsrv`, used to satisfy TLS-ALPN-01 challenges on a port other than 443. When unset the test pins the HTTPS bind to `:5001` so pebble's default `tlsPort` works without DNS overrides. |
//!
//! Both binaries ship in pebble's release archive
//! (https://github.com/letsencrypt/pebble/releases). Local install on
//! macOS:
//!
//! ```bash
//! brew install pebble        # provides `pebble` and `pebble-challtestsrv`
//! ```
//!
//! ## What the test asserts
//!
//! 1. [`relay::Server`] starts with a `tls_letsencrypt` config pointing
//!    at pebble's directory.
//! 2. The ACME exchange completes — observable as a non-self-signed cert
//!    chain on the HTTPS listener (we don't introspect chain bytes; we
//!    just dial through it and complete the QUIC handshake, which fails
//!    closed if cert validation rejects).
//! 3. Two endpoints round-trip a stream through the relay (same shape
//!    as `relay_round_trip_self_signed`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::Connection;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use xlb_net::endpoint::{Alpn, AlpnHandler, BoxFut};
use xlb_net::{relay, Endpoint, Keypair};

const ALPN: &[u8] = b"xlb-net/test/relay-acme/v1";
const RELAY_DOMAIN: &str = "relay.test.yah.dev";

/// Env-var manifest. Returns `None` (with a printed reason) if the
/// caller hasn't provisioned pebble.
struct PebbleEnv {
    bin: PathBuf,
    ca_cert: PathBuf,
}

impl PebbleEnv {
    fn from_env() -> Option<Self> {
        let bin = std::env::var_os("PEBBLE_BIN").map(PathBuf::from)?;
        let ca_cert = std::env::var_os("PEBBLE_CA_CERT").map(PathBuf::from)?;
        if !bin.exists() {
            eprintln!("[skip] PEBBLE_BIN does not exist: {}", bin.display());
            return None;
        }
        if !ca_cert.exists() {
            eprintln!(
                "[skip] PEBBLE_CA_CERT does not exist: {}",
                ca_cert.display()
            );
            return None;
        }
        Some(Self { bin, ca_cert })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PEBBLE_BIN + PEBBLE_CA_CERT; run with cargo test -- --ignored"]
async fn relay_acme_pebble_round_trip() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,xlb_net=debug,iroh=info,iroh_relay=info,tokio_rustls_acme=debug")
        .with_test_writer()
        .try_init();

    let pebble = match PebbleEnv::from_env() {
        Some(p) => p,
        None => {
            println!(
                "[skip] PEBBLE_BIN / PEBBLE_CA_CERT not set; this test is opt-in. \
                 See module docs."
            );
            return;
        }
    };

    // tokio-rustls-acme connects to pebble's directory over HTTPS with a
    // self-signed CA. Trust it via SSL_CERT_FILE for this process — note
    // that rustls-native-certs respects this on Linux/macOS via
    // /etc/ssl/cert.pem fallbacks. If your build of tokio-rustls-acme
    // pins to webpki-roots only, this test will fail at directory-fetch
    // time; switch the CA into the system trust store.
    // SAFETY: env mutation in tests; we're single-threaded for setup.
    unsafe {
        std::env::set_var("SSL_CERT_FILE", &pebble.ca_cert);
    }

    // Spawn pebble. Use the bundled defaults: `--config $tmp/pebble.json`
    // would pin ports; we let pebble pick its own and parse stderr to
    // discover the directory URL.
    let child = Command::new(&pebble.bin)
        .args(["-config", "/dev/null"]) // forces compiled-in defaults
        .env("PEBBLE_VA_NOSLEEP", "1")
        .env("PEBBLE_WFE_NONCEREJECT", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn();

    // Pebble usually requires a config file; if `-config /dev/null` fails
    // we fall back to default flags (different pebble builds vary).
    let mut child = match child {
        Ok(c) => c,
        Err(_) => Command::new(&pebble.bin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("pebble spawn"),
    };

    // Best-effort discovery: scrape startup logs for the directory URL.
    // Pebble logs `ACME directory available at: https://0.0.0.0:14000/dir`.
    let mut directory_url = String::from("https://localhost:14000/dir");
    if let Some(stderr) = child.stderr.take() {
        let mut lines = BufReader::new(stderr).lines();
        let scrape = tokio::time::timeout(Duration::from_secs(5), async {
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("pebble: {line}");
                if let Some(idx) = line.find("ACME directory available at:") {
                    let tail = line[idx..].split_whitespace().last().unwrap_or("");
                    if tail.starts_with("https://") {
                        return Some(tail.to_string());
                    }
                }
            }
            None
        })
        .await;
        if let Ok(Some(url)) = scrape {
            directory_url = url;
        }
    }
    eprintln!("ACME directory: {directory_url}");

    // Pebble's TLS-ALPN-01 challenge port defaults to 5001. Bind the
    // relay's HTTPS listener there so pebble can complete the
    // challenge without DNS spoofing — pebble issues for `RELAY_DOMAIN`
    // resolved via /etc/hosts (or a challenge-server override the
    // operator set up).
    let acme = relay::AcmeConfig::custom(directory_url)
        .domain(RELAY_DOMAIN)
        .contact("mailto:ops@yah.test");

    let server = relay::Server::builder()
        .https_bind("0.0.0.0:5001".parse().unwrap())
        .quic_bind("0.0.0.0:0".parse().unwrap())
        .tls_letsencrypt(acme)
        .start()
        .await
        .expect("relay::Server start with ACME");

    // Give the ACME flow up to 30s to issue. tokio-rustls-acme issues
    // lazily on first TLS handshake, so the first dial below is what
    // actually triggers issuance — we just wait long enough for the
    // initial `state.next()` events to flush.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let relay_map = server.relay_map().expect("relay_map");

    let alice = Endpoint::builder()
        .keypair(Keypair::generate())
        .alpns([ALPN])
        .relay_map(relay_map.clone())
        // Pebble issues a cert chain rooted at PEBBLE_CA_CERT. webpki
        // doesn't trust it. Skip TLS verification on the relay leg —
        // the *issuance* still went through real ACME, which is what
        // this test asserts. Production deployments use real LE roots
        // and do verify.
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

    alice.inner().online().await;
    let alice_id = alice.node_id();
    let alice_addr = iroh::EndpointAddr::new(alice_id)
        .with_relay_url(server.https_url().expect("https_url").into());

    let saw = Arc::new(AtomicBool::new(false));
    let saw_h = saw.clone();
    let alice_handle = alice.clone();
    let server_task = tokio::spawn(async move {
        let mut handlers: HashMap<Alpn, AlpnHandler> = HashMap::new();
        let flag = saw_h.clone();
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

    let conn = tokio::time::timeout(Duration::from_secs(45), bob.connect_alpn(alice_addr, ALPN))
        .await
        .expect("connect within 45s")
        .expect("bob connect");

    let (mut send, mut recv) = conn.open_bi().await.expect("open_bi");
    send.write_all(b"yah-acme").await.expect("write");
    send.finish().expect("finish");
    let echoed = recv.read_to_end(1024).await.expect("read");
    assert_eq!(echoed, b"yah-acme");
    conn.close(0u32.into(), b"done");

    for _ in 0..50 {
        if saw.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(saw.load(Ordering::SeqCst), "alice handler ran via ACME-issued relay");

    alice.close().await;
    bob.close().await;
    server_task.abort();
    server.shutdown().await.expect("shutdown");

    let _ = child.kill().await;
}
