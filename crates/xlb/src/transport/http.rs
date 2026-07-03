//! HTTP CDN-fallback adapter — tier 5 (`FetchTier::Cdn`).
//!
//! Fetches blobs from an HTTPS CDN using a URL template where `{blake3}` is
//! substituted with the hex-encoded hash per request. The response body is
//! streamed through a [`Verifier`] so a tampered CDN response is rejected
//! before bytes reach the caller.
//!
//! HTTP Range requests (required for future multi-source chunk dispatch) are
//! structurally supported: the URL maps to a single object on any CDN that
//! does object-level access (R2, S3, Cloudflare). xlb-3 fetches whole blobs;
//! range-based chunk dispatch arrives in the concurrent-fetch phase.

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    source::{BlobSource, FetchProgress, FetchTier, ProgressSink},
    verify::Verifier,
    BlakeHash,
};

// ─── HttpFetcher ─────────────────────────────────────────────────────────────

/// CDN HTTP source. Auto-wired by [`AssetClass::register`] when
/// `AssetClassConfig::cdn_fallback` is `Some`.
///
/// A single `reqwest::Client` is reused across fetches to benefit from
/// connection pooling on repeated requests to the same CDN origin.
pub(crate) struct HttpFetcher {
    /// URL template. `{blake3}` is replaced with the hex-encoded hash on
    /// every request.
    url_template: String,
    client: reqwest::Client,
}

impl HttpFetcher {
    pub fn new(url_template: impl Into<String>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            // Content is BLAKE3-verified, so redirects are never needed; refusing
            // to follow them closes a CDN-driven redirect-SSRF vector.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { url_template: url_template.into(), client })
    }

    fn url_for(&self, hash: &BlakeHash) -> String {
        self.url_template.replace("{blake3}", &hash.to_hex())
    }
}

#[async_trait]
impl BlobSource for HttpFetcher {
    fn tier(&self) -> FetchTier {
        FetchTier::Cdn
    }

    async fn fetch_raw(&self, hash: &BlakeHash) -> Option<Bytes> {
        self.fetch_raw_with_progress(hash, None).await
    }

    async fn fetch_raw_with_progress(
        &self,
        hash: &BlakeHash,
        sink: Option<&ProgressSink>,
    ) -> Option<Bytes> {
        let url = self.url_for(hash);
        tracing::debug!(%hash, %url, "CDN HTTP fetch");

        let mut resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| tracing::warn!(%hash, %url, "CDN connect error: {e}"))
            .ok()?;

        if !resp.status().is_success() {
            tracing::warn!(%hash, status = %resp.status(), "CDN non-2xx response");
            return None;
        }

        // Stream the body chunk-by-chunk rather than buffering the whole
        // (~558MB) object: each chunk is fed through the incremental Verifier
        // and counted against Content-Length for progress. `Response::chunk`
        // reads the body stream without needing reqwest's `stream` feature.
        let total = resp.content_length();
        let mut verifier = Verifier::new(*hash);
        let mut received: u64 = 0;

        loop {
            match resp.chunk().await {
                Ok(Some(chunk)) => {
                    verifier.update(&chunk);
                    received = received.saturating_add(chunk.len() as u64);
                    if let Some(sink) = sink {
                        sink(FetchProgress {
                            bytes_so_far: received,
                            total,
                            tier: FetchTier::Cdn,
                        });
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(%hash, "CDN body read error: {e}");
                    return None;
                }
            }
        }

        verifier
            .finish()
            .map_err(|e| tracing::warn!(%hash, "CDN BLAKE3 mismatch: {e}"))
            .ok()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlakeHash;

    /// Spin a bare TCP server that responds to one HTTP/1.1 GET with `status`
    /// and `body`. Returns the base URL template `http://127.0.0.1:{port}/{blake3}`.
    fn start_test_server(status: u16, body: Vec<u8>) -> String {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/{{blake3}}");

        std::thread::spawn(move || {
            // Serve requests until the listener is dropped.
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let body = body.clone();
                let status = status;
                std::thread::spawn(move || {
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf);
                    let status_line = match status {
                        200 => "200 OK",
                        404 => "404 Not Found",
                        _ => "500 Internal Server Error",
                    };
                    let header = format!(
                        "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(header.as_bytes());
                    let _ = stream.write_all(&body);
                });
            }
        });

        url
    }

    #[tokio::test]
    async fn fetches_blob_from_http_server() {
        let data = b"hello from cdn edge";
        let hash = BlakeHash::hash(data);
        let url_template = start_test_server(200, data.to_vec());

        let fetcher = HttpFetcher::new(&url_template).unwrap();
        let result = fetcher.fetch_raw(&hash).await;

        assert!(result.is_some(), "expected bytes, got None");
        assert_eq!(&result.unwrap()[..], data);
    }

    #[tokio::test]
    async fn returns_none_on_404() {
        let hash = BlakeHash::hash(b"missing blob");
        let url_template = start_test_server(404, vec![]);

        let fetcher = HttpFetcher::new(&url_template).unwrap();
        let result = fetcher.fetch_raw(&hash).await;

        assert!(result.is_none(), "expected None for 404, got Some");
    }

    #[tokio::test]
    async fn reports_progress_against_content_length() {
        use std::sync::{Arc, Mutex};

        // A multi-KB body so the streamed read carries a real Content-Length.
        let data = vec![7u8; 5000];
        let hash = BlakeHash::hash(&data);
        let url_template = start_test_server(200, data.clone());

        let fetcher = HttpFetcher::new(&url_template).unwrap();

        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = events.clone();
        let sink: ProgressSink = Arc::new(move |p| captured.lock().unwrap().push(p));

        let result = fetcher.fetch_raw_with_progress(&hash, Some(&sink)).await;
        assert_eq!(result.as_deref(), Some(&data[..]), "streamed bytes must verify");

        let events = events.lock().unwrap();
        assert!(!events.is_empty(), "expected at least one progress callback");
        let last = events.last().unwrap();
        assert_eq!(last.bytes_so_far, data.len() as u64, "final progress = full body");
        assert_eq!(last.total, Some(data.len() as u64), "Content-Length surfaced");
        assert_eq!(last.tier, FetchTier::Cdn);
        // Cumulative byte count is monotonically non-decreasing.
        assert!(events.windows(2).all(|w| w[0].bytes_so_far <= w[1].bytes_so_far));
    }

    #[tokio::test]
    async fn returns_none_on_hash_mismatch() {
        // Server returns wrong bytes — verifier must reject them.
        let real_data = b"authentic content";
        let hash = BlakeHash::hash(real_data);
        let wrong_data = b"tampered cdn response";

        let url_template = start_test_server(200, wrong_data.to_vec());

        let fetcher = HttpFetcher::new(&url_template).unwrap();
        let result = fetcher.fetch_raw(&hash).await;

        assert!(result.is_none(), "tampered bytes must be rejected");
    }
}
