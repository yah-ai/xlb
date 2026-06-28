//! `xlb seed` — publish content-addressed blobs to an S3-compatible origin.
//!
//! The serving side of xlb (cache → LAN → swarm → seed → CDN) is read-only:
//! [`crate::AssetClass`] *fetches* blobs but never writes the origin. The seed
//! capability closes that gap. Any xlb node can run `xlb seed --class <name>
//! <file>` to push bytes into the private CDN bucket (Cloudflare R2 / any
//! S3-compatible store) at the exact content-addressed key the class's
//! `cdn_fallback` template reads back. With it, R2 becomes the self-serving
//! origin (`FetchTier::Cdn`) and no standing seed daemon is required for
//! correctness — see W171.
//!
//! ## Why not `aws-sdk-s3`
//!
//! xlb already links `reqwest` with `rustls-tls` and is built for musl
//! serverless targets. Pulling the full AWS SDK drags in a large dependency
//! tree and its own TLS/transport story. A single-object PUT/HEAD needs only
//! AWS Signature V4, which is ~150 lines of SHA-256 + HMAC over the existing
//! `reqwest` client — so that is what this module implements.
//!
//! ## Idempotency
//!
//! Keys are content-addressed (the BLAKE3 hash *is* the key suffix), so
//! seeding is naturally idempotent: [`seed_blob`] issues a HEAD first and
//! skips the PUT when the object already exists. This works across machines
//! (CI runners are ephemeral) without any shared local manifest.

use bytes::Bytes;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::BlakeHash;

type HmacSha256 = Hmac<Sha256>;

/// An S3-compatible publish target (Cloudflare R2 by default).
///
/// Path-style addressing is used (`{endpoint}/{bucket}/{key}`) because R2's
/// account endpoint serves buckets path-style and it avoids DNS-per-bucket.
#[derive(Clone)]
pub struct R2Target {
    /// e.g. `https://<account>.r2.cloudflarestorage.com` (no trailing slash).
    pub endpoint: String,
    /// Bucket name, e.g. `yah-dev`.
    pub bucket: String,
    /// SigV4 region. R2 ignores it but requires a value; `auto` by convention.
    pub region: String,
    access_key: String,
    secret_key: String,
    client: reqwest::Client,
}

/// The result of a seed operation.
#[derive(Debug, Clone)]
pub struct SeedOutcome {
    /// The S3 key the bytes live at (e.g. `yah-cli/<blake3-hex>`).
    pub key: String,
    /// Size of the seeded object in bytes.
    pub size: u64,
    /// `true` when the object already existed and the PUT was skipped.
    pub already_present: bool,
}

impl R2Target {
    /// Build a target from the environment, reusing the `CF_R2_*` secret shape
    /// the release pipeline already uses for `publish-yubaba`.
    ///
    /// Resolution order (first set wins) per field:
    /// - access key:  `XLB_R2_ACCESS_KEY_ID` → `AWS_ACCESS_KEY_ID` → `CF_R2_ACCESS_KEY_ID`
    /// - secret key:  `XLB_R2_SECRET_ACCESS_KEY` → `AWS_SECRET_ACCESS_KEY` → `CF_R2_SECRET_KEY`
    /// - endpoint:    `XLB_R2_ENDPOINT`, else derived from `CF_R2_ACCOUNT_ID`
    /// - bucket:      `XLB_R2_BUCKET`
    /// - region:      `AWS_DEFAULT_REGION` → `auto`
    pub fn from_env() -> anyhow::Result<Self> {
        let access_key = first_env(&[
            "XLB_R2_ACCESS_KEY_ID",
            "AWS_ACCESS_KEY_ID",
            "CF_R2_ACCESS_KEY_ID",
        ])
        .ok_or_else(|| anyhow::anyhow!("no R2 access key (set XLB_R2_ACCESS_KEY_ID / AWS_ACCESS_KEY_ID / CF_R2_ACCESS_KEY_ID)"))?;
        let secret_key = first_env(&[
            "XLB_R2_SECRET_ACCESS_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "CF_R2_SECRET_KEY",
        ])
        .ok_or_else(|| anyhow::anyhow!("no R2 secret key (set XLB_R2_SECRET_ACCESS_KEY / AWS_SECRET_ACCESS_KEY / CF_R2_SECRET_KEY)"))?;

        let endpoint = first_env(&["XLB_R2_ENDPOINT"])
            .or_else(|| {
                first_env(&["CF_R2_ACCOUNT_ID"])
                    .map(|id| format!("https://{id}.r2.cloudflarestorage.com"))
            })
            .ok_or_else(|| anyhow::anyhow!("no R2 endpoint (set XLB_R2_ENDPOINT or CF_R2_ACCOUNT_ID)"))?;

        let bucket = first_env(&["XLB_R2_BUCKET"])
            .ok_or_else(|| anyhow::anyhow!("no R2 bucket (set XLB_R2_BUCKET)"))?;

        let region = first_env(&["AWS_DEFAULT_REGION"]).unwrap_or_else(|| "auto".into());

        Self::new(endpoint, bucket, region, access_key, secret_key)
    }

    /// Build a target explicitly (used by tests and non-env callers).
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder().use_rustls_tls().build()?;
        Ok(Self {
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            bucket: bucket.into(),
            region: region.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            client,
        })
    }

    /// `{endpoint}/{bucket}/{key}` — the full request URL.
    fn url_for(&self, key: &str) -> String {
        format!("{}/{}/{}", self.endpoint, self.bucket, key)
    }

    /// Canonical URI for SigV4: `/{bucket}/{key}`, per-segment URI-encoded.
    fn canonical_uri(&self, key: &str) -> String {
        let path = format!("{}/{}", self.bucket, key);
        let encoded = path
            .split('/')
            .map(uri_encode_segment)
            .collect::<Vec<_>>()
            .join("/");
        format!("/{encoded}")
    }

    fn host(&self) -> &str {
        self.endpoint
            .strip_prefix("https://")
            .or_else(|| self.endpoint.strip_prefix("http://"))
            .unwrap_or(&self.endpoint)
    }

    /// HEAD the object. `Ok(Some(size))` if it exists, `Ok(None)` on 404.
    pub async fn head_object(&self, key: &str) -> anyhow::Result<Option<u64>> {
        let payload_hash = sha256_hex(b"");
        let (auth, amzdate) = self.sign("HEAD", key, &payload_hash, now_unix());
        let resp = self
            .client
            .head(self.url_for(key))
            .header("host", self.host())
            .header("x-amz-content-sha256", &payload_hash)
            .header("x-amz-date", &amzdate)
            .header("authorization", auth)
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            anyhow::bail!("HEAD {key} → {}", resp.status());
        }
        let size = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(Some(size))
    }

    /// PUT `bytes` at `key`.
    pub async fn put_object(&self, key: &str, bytes: &Bytes) -> anyhow::Result<()> {
        let payload_hash = sha256_hex(bytes);
        let (auth, amzdate) = self.sign("PUT", key, &payload_hash, now_unix());
        let resp = self
            .client
            .put(self.url_for(key))
            .header("host", self.host())
            .header("x-amz-content-sha256", &payload_hash)
            .header("x-amz-date", &amzdate)
            .header("authorization", auth)
            .header("content-type", "application/octet-stream")
            .body(bytes.clone())
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("PUT {key} → {status}: {body}");
        }
        Ok(())
    }

    /// AWS Signature V4 for a single-object request. Returns the
    /// `Authorization` header value and the `x-amz-date` value (the caller
    /// must send the same `x-amz-date` it was signed with).
    ///
    /// Signed headers are fixed at `host;x-amz-content-sha256;x-amz-date`,
    /// which is sufficient for object PUT/HEAD with an in-memory payload hash.
    fn sign(
        &self,
        method: &str,
        key: &str,
        payload_hash: &str,
        unix_secs: u64,
    ) -> (String, String) {
        let (amzdate, datestamp) = format_amz_time(unix_secs);
        let service = "s3";
        let scope = format!("{datestamp}/{}/{service}/aws4_request", self.region);

        let canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
            self.host(),
            payload_hash,
            amzdate,
        );
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        let canonical_request = format!(
            "{method}\n{}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
            self.canonical_uri(key),
        );

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes()),
        );

        let signing_key = self.signing_key(&datestamp, service);
        let signature = hex::encode(hmac(&signing_key, string_to_sign.as_bytes()));

        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key,
        );
        (auth, amzdate)
    }

    fn signing_key(&self, datestamp: &str, service: &str) -> Vec<u8> {
        let k_date = hmac(format!("AWS4{}", self.secret_key).as_bytes(), datestamp.as_bytes());
        let k_region = hmac(&k_date, self.region.as_bytes());
        let k_service = hmac(&k_region, service.as_bytes());
        hmac(&k_service, b"aws4_request")
    }
}

/// Seed `bytes` to `target` at `key`, skipping the PUT when the
/// content-addressed object is already present (HEAD-then-PUT idempotency).
pub async fn seed_blob(
    target: &R2Target,
    key: &str,
    bytes: impl Into<Bytes>,
) -> anyhow::Result<SeedOutcome> {
    let bytes = bytes.into();
    let size = bytes.len() as u64;
    if let Some(existing) = target.head_object(key).await? {
        tracing::debug!(%key, existing, "blob already seeded — skipping PUT");
        return Ok(SeedOutcome { key: key.to_string(), size: existing, already_present: true });
    }
    target.put_object(key, &bytes).await?;
    Ok(SeedOutcome { key: key.to_string(), size, already_present: false })
}

/// Derive the S3 key from a class's `cdn_fallback` URL template by taking its
/// path component and substituting `{blake3}` with the hash.
///
/// `https://r2.yah.run/yah-cli/{blake3}` + `<hash>` → `yah-cli/<hash-hex>`.
/// The scheme/host are irrelevant to the key (the PUT goes to the S3
/// endpoint+bucket); only the path identifies the object.
pub fn derive_key(cdn_fallback: &str, hash: &BlakeHash) -> anyhow::Result<String> {
    // Strip scheme://host, keep the path.
    let after_scheme = cdn_fallback
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(cdn_fallback);
    let path = match after_scheme.split_once('/') {
        Some((_host, path)) => path,
        None => anyhow::bail!("cdn_fallback has no path component: {cdn_fallback}"),
    };
    if !path.contains("{blake3}") {
        anyhow::bail!("cdn_fallback template missing {{blake3}} token: {cdn_fallback}");
    }
    Ok(path.trim_start_matches('/').replace("{blake3}", &hash.to_hex()))
}

// ─── helpers ────────────────────────────────────────────────────────────────

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// URI-encode one path segment per AWS rules: unreserved chars
/// (`A-Za-z0-9-_.~`) pass through; everything else becomes `%XX` (uppercase).
fn uri_encode_segment(seg: &str) -> String {
    let mut out = String::with_capacity(seg.len());
    for b in seg.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Format a Unix timestamp (UTC) as the SigV4 `(x-amz-date, datestamp)` pair:
/// `("YYYYMMDDTHHMMSSZ", "YYYYMMDD")`. Uses Howard Hinnant's
/// `civil_from_days` so no date dependency is needed.
fn format_amz_time(unix_secs: u64) -> (String, String) {
    let days = (unix_secs / 86_400) as i64;
    let sod = unix_secs % 86_400;
    let (hour, minute, second) = (sod / 3600, (sod % 3600) / 60, sod % 60);

    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };

    let datestamp = format!("{year:04}{month:02}{day:02}");
    let amzdate = format!("{datestamp}T{hour:02}{minute:02}{second:02}Z");
    (amzdate, datestamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_formatter_known_timestamps() {
        // 2013-05-24T00:00:00Z = 1369353600 (AWS SigV4 doc example date)
        assert_eq!(format_amz_time(1_369_353_600), ("20130524T000000Z".into(), "20130524".into()));
        // 1970-01-01T00:00:00Z
        assert_eq!(format_amz_time(0), ("19700101T000000Z".into(), "19700101".into()));
        // 2026-06-21T13:45:07Z = 1782049507
        assert_eq!(format_amz_time(1_782_049_507), ("20260621T134507Z".into(), "20260621".into()));
    }

    #[test]
    fn derive_key_extracts_path_and_substitutes() {
        let hash = BlakeHash::hash(b"hello");
        let key = derive_key("https://r2.yah.run/yah-cli/{blake3}", &hash).unwrap();
        assert_eq!(key, format!("yah-cli/{}", hash.to_hex()));
    }

    #[test]
    fn derive_key_rejects_missing_token() {
        let hash = BlakeHash::hash(b"x");
        assert!(derive_key("https://r2.yah.run/yah-cli/static.bin", &hash).is_err());
    }

    #[test]
    fn derive_key_handles_nested_path() {
        let hash = BlakeHash::hash(b"x");
        let key = derive_key("https://cdn.example.com/assets/v2/{blake3}", &hash).unwrap();
        assert_eq!(key, format!("assets/v2/{}", hash.to_hex()));
    }

    #[test]
    fn signature_is_deterministic_for_fixed_inputs() {
        // Fixed key/time → stable signature. This is a regression anchor, not
        // an AWS-verified vector: it pins the canonicalization so an accidental
        // change to header ordering / encoding is caught.
        let t = R2Target::new(
            "https://acct.r2.cloudflarestorage.com",
            "yah-dev",
            "auto",
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        )
        .unwrap();
        let payload = sha256_hex(b"");
        let (auth1, date1) = t.sign("HEAD", "yah-cli/abc", &payload, 1_782_049_507);
        let (auth2, date2) = t.sign("HEAD", "yah-cli/abc", &payload, 1_782_049_507);
        assert_eq!(auth1, auth2);
        assert_eq!(date1, "20260621T134507Z");
        assert!(auth1.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260621/auto/s3/aws4_request"));
        assert!(auth1.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        // Different key → different signature.
        let (auth3, _) = t.sign("HEAD", "yah-cli/xyz", &payload, 1_782_049_507);
        assert_ne!(auth1, auth3);
    }

    #[test]
    fn canonical_uri_is_path_style_and_encoded() {
        let t = R2Target::new("https://h", "yah-dev", "auto", "a", "b").unwrap();
        assert_eq!(t.canonical_uri("yah-cli/deadbeef"), "/yah-dev/yah-cli/deadbeef");
    }

    /// Spin a one-request HTTP/1.1 mock that captures the request line +
    /// headers + body and replies with `status`. Returns the captured request
    /// (via the channel) and the `http://127.0.0.1:{port}` base URL.
    fn mock_server(
        status: u16,
        extra_headers: &'static str,
    ) -> (String, std::sync::mpsc::Receiver<(String, Vec<u8>)>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Read until we have headers; then read Content-Length bytes of body.
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            loop {
                let n = stream.read(&mut tmp).unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = find_headers_end(&buf) {
                    let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                    let clen = content_length(&head);
                    let body_start = pos + 4;
                    while buf.len() < body_start + clen {
                        let n = stream.read(&mut tmp).unwrap();
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    let body = buf[body_start..(body_start + clen).min(buf.len())].to_vec();
                    let line = match status {
                        200 => "200 OK",
                        404 => "404 Not Found",
                        _ => "500 Internal Server Error",
                    };
                    let resp = format!("HTTP/1.1 {line}\r\n{extra_headers}Content-Length: 0\r\nConnection: close\r\n\r\n");
                    let _ = stream.write_all(resp.as_bytes());
                    let _ = tx.send((head, body));
                    break;
                }
            }
        });

        (format!("http://127.0.0.1:{port}"), rx)
    }

    fn find_headers_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    fn content_length(head: &str) -> usize {
        head.lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                (k.trim().eq_ignore_ascii_case("content-length"))
                    .then(|| v.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn put_object_sends_signed_request_with_body() {
        let (base, rx) = mock_server(200, "");
        let t = R2Target::new(base, "yah-dev", "auto", "AKID", "SECRET").unwrap();
        let body = Bytes::from_static(b"the seeded bytes");
        t.put_object("yah-cli/deadbeef", &body).await.unwrap();

        let (head, captured_body) = rx.recv().unwrap();
        let request_line = head.lines().next().unwrap();
        assert_eq!(request_line, "PUT /yah-dev/yah-cli/deadbeef HTTP/1.1");
        assert_eq!(captured_body, b"the seeded bytes");

        let lower = head.to_lowercase();
        assert!(lower.contains("authorization: aws4-hmac-sha256 credential=akid/"));
        assert!(lower.contains("x-amz-date:"));
        // x-amz-content-sha256 must equal the SHA-256 of the actual body.
        assert!(head.contains(&sha256_hex(&body)));
    }

    #[tokio::test]
    async fn head_object_maps_status_to_presence() {
        // 404 → not present.
        let (base, _rx) = mock_server(404, "");
        let t = R2Target::new(base, "yah-dev", "auto", "AKID", "SECRET").unwrap();
        assert_eq!(t.head_object("yah-cli/missing").await.unwrap(), None);

        // 200 + Content-Length → present with size.
        let (base, _rx) = mock_server(200, "Content-Length-Echo: 0\r\n");
        let t = R2Target::new(base, "yah-dev", "auto", "AKID", "SECRET").unwrap();
        // Our mock replies Content-Length: 0, so size resolves to 0; presence is
        // what matters here (Some vs None).
        assert_eq!(t.head_object("yah-cli/present").await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn seed_blob_skips_put_when_already_present() {
        // HEAD returns 200 → seed_blob must report already_present and NOT PUT.
        let (base, rx) = mock_server(200, "");
        let t = R2Target::new(base, "yah-dev", "auto", "AKID", "SECRET").unwrap();
        let outcome = seed_blob(&t, "yah-cli/abc", Bytes::from_static(b"xx")).await.unwrap();
        assert!(outcome.already_present);

        // The single captured request must be the HEAD, never a PUT.
        let (head, _body) = rx.recv().unwrap();
        assert!(head.lines().next().unwrap().starts_with("HEAD "));
    }
}
