//! `xlb seed` — publish a local file to the class's CDN origin (R2).
//!
//! Standalone one-shot: unlike `fetch`/`inspect`/`gui` it does NOT talk to a
//! running node socket. It hashes the file, looks up the class's
//! `cdn_fallback` template in the node config to derive the content-addressed
//! key, and PUTs the bytes to the S3-compatible origin (credentials from the
//! environment — see [`xlb::R2Target::from_env`]). Seeding is idempotent: an
//! already-present object is reported, not re-uploaded.

use std::path::Path;

use anyhow::{Context, Result};
use xlb::{derive_key, seed_blob, BlakeHash, R2Target};

use crate::config::NodeConfig;

pub async fn run(
    class: &str,
    file: &str,
    config_path: &str,
    manifest: Option<&str>,
    json: bool,
) -> Result<()> {
    let bytes = std::fs::read(file).with_context(|| format!("reading {file}"))?;
    let hash = BlakeHash::hash(&bytes);

    let cfg = NodeConfig::load(config_path)?;
    let cc = cfg
        .classes
        .iter()
        .find(|c| c.name == class)
        .with_context(|| format!("class '{class}' not found in {config_path}"))?;
    let template = cc
        .cdn_fallback
        .as_deref()
        .with_context(|| format!("class '{class}' has no cdn_fallback; nowhere to seed to"))?;

    let key = derive_key(template, &hash)?;
    let target = R2Target::from_env()?;
    let public_url = template.replace("{blake3}", &hash.to_hex());

    let outcome = seed_blob(&target, &key, bytes)
        .await
        .with_context(|| format!("seeding {file} to {key}"))?;

    if let Some(path) = manifest {
        append_manifest(path, class, &hash, &outcome, &public_url)?;
    }

    if json {
        // Machine-readable line for the release pipeline to capture as a job
        // output (R423-T5 consumes the blake3 list from this).
        println!(
            r#"{{"class":"{class}","blake3":"{}","key":"{}","size":{},"url":"{public_url}","already_present":{}}}"#,
            hash.to_hex(),
            outcome.key,
            outcome.size,
            outcome.already_present,
        );
    } else {
        let verb = if outcome.already_present { "already seeded" } else { "seeded" };
        println!(
            "{verb}: {class} {} ({} bytes)\n  key: {}\n  url: {public_url}",
            hash.to_hex(),
            outcome.size,
            outcome.key,
        );
    }
    Ok(())
}

/// Append one JSONL record to the seeded-manifest log (the persistent
/// "known-seeded from then on" record; idempotency itself is HEAD-based).
fn append_manifest(
    path: &str,
    class: &str,
    hash: &BlakeHash,
    outcome: &xlb::SeedOutcome,
    public_url: &str,
) -> Result<()> {
    use std::io::Write;
    let p = Path::new(path);
    if let Some(dir) = p.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(p)
        .with_context(|| format!("opening manifest {path}"))?;
    writeln!(
        f,
        r#"{{"class":"{class}","blake3":"{}","key":"{}","size":{},"url":"{public_url}","already_present":{}}}"#,
        hash.to_hex(),
        outcome.key,
        outcome.size,
        outcome.already_present,
    )
    .with_context(|| format!("writing manifest {path}"))?;
    Ok(())
}
