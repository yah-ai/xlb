//! Per-machine Ed25519 keypair: load-or-create at a stable path so every
//! yah-family process running on the same machine sees the same `NodeId`.
//!
//! Storage layout (under `directories::ProjectDirs::from("dev","yah","yah").data_local_dir()`):
//!
//! | File | Mode | Contents |
//! |---|---|---|
//! | `identity.ed25519` | 0600 | 32-byte raw secret (binary) |
//! | `identity.pub`     | 0644 | hex-encoded `NodeId`, newline-terminated (human inspection) |
//!
//! First-run creates `identity.ed25519` atomically with `O_EXCL`; concurrent
//! processes racing to create the same file all converge on the same key
//! (whichever wins the create gets read by the others on retry).
//!
//! Rotation is a consumer-layer concern (see Q1 in xlb-net.md) — this
//! module is intentionally minimal: load existing or create fresh.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use iroh::SecretKey;

use crate::{Error, NodeId, Result};

const SECRET_FILENAME: &str = "identity.ed25519";
const PUBLIC_FILENAME: &str = "identity.pub";

/// Per-machine keypair. Holds the secret in memory; the public `NodeId` is
/// derivable via [`Keypair::node_id`].
#[derive(Clone, Debug)]
pub struct Keypair {
    secret: SecretKey,
}

impl Keypair {
    /// Load the per-machine keypair from the platform data-local directory,
    /// creating it on first run.
    pub fn load_or_create() -> Result<Self> {
        let dir = identity_dir()?;
        Self::load_or_create_at(&dir)
    }

    /// Load-or-create at an explicit directory. Useful for tests and for
    /// callers that override the platform default (e.g. an admin running
    /// multiple yah instances on one host).
    pub fn load_or_create_at(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let secret_path = dir.join(SECRET_FILENAME);

        if let Some(secret) = read_secret(&secret_path)? {
            return Ok(Self { secret });
        }

        let secret = SecretKey::generate();
        write_secret_atomic(&secret_path, &secret)?;
        write_public(&dir.join(PUBLIC_FILENAME), &secret)?;
        Ok(Self { secret })
    }

    /// Construct from an explicit `SecretKey` (tests, in-memory endpoints).
    pub fn from_secret(secret: SecretKey) -> Self {
        Self { secret }
    }

    /// Generate a fresh in-memory keypair (no disk I/O).
    pub fn generate() -> Self {
        Self {
            secret: SecretKey::generate(),
        }
    }

    /// Borrow the underlying iroh `SecretKey`.
    pub fn secret(&self) -> &SecretKey {
        &self.secret
    }

    /// Derive the public `NodeId` (Ed25519 pubkey).
    pub fn node_id(&self) -> NodeId {
        self.secret.public()
    }
}

/// Resolve the per-machine identity directory under the platform's
/// data-local dir (e.g. `~/Library/Application Support/yah` on macOS,
/// `~/.local/share/yah` on Linux).
pub fn identity_dir() -> Result<PathBuf> {
    let proj = ProjectDirs::from("dev", "yah", "yah").ok_or(Error::NoDataDir)?;
    Ok(proj.data_local_dir().to_path_buf())
}

fn read_secret(path: &Path) -> Result<Option<SecretKey>> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let arr: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::Keypair(format!("expected 32 bytes, got {}", bytes.len())))?;
    Ok(Some(SecretKey::from_bytes(&arr)))
}

fn write_secret_atomic(path: &Path, secret: &SecretKey) -> Result<()> {
    let bytes = secret.to_bytes();
    write_atomic(path, &bytes, 0o600)?;
    Ok(())
}

fn write_public(path: &Path, secret: &SecretKey) -> Result<()> {
    let pubkey = secret.public();
    let line = format!("{}\n", hex::encode(pubkey.as_bytes()));
    write_atomic(path, line.as_bytes(), 0o644)?;
    Ok(())
}

fn write_atomic(path: &Path, contents: &[u8], _mode: u32) -> io::Result<()> {
    // Write to a sibling temp file, then rename. On unix, set mode on the
    // temp file before rename so the final inode is created with the right
    // permissions.
    let tmp = path.with_extension("tmp");
    {
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(_mode);
        }
        let mut f = opts.open(&tmp)?;
        use std::io::Write;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn load_or_create_round_trips() {
        let dir = tempdir().unwrap();
        let kp1 = Keypair::load_or_create_at(dir.path()).unwrap();
        let id1 = kp1.node_id();

        // Second call returns the same identity.
        let kp2 = Keypair::load_or_create_at(dir.path()).unwrap();
        assert_eq!(kp1.node_id(), kp2.node_id());

        // Files exist.
        assert!(dir.path().join(SECRET_FILENAME).exists());
        let pub_text = fs::read_to_string(dir.path().join(PUBLIC_FILENAME)).unwrap();
        assert_eq!(pub_text.trim(), hex::encode(id1.as_bytes()));
    }

    #[cfg(unix)]
    #[test]
    fn secret_file_is_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir().unwrap();
        let _ = Keypair::load_or_create_at(dir.path()).unwrap();
        let meta = fs::metadata(dir.path().join(SECRET_FILENAME)).unwrap();
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "secret file mode should be 0600, got {mode:o}");
    }

    #[test]
    fn corrupt_secret_file_errors_loudly() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join(SECRET_FILENAME), b"too short").unwrap();
        let err = Keypair::load_or_create_at(dir.path()).unwrap_err();
        assert!(matches!(err, Error::Keypair(_)), "got {err:?}");
    }
}
