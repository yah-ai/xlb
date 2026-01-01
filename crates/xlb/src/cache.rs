//! Per-class blob cache — `FetchTier::Cache` (tier 0).
//!
//! Two flavors selected at [`AssetClass::register`] time by
//! `AssetClassConfig::cache_dir`:
//!
//! - **In-memory** (`cache_dir` = `None`): a `HashMap<BlakeHash, Bytes>` in a
//!   single `RwLock`. Lossy on process restart. Suitable for short-lived
//!   clients (CLI one-shots, tests) and for callers that intentionally opt
//!   out of disk persistence.
//! - **Disk-backed with LRU eviction** (`cache_dir` = `Some(path)`): each blob
//!   is one file at `<cache_dir>/<blake3-hex>`, written atomically via a
//!   `<hash>.tmp` + rename so a torn write can't be served as truth. An
//!   in-memory LRU index tracks recency and total bytes; eviction kicks in
//!   when an insert would push the on-disk footprint past
//!   `cache_budget_bytes`. The index is **rebuilt by scanning the directory
//!   on construction** so the cache survives restart — this is the whole
//!   point of the disk-backed variant (without it the seed economics are
//!   strictly worse than R2-direct, cf. W160 F3).
//!
//! ## Verification on read
//!
//! Disk-cache `get` re-hashes every byte before returning, so silent
//! bit-rot or out-of-band tampering surfaces as a miss + an eviction of the
//! corrupted entry rather than a serve. The cost is one BLAKE3 pass per
//! cache hit; cheap relative to a network round-trip and load-bearing for
//! the "cache survives restart" property — without it we'd have to either
//! trust the disk blindly or store a sidecar mac (which buys nothing
//! beyond what blake3-rehash already gives us).
//!
//! ## LRU ordering
//!
//! On a hit, the entry is moved to the back of the `VecDeque` (most-recently
//! used). On insert, entries are popped from the front until the new write
//! fits within budget. Scan-to-touch is `O(n)` but `n` is bounded by the
//! number of blobs in the cache — at xlb's target scales (50–500 blobs
//! per class, multi-MB each) the per-touch cost is negligible.
//!
//! ## Single-flight is *not* provided here
//!
//! Concurrent fetches for the same hash may all run the fetch chain to
//! completion; the cache layer just guarantees that the resulting writes
//! are atomic (no torn files) and idempotent (last-writer-wins with
//! identical bytes). True single-flight would need an inflight-tracking
//! map at the [`AssetClass::fetch_bytes`] level — separate concern, not
//! T3's scope. The tests in `tests/disk_cache.rs` lock in the
//! atomicity-under-contention property.

use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use tokio::sync::{Mutex, RwLock};

use crate::BlakeHash;

/// Monotonic counter for temp-file scratch paths. Process-static so
/// concurrent `put` calls (especially against the same hash) produce
/// distinct temp files — otherwise the rename of one tramples the
/// temp file another task is still writing.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

// ─── Cache (public to the crate, hidden from users) ───────────────────────────

/// Per-class cache — `FetchTier::Cache`. Either in-memory or disk-backed.
pub(crate) enum Cache {
    Mem(MemCache),
    Disk(DiskCache),
}

impl Cache {
    /// Build a cache for one `AssetClass`.
    ///
    /// `cache_dir = None` → in-memory; `Some(path)` → disk-backed at `path`
    /// with `budget_bytes` LRU eviction budget. The directory is created if
    /// it doesn't exist; existing entries on disk are picked up into the
    /// LRU index (cache survives restart).
    pub(crate) fn new(
        cache_dir: Option<&Path>,
        budget_bytes: u64,
    ) -> std::io::Result<Self> {
        match cache_dir {
            None => Ok(Self::Mem(MemCache::default())),
            Some(dir) => Ok(Self::Disk(DiskCache::open(dir, budget_bytes)?)),
        }
    }

    /// Try to read `hash` from the cache. Returns `None` on miss.
    ///
    /// Disk-backed: re-verifies BLAKE3 before returning; a mismatched entry
    /// is removed and counted as a miss.
    pub(crate) async fn get(&self, hash: &BlakeHash) -> Option<Bytes> {
        match self {
            Self::Mem(m) => m.get(hash).await,
            Self::Disk(d) => d.get(hash).await,
        }
    }

    /// Insert `bytes` for `hash`. Disk-backed: writes atomically via
    /// temp+rename and evicts oldest LRU entries until the on-disk footprint
    /// is within `budget_bytes`.
    pub(crate) async fn put(
        &self,
        hash: BlakeHash,
        bytes: Bytes,
    ) -> std::io::Result<()> {
        match self {
            Self::Mem(m) => {
                m.put(hash, bytes).await;
                Ok(())
            }
            Self::Disk(d) => d.put(hash, bytes).await,
        }
    }

    /// Cheap presence check — does not read the blob.
    ///
    /// Disk-backed: checks the in-memory LRU index only, so a hit does
    /// *not* re-verify BLAKE3. Callers needing a verified hit must call
    /// [`Cache::get`].
    pub(crate) async fn contains(&self, hash: &BlakeHash) -> bool {
        match self {
            Self::Mem(m) => m.contains(hash).await,
            Self::Disk(d) => d.contains(hash).await,
        }
    }
}

// ─── MemCache ─────────────────────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct MemCache {
    inner: RwLock<HashMap<BlakeHash, Bytes>>,
}

impl MemCache {
    async fn get(&self, hash: &BlakeHash) -> Option<Bytes> {
        self.inner.read().await.get(hash).cloned()
    }

    async fn put(&self, hash: BlakeHash, bytes: Bytes) {
        self.inner.write().await.insert(hash, bytes);
    }

    async fn contains(&self, hash: &BlakeHash) -> bool {
        self.inner.read().await.contains_key(hash)
    }
}

// ─── DiskCache ────────────────────────────────────────────────────────────────

pub(crate) struct DiskCache {
    dir: PathBuf,
    budget_bytes: u64,
    state: Mutex<LruState>,
}

#[derive(Default)]
struct LruState {
    /// Per-entry size in bytes. `entries.len()` is the cache cardinality.
    entries: HashMap<BlakeHash, u64>,
    /// LRU ordering: front = oldest, back = most-recently-used.
    /// Invariant: `lru` contains the same keys as `entries`, no duplicates.
    lru: VecDeque<BlakeHash>,
    total_bytes: u64,
}

impl LruState {
    /// Mark `hash` as most-recently-used (called on a hit).
    fn touch(&mut self, hash: &BlakeHash) {
        if let Some(pos) = self.lru.iter().position(|h| h == hash) {
            self.lru.remove(pos);
            self.lru.push_back(*hash);
        }
    }

    /// Insert `hash` with `size` bytes, evicting from the front until total
    /// is within `budget`. Returns the list of evicted hashes (caller must
    /// remove them from disk).
    ///
    /// If `hash` is already present, this is a touch + size-update.
    fn insert(&mut self, hash: BlakeHash, size: u64, budget: u64) -> Vec<BlakeHash> {
        // Replace existing entry — drop the old size first.
        if let Some(old_size) = self.entries.remove(&hash) {
            self.total_bytes = self.total_bytes.saturating_sub(old_size);
            if let Some(pos) = self.lru.iter().position(|h| h == &hash) {
                self.lru.remove(pos);
            }
        }

        let mut evicted = Vec::new();
        // Evict oldest entries until the new write fits.
        while self.total_bytes + size > budget {
            let Some(victim) = self.lru.pop_front() else { break };
            if let Some(victim_size) = self.entries.remove(&victim) {
                self.total_bytes = self.total_bytes.saturating_sub(victim_size);
            }
            evicted.push(victim);
        }

        self.entries.insert(hash, size);
        self.lru.push_back(hash);
        self.total_bytes += size;
        evicted
    }
}

impl DiskCache {
    /// Open the cache directory, creating it if missing and rebuilding the
    /// LRU index from any pre-existing entries. Files whose name isn't a
    /// 64-char hex BlakeHash are ignored (and silently left alone — callers
    /// own the dir).
    ///
    /// Ordering of pre-existing entries in the rebuilt LRU is by file mtime,
    /// oldest first → most-recently-used at the back. mtime is portable
    /// enough for our scales; on platforms with weird/zero mtimes the order
    /// degenerates to filesystem iteration order, which is still a valid
    /// (if arbitrary) LRU starting state.
    pub(crate) fn open(dir: &Path, budget_bytes: u64) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut found: Vec<(std::time::SystemTime, BlakeHash, u64)> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            // Skip + clean up in-flight temp files left over from a crashed
            // write. Two formats are accepted: `<name>.tmp` (legacy) and
            // `<hex>.tmp.<pid>.<n>` (what `put` writes today). Real cache
            // entries are 64-char hex hashes with no `.tmp` substring, so
            // this is a safe match.
            if name.ends_with(".tmp") || name.contains(".tmp.") {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            let Ok(hash) = BlakeHash::from_hex(name) else { continue };
            let Ok(meta) = entry.metadata() else { continue };
            if !meta.is_file() {
                continue;
            }
            let mtime = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            found.push((mtime, hash, meta.len()));
        }
        found.sort_by_key(|(t, _, _)| *t);

        let mut state = LruState::default();
        for (_, hash, size) in found {
            state.entries.insert(hash, size);
            state.lru.push_back(hash);
            state.total_bytes += size;
        }

        // A pre-existing cache may already exceed budget if budget was
        // shrunk between runs. Evict oldest until it fits.
        let dir = dir.to_path_buf();
        while state.total_bytes > budget_bytes {
            let Some(victim) = state.lru.pop_front() else { break };
            if let Some(size) = state.entries.remove(&victim) {
                state.total_bytes = state.total_bytes.saturating_sub(size);
            }
            let _ = std::fs::remove_file(blob_path(&dir, &victim));
        }

        Ok(Self {
            dir,
            budget_bytes,
            state: Mutex::new(state),
        })
    }

    async fn get(&self, hash: &BlakeHash) -> Option<Bytes> {
        // Fast presence-check against the index; without it we'd open + read
        // a missing file on every miss, which is wasteful but not wrong.
        {
            let state = self.state.lock().await;
            if !state.entries.contains_key(hash) {
                return None;
            }
        }
        let path = blob_path(&self.dir, hash);
        let data = match tokio::fs::read(&path).await {
            Ok(d) => d,
            Err(_) => {
                // File vanished out from under us — drop it from the index.
                let mut state = self.state.lock().await;
                if let Some(size) = state.entries.remove(hash) {
                    state.total_bytes = state.total_bytes.saturating_sub(size);
                }
                if let Some(pos) = state.lru.iter().position(|h| h == hash) {
                    state.lru.remove(pos);
                }
                return None;
            }
        };
        let bytes = Bytes::from(data);
        // Verify on read — defends against bit-rot and out-of-band tampering.
        if !hash.verify(&bytes) {
            tracing::warn!(
                %hash,
                "disk cache BLAKE3 mismatch — evicting entry"
            );
            let _ = tokio::fs::remove_file(&path).await;
            let mut state = self.state.lock().await;
            if let Some(size) = state.entries.remove(hash) {
                state.total_bytes = state.total_bytes.saturating_sub(size);
            }
            if let Some(pos) = state.lru.iter().position(|h| h == hash) {
                state.lru.remove(pos);
            }
            return None;
        }
        // Touch LRU.
        self.state.lock().await.touch(hash);
        Some(bytes)
    }

    async fn put(&self, hash: BlakeHash, bytes: Bytes) -> std::io::Result<()> {
        let size = bytes.len() as u64;
        if size > self.budget_bytes {
            // Single blob exceeds the budget. Don't write — would force
            // evicting everything else and still not fit.
            tracing::warn!(
                %hash,
                size,
                budget = self.budget_bytes,
                "disk cache: blob exceeds budget, skipping write"
            );
            return Ok(());
        }
        let final_path = blob_path(&self.dir, &hash);
        // Unique scratch path per put — process pid + monotonic counter.
        // Concurrent puts (especially for the same hash) MUST get distinct
        // temp files so the rename of one doesn't trample the in-flight
        // write of another. Rename-to-final-name is the atomicity point.
        let tmp_path = {
            let mut p = final_path.clone();
            let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            p.set_extension(format!("tmp.{pid}.{n}"));
            p
        };
        tokio::fs::write(&tmp_path, &bytes).await?;
        // rename is atomic on POSIX + on Windows when target doesn't exist;
        // when the target *does* exist Windows differs, but we accept
        // last-writer-wins for the same hash.
        tokio::fs::rename(&tmp_path, &final_path).await?;

        let mut state = self.state.lock().await;
        let evicted = state.insert(hash, size, self.budget_bytes);
        // Drop the lock before doing IO for the evicted files.
        drop(state);
        for victim in evicted {
            let _ = tokio::fs::remove_file(blob_path(&self.dir, &victim)).await;
        }
        Ok(())
    }

    async fn contains(&self, hash: &BlakeHash) -> bool {
        self.state.lock().await.entries.contains_key(hash)
    }
}

fn blob_path(dir: &Path, hash: &BlakeHash) -> PathBuf {
    dir.join(hash.to_hex())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn tmpdir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "xlb-cache-test-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn mem_cache_roundtrip() {
        let cache = Cache::new(None, 1024).unwrap();
        let data = Bytes::from_static(b"hello mem cache");
        let hash = BlakeHash::hash(&data);
        cache.put(hash, data.clone()).await.unwrap();
        assert_eq!(cache.get(&hash).await.as_deref(), Some(&data[..]));
        assert!(cache.contains(&hash).await);
    }

    #[tokio::test]
    async fn disk_cache_roundtrip() {
        let dir = tmpdir("roundtrip");
        let cache = Cache::new(Some(&dir), 1024).unwrap();
        let data = Bytes::from_static(b"hello disk cache");
        let hash = BlakeHash::hash(&data);
        cache.put(hash, data.clone()).await.unwrap();
        assert_eq!(cache.get(&hash).await.as_deref(), Some(&data[..]));
        // Check the file actually landed on disk.
        let on_disk = std::fs::read(dir.join(hash.to_hex())).unwrap();
        assert_eq!(on_disk, data.as_ref());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn disk_cache_survives_reopen() {
        // The whole reason this module exists. Drop the cache, re-open the
        // dir, verify the blob is still there.
        let dir = tmpdir("reopen");
        let data = Bytes::from_static(b"survive across restart");
        let hash = BlakeHash::hash(&data);
        {
            let cache = Cache::new(Some(&dir), 1024).unwrap();
            cache.put(hash, data.clone()).await.unwrap();
        }
        let cache2 = Cache::new(Some(&dir), 1024).unwrap();
        assert!(cache2.contains(&hash).await);
        assert_eq!(cache2.get(&hash).await.as_deref(), Some(&data[..]));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn lru_evicts_oldest_under_budget() {
        // Budget = 100 bytes. Insert 4×40 = 160 bytes worth → oldest must go.
        let dir = tmpdir("lru-evict");
        let cache = Cache::new(Some(&dir), 100).unwrap();
        let mk = |n: u8| Bytes::from(vec![n; 40]);
        let a = mk(b'A');
        let b = mk(b'B');
        let c = mk(b'C');
        let ha = BlakeHash::hash(&a);
        let hb = BlakeHash::hash(&b);
        let hc = BlakeHash::hash(&c);

        cache.put(ha, a.clone()).await.unwrap();
        cache.put(hb, b.clone()).await.unwrap();
        cache.put(hc, c.clone()).await.unwrap();

        // 3×40=120 > budget=100, so A (oldest) must have been evicted.
        assert!(!cache.contains(&ha).await, "A should be evicted (oldest)");
        assert!(cache.contains(&hb).await, "B should survive");
        assert!(cache.contains(&hc).await, "C should survive");
        // Disk reflects the eviction.
        assert!(
            !dir.join(ha.to_hex()).exists(),
            "A's file should be gone from disk"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn lru_get_touches_recency() {
        // Insert A, B; get(A) so A becomes MRU; insert C that forces eviction.
        // B (now oldest) should go, not A.
        let dir = tmpdir("lru-touch");
        let cache = Cache::new(Some(&dir), 100).unwrap();
        let a = Bytes::from(vec![b'A'; 40]);
        let b = Bytes::from(vec![b'B'; 40]);
        let c = Bytes::from(vec![b'C'; 40]);
        let ha = BlakeHash::hash(&a);
        let hb = BlakeHash::hash(&b);
        let hc = BlakeHash::hash(&c);

        cache.put(ha, a.clone()).await.unwrap();
        cache.put(hb, b.clone()).await.unwrap();
        // Touch A — moves it to MRU.
        let _ = cache.get(&ha).await;
        cache.put(hc, c.clone()).await.unwrap();

        assert!(cache.contains(&ha).await, "A was touched, should survive");
        assert!(!cache.contains(&hb).await, "B is now oldest, should be evicted");
        assert!(cache.contains(&hc).await);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn disk_cache_verifies_on_read() {
        // Corrupt a cache file by hand — get must reject + evict.
        let dir = tmpdir("verify");
        let cache = Cache::new(Some(&dir), 1024).unwrap();
        let data = Bytes::from_static(b"real bytes");
        let hash = BlakeHash::hash(&data);
        cache.put(hash, data.clone()).await.unwrap();

        // Out-of-band tamper.
        std::fs::write(dir.join(hash.to_hex()), b"TAMPERED").unwrap();
        assert!(
            cache.get(&hash).await.is_none(),
            "tampered cache entry must be treated as a miss"
        );
        assert!(
            !cache.contains(&hash).await,
            "tampered entry must be evicted from the index"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn concurrent_put_same_hash_atomic() {
        // N concurrent writers for the same hash must not produce a torn
        // file: a reader either sees the full blob or nothing, never a
        // half-rename.
        let dir = tmpdir("concurrent");
        let cache = Arc::new(Cache::new(Some(&dir), 4096).unwrap());
        let data = Bytes::from(vec![0xab; 200]);
        let hash = BlakeHash::hash(&data);

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let data = data.clone();
            tasks.push(tokio::spawn(async move {
                cache.put(hash, data).await.unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        // Final state: blob is present, full, verified.
        let got = cache.get(&hash).await.expect("blob must be present");
        assert_eq!(got, data, "no torn writes");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn shrunk_budget_evicts_on_open() {
        // Write two blobs with a generous budget, then re-open with a tiny
        // budget. The oldest should be evicted at open time.
        let dir = tmpdir("shrink");
        let a = Bytes::from(vec![b'A'; 40]);
        let b = Bytes::from(vec![b'B'; 40]);
        let ha = BlakeHash::hash(&a);
        let hb = BlakeHash::hash(&b);
        {
            let cache = Cache::new(Some(&dir), 1024).unwrap();
            cache.put(ha, a.clone()).await.unwrap();
            // Bump B's mtime past A's so the rebuilt LRU treats A as older.
            // tokio::time::sleep doesn't move mtime on macOS at sub-second
            // resolution reliably; touch via std::fs to guarantee ordering.
            std::thread::sleep(std::time::Duration::from_millis(50));
            cache.put(hb, b.clone()).await.unwrap();
        }
        let cache = Cache::new(Some(&dir), 60).unwrap(); // < 2×40
        assert!(
            !cache.contains(&ha).await,
            "A (older) should have been evicted on open under shrunk budget"
        );
        assert!(
            cache.contains(&hb).await,
            "B (newer) should survive shrunk-budget open"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn temp_files_cleaned_on_open() {
        // A crashed write leaves a *.tmp behind. open() should clear them.
        let dir = tmpdir("tmp-cleanup");
        std::fs::write(dir.join("abc.tmp"), b"crashed write").unwrap();
        std::fs::write(dir.join("def.tmp.123.4"), b"another").unwrap();
        let _cache = Cache::new(Some(&dir), 1024).unwrap();
        assert!(!dir.join("abc.tmp").exists());
        assert!(!dir.join("def.tmp.123.4").exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}
