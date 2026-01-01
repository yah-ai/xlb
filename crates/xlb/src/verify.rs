//! BLAKE3 streaming verifier — unified glue for QUIC (iroh-blobs) and HTTP paths.
//!
//! Both transports feed their bytes through a `Verifier` before returning to
//! the `FetchChain`, so CDN or peer tampering is caught at the source rather
//! than surfacing as a hash mismatch in the chain.

use bytes::{Bytes, BytesMut};

use crate::{BlakeHash, Error, Result};

/// Accumulate byte chunks and assert the BLAKE3 root hash on completion.
///
/// Create one per fetch, feed chunks with [`Verifier::update`], finalize
/// with [`Verifier::finish`]. Short-lived by design — do not reuse across
/// separate fetches.
pub(crate) struct Verifier {
    expected: BlakeHash,
    hasher: blake3::Hasher,
    buf: BytesMut,
}

impl Verifier {
    pub fn new(expected: BlakeHash) -> Self {
        Self {
            expected,
            hasher: blake3::Hasher::new(),
            buf: BytesMut::new(),
        }
    }

    /// Feed a chunk of bytes into the accumulator.
    pub fn update(&mut self, chunk: &[u8]) {
        self.hasher.update(chunk);
        self.buf.extend_from_slice(chunk);
    }

    /// Verify the root BLAKE3 and return the accumulated bytes on success.
    ///
    /// Returns `Err(HashMismatch)` if the hash doesn't match; the buffered
    /// bytes are discarded in that case.
    pub fn finish(self) -> Result<Bytes> {
        let actual = BlakeHash::from_bytes(*self.hasher.finalize().as_bytes());
        if actual != self.expected {
            return Err(Error::HashMismatch { expected: self.expected, actual });
        }
        Ok(self.buf.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_correct_hash() {
        let data = b"hello verifier";
        let hash = BlakeHash::hash(data);
        let mut v = Verifier::new(hash);
        v.update(data);
        assert!(v.finish().is_ok());
    }

    #[test]
    fn verify_chunked_input() {
        let data = b"chunk one chunk two chunk three";
        let hash = BlakeHash::hash(data);
        let mut v = Verifier::new(hash);
        // Feed in three chunks.
        v.update(&data[..10]);
        v.update(&data[10..20]);
        v.update(&data[20..]);
        let result = v.finish().unwrap();
        assert_eq!(&result[..], data);
    }

    #[test]
    fn verify_wrong_hash() {
        let data = b"correct bytes";
        let wrong_hash = BlakeHash::hash(b"different bytes");
        let mut v = Verifier::new(wrong_hash);
        v.update(data);
        assert!(matches!(v.finish(), Err(Error::HashMismatch { .. })));
    }

    #[test]
    fn verify_empty_blob() {
        let data = b"";
        let hash = BlakeHash::hash(data);
        let v = Verifier::new(hash);
        // No update calls — empty blob.
        let result = v.finish().unwrap();
        assert_eq!(result.len(), 0);
    }
}
