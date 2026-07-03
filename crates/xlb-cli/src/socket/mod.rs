pub mod client;
pub mod protocol;
pub mod server;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Upper bound on a single length-prefixed control frame. Control-plane JSON
/// is tiny; this cap prevents a malicious/corrupt length prefix from driving an
/// unbounded heap allocation before any bytes are read.
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Write a 4-byte big-endian length-prefixed JSON frame.
pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: serde::Serialize,
{
    let json = serde_json::to_vec(value)?;
    writer.write_all(&(json.len() as u32).to_be_bytes()).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a 4-byte big-endian length-prefixed JSON frame.
pub async fn read_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncReadExt + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        anyhow::bail!("control frame too large: {len} bytes (max {MAX_FRAME_BYTES})");
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(serde_json::from_slice(&buf)?)
}
