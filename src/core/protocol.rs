//! Wire protocol constants, helpers, and the sender-side connection handler.
//!
//! # Wire protocol `iroh/ring/1`
//!
//! ```text
//! Request (receiver → sender)
//!  [32 B]  BLAKE3 hash of the blob being requested
//!  [ 4 B]  u32-le: number of already-have chunk-group ranges (N)
//!  [N×16B] N × (start: u64-le, end: u64-le) chunk-group index pairs
//!          These are 16 KiB chunk groups (matching the FsStore bitfield
//!          granularity).  An empty list means "I have nothing, send all".
//!
//! Response (sender → receiver)
//!  [ 1 B]  status: 0x00 = DENIED, 0x01 = ALLOWED
//!  if DENIED: stream closes.
//!  if ALLOWED:
//!    [rest]  bao-encoded stream covering only the requested (missing) ranges.
//!            The first 8 bytes are the total blob size (u64-le), followed by
//!            BLAKE3 parent hashes from the outboard tree and the chunk data,
//!            enabling the receiver to verify each chunk independently.
//! ```

use std::{fmt, io};

use anyhow::{bail, Context, Result};
use bao_tree::{ChunkNum, ChunkRanges};
use bytes::Bytes;
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
    EndpointId,
};
use iroh_blobs::{store::fs::FsStore, Hash};
use iroh_io::AsyncStreamWriter;
use tracing::{debug, info, warn};

use crate::registry::Registry;

pub const SC_ALPN: &[u8] = b"iroh/ring/1";

/// Thin `AsyncStreamWriter` wrapper so `export_bao` can write directly into a
/// QUIC send stream without buffering the entire bao-encoded payload in RAM.
struct SendStreamWriter<'a>(&'a mut iroh::endpoint::SendStream);

impl AsyncStreamWriter for SendStreamWriter<'_> {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        Ok(self.0.write_all(data).await?)
    }
    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        Ok(self.0.write_chunk(data).await?)
    }
    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[repr(u8)]
pub(super) enum Status {
    Denied = 0x00,
    Allowed = 0x01,
}

impl TryFrom<u8> for Status {
    type Error = anyhow::Error;
    fn try_from(b: u8) -> Result<Self> {
        match b {
            0x00 => Ok(Status::Denied),
            0x01 => Ok(Status::Allowed),
            _ => Err(anyhow::anyhow!("unexpected status byte: 0x{b:02x}")),
        }
    }
}

/// Encode chunk-group ranges into wire bytes:
///   [u32-le count] [count × (start u64-le, end u64-le)]
pub(super) fn encode_ranges_wire(ranges: &ChunkRanges) -> Vec<u8> {
    let boundaries = ranges.boundaries();
    debug_assert!(
        boundaries.len().is_multiple_of(2),
        "invariant: already-have ranges are always bounded"
    );
    let pair_count = (boundaries.len() / 2) as u32;
    let mut out = Vec::with_capacity(4 + pair_count as usize * 16);
    out.extend_from_slice(&pair_count.to_le_bytes());
    let mut i = 0;
    while i + 1 < boundaries.len() {
        out.extend_from_slice(&boundaries[i].0.to_le_bytes());
        out.extend_from_slice(&boundaries[i + 1].0.to_le_bytes());
        i += 2;
    }
    out
}

fn decode_ranges_wire(count: u32, raw: &[u8]) -> Result<ChunkRanges> {
    let mut ranges = ChunkRanges::empty();
    for i in 0..count as usize {
        let base = i * 16;
        if base + 16 > raw.len() {
            bail!("range data truncated at index {i}");
        }
        let start = u64::from_le_bytes(
            raw[base..base + 8]
                .try_into()
                .expect("invariant: slice is exactly 8 bytes"),
        );
        let end = u64::from_le_bytes(
            raw[base + 8..base + 16]
                .try_into()
                .expect("invariant: slice is exactly 8 bytes"),
        );
        ranges |= ChunkRanges::from(ChunkNum(start)..ChunkNum(end));
    }
    Ok(ranges)
}

#[derive(Clone)]
pub(super) struct RingGate {
    registry: Registry,
    store: FsStore,
}

impl fmt::Debug for RingGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingGate").finish_non_exhaustive()
    }
}

impl RingGate {
    pub(super) fn new(registry: Registry, store: FsStore) -> Self {
        RingGate { registry, store }
    }
}

impl ProtocolHandler for RingGate {
    fn accept(
        &self,
        conn: Connection,
    ) -> impl std::future::Future<Output = Result<(), AcceptError>> + Send {
        let gate = self.clone();
        async move {
            gate.handle(conn)
                .await
                .map_err(|e| AcceptError::from_boxed(e.into()))
        }
    }
}

impl RingGate {
    async fn handle(&self, conn: Connection) -> Result<()> {
        let peer: EndpointId = conn.remote_id();
        while let Ok((send, recv)) = conn.accept_bi().await {
            let gate = self.clone();
            tokio::spawn(async move {
                if let Err(e) = gate.handle_request(peer, send, recv).await {
                    warn!(%peer, "request error: {e:#}");
                }
            });
        }
        Ok(())
    }

    async fn handle_request(
        &self,
        peer: EndpointId,
        mut send: iroh::endpoint::SendStream,
        mut recv: iroh::endpoint::RecvStream,
    ) -> Result<()> {
        let mut hash_bytes = [0u8; 32];
        recv.read_exact(&mut hash_bytes)
            .await
            .context("reading hash")?;
        let hash = Hash::from_bytes(hash_bytes);

        let mut count_buf = [0u8; 4];
        recv.read_exact(&mut count_buf)
            .await
            .context("reading range count")?;
        let range_count = u32::from_le_bytes(count_buf);

        let range_data_len = range_count as usize * 16;
        let mut range_data = vec![0u8; range_data_len];
        if range_data_len > 0 {
            recv.read_exact(&mut range_data)
                .await
                .context("reading ranges")?;
        }

        let already_have = decode_ranges_wire(range_count, &range_data)?;
        let missing = ChunkRanges::all() & !already_have;

        debug!(%peer, %hash, "request — {} already-have range(s)", range_count);

        if !self.registry.is_allowed(&peer, &hash).unwrap_or(false) {
            warn!(%peer, %hash, "DENIED");
            send.write_all(&[Status::Denied as u8]).await?;
            send.finish()?;
            return Ok(());
        }

        info!(%peer, %hash, "ALLOWED — streaming missing ranges");
        send.write_all(&[Status::Allowed as u8]).await?;

        self.store
            .blobs()
            .export_bao(hash, missing)
            .write(&mut SendStreamWriter(&mut send))
            .await
            .context("bao streaming failed")?;

        send.finish()?;

        info!(%peer, %hash, "done");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bao_tree::ChunkNum;

    #[test]
    fn encode_decode_empty_ranges() {
        let ranges = ChunkRanges::empty();
        let encoded = encode_ranges_wire(&ranges);
        assert_eq!(&encoded[..4], &0u32.to_le_bytes());
        let decoded = decode_ranges_wire(0, &[]).unwrap();
        assert_eq!(decoded, ChunkRanges::empty());
    }

    #[test]
    fn encode_decode_single_range() {
        let ranges = ChunkRanges::from(ChunkNum(0)..ChunkNum(10));
        let encoded = encode_ranges_wire(&ranges);
        let count = u32::from_le_bytes(encoded[..4].try_into().unwrap());
        let decoded = decode_ranges_wire(count, &encoded[4..]).unwrap();
        assert_eq!(decoded, ranges);
    }

    #[test]
    fn encode_decode_multiple_ranges() {
        let r1 = ChunkRanges::from(ChunkNum(0)..ChunkNum(4));
        let r2 = ChunkRanges::from(ChunkNum(10)..ChunkNum(20));
        let ranges = r1 | r2;
        let encoded = encode_ranges_wire(&ranges);
        let count = u32::from_le_bytes(encoded[..4].try_into().unwrap());
        let decoded = decode_ranges_wire(count, &encoded[4..]).unwrap();
        assert_eq!(decoded, ranges);
    }

    #[test]
    fn decode_truncated_data_errors() {
        let result = decode_ranges_wire(1, &[0u8; 8]); // needs 16 bytes per range
        assert!(result.is_err());
    }
}
