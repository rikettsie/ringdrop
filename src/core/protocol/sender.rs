use std::{fmt, io};

use anyhow::{Context, Result};
use bao_tree::ChunkRanges;
use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
    EndpointId,
};
use iroh_blobs::{hashseq::HashSeq, store::fs::FsStore, BlobFormat, Hash};
use iroh_io::AsyncStreamWriter;
use tracing::{debug, info, warn};

use crate::registry::Registry;

use super::{decode_ranges_wire, Status};

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

#[derive(Clone)]
pub(crate) struct RingGate {
    registry: Registry,
    store: FsStore,
}

impl fmt::Debug for RingGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingGate").finish_non_exhaustive()
    }
}

impl RingGate {
    pub(crate) fn new(registry: Registry, store: FsStore) -> Self {
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
    /// Returns true if `hash` is referenced by any collection the peer is allowed to access.
    /// Iterates over all HashSeq tags; called only when the direct registry check fails.
    async fn is_member_of_allowed_collection(&self, peer: &EndpointId, hash: &Hash) -> bool {
        let Ok(mut stream) = self.store.tags().list().await else {
            return false;
        };
        while let Some(Ok(info)) = stream.next().await {
            if info.format != BlobFormat::HashSeq {
                continue;
            }
            if !self.registry.is_allowed(peer, &info.hash).unwrap_or(false) {
                continue;
            }
            // Read the raw HashSeq bytes and check if hash appears anywhere in it.
            // This covers both the metadata blob and the file blobs.
            if let Ok(bytes) = self.store.blobs().get_bytes(info.hash).await {
                if let Ok(seq) = HashSeq::try_from(bytes) {
                    if seq.into_iter().any(|h| &h == hash) {
                        return true;
                    }
                }
            }
        }
        false
    }

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

        debug!(%peer, %hash, "request — {} already-have ranges", range_count);

        let allowed = self.registry.is_allowed(&peer, &hash).unwrap_or(false)
            || self.is_member_of_allowed_collection(&peer, &hash).await;
        if !allowed {
            warn!(%peer, %hash, "DENIED");
            send.write_all(&[Status::Denied as u8]).await?;
            send.finish()?;
            return Ok(());
        }

        send.write_all(&[Status::Allowed as u8]).await?;
        info!(%peer, %hash, "TRANSFER ALLOWED");

        match self
            .store
            .blobs()
            .export_bao(hash, missing)
            .write(&mut SendStreamWriter(&mut send))
            .await
        {
            Ok(()) => {
                send.finish()?;
                info!(%peer, %hash, "TRANSFER COMPLETED");
            }
            Err(e) => {
                warn!(%peer, %hash, "TRANSFER FAILED");
                return Err(e).context("bao streaming failed");
            }
        }

        Ok(())
    }
}
