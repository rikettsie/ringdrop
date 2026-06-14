use std::{io, num::NonZeroU64, path::Path, sync::Arc};

use anyhow::{bail, Context, Result};
use bao_tree::{
    io::{
        fsm::{ResponseDecoder, ResponseDecoderNext},
        BaoContentItem,
    },
    BaoTree, ChunkRanges,
};
use iroh::endpoint::Connection;
use iroh_blobs::{
    api::blobs::BlobStatus,
    format::collection::Collection,
    hashseq::HashSeq,
    store::{fs::FsStore, IROH_BLOCK_SIZE},
    util::RecvStreamAsyncStreamReader,
    BlobFormat, Hash,
};
use tracing::info;

use iroh_rings::Permission;

use super::{encode_ranges_wire, encode_request, Status};

// export_bao emits an 8-byte little-endian content size before the bao tree,
// so the receiver knows the total before any leaf data arrives.
const BAO_SIZE_HEADER: usize = size_of::<u64>();

/// Progress event emitted by [`Node::download_with_progress`].
///
/// Raw-file downloads emit only [`ProgressEvent::Bytes`] events.
/// Directory (`HashSeq`) downloads additionally emit a
/// [`ProgressEvent::FileStart`] before each member file begins.
///
/// [`Node::download_with_progress`]: crate::core::Node::download_with_progress
#[derive(Debug)]
#[non_exhaustive]
pub enum ProgressEvent {
    /// A new file within a directory blob is about to be downloaded.
    FileStart {
        /// 1-based index of this file in the collection.
        index: usize,
        /// Total number of files in the collection.
        total: usize,
        /// Relative path of the file within the collection.
        name: String,
    },
    /// Byte-level progress for the current blob (raw file or collection member).
    Bytes {
        /// Bytes received so far.
        done: u64,
        /// Total expected bytes.
        total: u64,
    },
}

pub(crate) struct RingReceiver {
    store: FsStore,
}

impl RingReceiver {
    pub(crate) fn new(store: FsStore) -> Self {
        RingReceiver { store }
    }

    /// Download `hash` (and its members if it is a `HashSeq`) over `conn`,
    /// then export the result to `dest`.
    ///
    /// For raw blobs, `on_progress` receives [`ProgressEvent::Bytes`] for each
    /// received chunk. For `HashSeq` collections it additionally receives a
    /// [`ProgressEvent::FileStart`] before each member file begins; the tiny
    /// metadata root is downloaded silently.
    pub(crate) async fn download<F: Fn(ProgressEvent) + Send + Sync>(
        &self,
        conn: &Connection,
        hash: Hash,
        format: BlobFormat,
        name: &Option<String>,
        dest: &Path,
        on_progress: Arc<F>,
    ) -> Result<()> {
        // Root blob — skip if already complete (happens on resume).
        let root_complete = matches!(
            self.store.blobs().status(hash).await,
            Ok(BlobStatus::Complete { .. })
        );
        if !root_complete {
            // For HashSeq the root is tiny metadata; suppress misleading progress.
            let progress: Option<Arc<F>> =
                (format == BlobFormat::Raw).then(|| Arc::clone(&on_progress));
            self.fetch_blob(conn, hash, progress).await?;
            info!(hash = %hash, "root blob received");
        }

        // For collections: load the meta blob first (needed by Collection::load),
        // then fetch each named member file with per-file FileStart events.
        if format == BlobFormat::HashSeq {
            let root_bytes = self
                .store
                .blobs()
                .get_bytes(hash)
                .await
                .context("reading root HashSeq")?;
            let hash_seq = HashSeq::try_from(root_bytes).context("parsing HashSeq")?;

            let mut hs_iter = hash_seq.into_iter();
            if let Some(meta_hash) = hs_iter.next() {
                self.fetch_blob(conn, meta_hash, None::<Arc<F>>).await?;
            }

            let collection = Collection::load(hash, &*self.store)
                .await
                .context("loading collection")?;
            let file_total = collection.iter().count();

            for (file_idx, (file_name, item_hash)) in collection.iter().enumerate() {
                info!(item_hash = %item_hash, "fetching collection item");
                on_progress(ProgressEvent::FileStart {
                    index: file_idx + 1,
                    total: file_total,
                    name: file_name.to_owned(),
                });
                self.fetch_blob(conn, *item_hash, Some(Arc::clone(&on_progress)))
                    .await?;
            }
        }

        self.export(hash, format, name, dest).await
    }

    /// Fetch a single raw blob over the connection.
    ///
    /// Skips silently if the blob is already complete in the local store.
    /// `on_progress` is `None` when the caller wants to suppress progress
    /// reporting (e.g. for the tiny HashSeq metadata root).
    async fn fetch_blob<F: Fn(ProgressEvent) + Send + Sync>(
        &self,
        conn: &Connection,
        hash: Hash,
        on_progress: Option<Arc<F>>,
    ) -> Result<()> {
        if matches!(
            self.store.blobs().status(hash).await,
            Ok(BlobStatus::Complete { .. })
        ) {
            info!(%hash, "already complete — skipping");
            return Ok(());
        }

        let already_have = ChunkRanges::default();
        let missing = ChunkRanges::all();

        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&encode_request(hash.as_bytes(), Permission::Read)?)
            .await?;
        send.write_all(&encode_ranges_wire(&already_have)).await?;
        send.finish()?;

        let mut status_byte = [0u8; 1];
        recv.read_exact(&mut status_byte)
            .await
            .context("reading status")?;
        // Status is #[non_exhaustive]
        match Status::try_from(status_byte[0])? {
            Status::Denied => bail!("access denied — not in a ring for this blob"),
            Status::Allowed => {}
            _ => bail!("unexpected status byte from sender"),
        }

        let mut size_buf = [0u8; BAO_SIZE_HEADER];
        recv.read_exact(&mut size_buf)
            .await
            .context("reading bao size header")?;
        let content_size = u64::from_le_bytes(size_buf);
        if let Some(ref p) = on_progress {
            p(ProgressEvent::Bytes {
                done: 0,
                total: content_size,
            });
        }

        if let Some(size) = NonZeroU64::new(content_size) {
            let tree = BaoTree::new(content_size, IROH_BLOCK_SIZE);
            let iroh_blobs::api::blobs::ImportBaoHandle { tx, rx } = self
                .store
                .blobs()
                .import_bao(hash, size, 32)
                .await
                .map_err(io::Error::from)
                .context("starting bao import")?;
            let reader = RecvStreamAsyncStreamReader::new(recv);
            let mut decoder = ResponseDecoder::new(hash.into(), missing, tree, reader);

            // `tx` must be explicitly dropped inside `driver` before it returns so
            // that the store sees the end-of-stream and signals completion via `rx`.
            // If we relied on scope-based drop, `tx` would outlive `driver`'s final
            // poll (still owned by the join state machine), causing `rx.await` to
            // block forever.
            let driver = async move {
                let result = loop {
                    match decoder.next().await {
                        ResponseDecoderNext::Done(_) => break io::Result::Ok(()),
                        ResponseDecoderNext::More((next, item)) => {
                            let item = item.map_err(io::Error::other)?;
                            if let BaoContentItem::Leaf(ref leaf) = item {
                                if let Some(ref p) = on_progress {
                                    p(ProgressEvent::Bytes {
                                        done: leaf.offset + leaf.data.len() as u64,
                                        total: content_size,
                                    });
                                }
                            }
                            tx.send(item).await.map_err(io::Error::from)?;
                            decoder = next;
                        }
                    }
                };
                drop(tx);
                result
            };

            let (drive_res, rx_res) =
                tokio::join!(driver, async move { rx.await.map_err(io::Error::from)? });
            drive_res.context("bao decode")?;
            rx_res.context("bao import")?;
        }

        info!(%hash, "blob received");
        Ok(())
    }

    /// Export a locally complete blob (or collection) to `dest` on the filesystem.
    pub(crate) async fn export(
        &self,
        hash: Hash,
        format: BlobFormat,
        name: &Option<String>,
        dest: &Path,
    ) -> Result<()> {
        let hash_hex = hash.to_string();
        let export_path = if dest.is_dir() {
            dest.join(name.as_deref().unwrap_or(&hash_hex))
        } else {
            dest.to_path_buf()
        };
        let export_path = std::path::absolute(&export_path)?;

        match format {
            BlobFormat::HashSeq => {
                tokio::fs::create_dir_all(&export_path).await?;
                let collection = Collection::load(hash, &*self.store)
                    .await
                    .context("loading collection")?;
                for (name, blob_hash) in collection.iter() {
                    let target = export_path.join(name);
                    if let Some(parent) = target.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    self.store
                        .blobs()
                        .export(*blob_hash, &target)
                        .finish()
                        .await
                        .with_context(|| format!("exporting {name}"))?;
                }
            }
            _ => {
                self.store
                    .blobs()
                    .export(hash, &export_path)
                    .finish()
                    .await
                    .context("exporting blob")?;
            }
        }
        info!("export complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iroh_blobs::Hash;

    use iroh_rings::ALPN;

    use super::{encode_request, Permission};

    #[test]
    fn request_encoding_is_length_prefixed() {
        let hash = Hash::from_bytes([0xab; 32]);
        let encoded = encode_request(hash.as_bytes(), Permission::Read).unwrap();
        let len = u16::from_le_bytes(encoded[..2].try_into().unwrap());
        assert_eq!(len as usize, 32);
        // wire format: [u16 len][resource id][op byte]
        assert_eq!(&encoded[2..2 + 32], hash.as_bytes());
    }

    #[test]
    fn alpn_is_iroh_rings_v2() {
        assert_eq!(ALPN, b"/iroh-rings/2");
    }
}
