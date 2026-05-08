//! The ringdrop node.
//!
//! Wraps:
//!  - an iroh `Endpoint`        — QUIC, NAT traversal, relay fallback
//!  - an iroh-blobs `FsStore`   — BLAKE3 chunking, outboard, bitfield tracking
//!  - a `RingGate`              — custom ALPN with per-blob access control
//!  - a `Registry`              — ring membership and file tagging
//!
//! # Resumption
//!
//! The FsStore writes a `.bitfield` file alongside each partial blob.  This
//! bitfield records which 16 KiB chunk groups have been received **and
//! verified**.  On reconnect:
//!
//! 1. Receiver reads its local bitfield (`store.observe(hash)`) and inverts it
//!    to produce the set of still-missing ranges.
//! 2. Missing ranges are encoded and sent in the request header.
//! 3. Sender streams only those ranges from its own FsStore using bao encoding.
//! 4. Receiver writes each incoming chunk group, verifying it against the
//!    BLAKE3 outboard before committing it to the `.data` file and updating
//!    the `.bitfield`.
//! 5. If the connection drops again, step 1 picks up from the last committed
//!    chunk group — no already-verified data is re-transferred.

use std::{
    io,
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use bao_tree::{
    io::{
        fsm::{ResponseDecoder, ResponseDecoderNext},
        BaoContentItem,
    },
    BaoTree, ChunkRanges,
};
use futures_lite::StreamExt;
use iroh::{endpoint::presets, protocol::Router, Endpoint, EndpointAddr};
use iroh_blobs::{
    api::blobs::{AddPathOptions, BlobStatus, ImportMode},
    format::collection::Collection,
    store::{fs::FsStore, GcConfig, IROH_BLOCK_SIZE},
    util::RecvStreamAsyncStreamReader,
    BlobFormat, Hash, HashAndFormat,
};
use std::time::Duration;
use tracing::info;
use walkdir::WalkDir;

use super::protocol::{encode_ranges_wire, RingGate, Status, SC_ALPN};
use crate::config::Config;
use crate::registry::Registry;
use crate::ticket::ShareTicket;

/// Length of the u64-le content-size header that opens every bao-encoded stream.
const BAO_SIZE_HEADER: usize = size_of::<u64>();

pub struct Node {
    pub endpoint: Endpoint,
    pub store: FsStore,
    pub registry: Registry,
    router: Router,
}

impl Node {
    pub async fn start(data_dir: impl AsRef<Path>, cfg: Config) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&data_dir).await?;

        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(cfg.secret_key)
            .bind()
            .await
            .context("binding iroh endpoint")?;

        // FsStore — BLAKE3 persistent store.
        // Per-blob disk layout (data_dir/blobs/):
        //   <hash>.data     — raw bytes, never mutated after import
        //   <hash>.obao4    — flattened BLAKE3 hash tree (16 KiB chunk groups)
        //   <hash>.bitfield — bitmask of validated chunk groups (crash-safe)
        let blobs_dir = data_dir.join("blobs");
        let db_path = blobs_dir.join("blobs.db");
        let mut fs_opts = iroh_blobs::store::fs::options::Options::new(&blobs_dir);
        fs_opts.gc = Some(GcConfig {
            interval: Duration::from_secs(30),
            add_protected: None,
        });
        let store = FsStore::load_with_opts(db_path, fs_opts)
            .await
            .context("loading FsStore")?;

        let registry =
            Registry::open(data_dir.join("registry.redb")).context("opening registry")?;

        let gate = RingGate::new(registry.clone(), store.clone());

        let router = Router::builder(endpoint.clone())
            .accept(SC_ALPN, gate)
            .spawn();

        endpoint.online().await;
        info!(peer_id = %endpoint.id(), "node online");

        Ok(Node {
            endpoint,
            store,
            registry,
            router,
        })
    }

    pub fn node_addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    pub async fn import_file(&self, path: impl AsRef<Path>) -> Result<(Hash, BlobFormat)> {
        let path = std::path::absolute(path.as_ref())?;
        info!(path = %path.display(), "importing file");
        let tag_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned());
        let tag = self
            .store
            .blobs()
            .add_path_with_opts(AddPathOptions {
                path,
                mode: ImportMode::TryReference,
                format: BlobFormat::Raw,
            })
            .temp_tag()
            .await
            .context("add_path")?;
        let hash = tag.hash();
        let format = BlobFormat::Raw;
        let tag_key = tag_name.unwrap_or_else(|| hash.to_string());
        // Persist: replace temp tag with a named tag so GC won't collect this blob.
        self.store
            .tags()
            .set(tag_key, HashAndFormat { hash, format })
            .await
            .context("pinning blob tag")?;
        info!(%hash, "imported — outboard computed");
        Ok((hash, format))
    }

    pub async fn import_directory(&self, dir: impl AsRef<Path>) -> Result<(Hash, BlobFormat)> {
        let dir = dir.as_ref();
        info!(dir = %dir.display(), "importing directory");
        let dir_name = dir.file_name().map(|n| n.to_string_lossy().into_owned());

        let mut files: Vec<(String, PathBuf)> = Vec::new();
        for entry in WalkDir::new(dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let rel = entry
                .path()
                .strip_prefix(dir)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .into_owned();
            files.push((rel, entry.path().to_path_buf()));
        }

        let mut collection = Collection::default();
        for (name, path) in files {
            let tag = self
                .store
                .blobs()
                .add_path_with_opts(AddPathOptions {
                    path: std::path::absolute(&path)?,
                    mode: ImportMode::TryReference,
                    format: BlobFormat::Raw,
                })
                .temp_tag()
                .await?;
            info!(name, hash = %tag.hash(), "added to collection");
            collection.push(name, tag.hash());
        }

        let col_tag = collection.store(&self.store).await?;
        let hash = col_tag.hash();
        let format = BlobFormat::HashSeq;
        let tag_key = dir_name.unwrap_or_else(|| hash.to_string());
        // Persist: named tag on the collection; GC follows HashSeq refs to keep member blobs.
        self.store
            .tags()
            .set(tag_key, HashAndFormat { hash, format })
            .await
            .context("pinning collection tag")?;
        info!(%hash, "collection stored");
        Ok((hash, format))
    }

    /// List all blobs that have been imported (hash + format + tag name).
    pub async fn list_blobs(&self) -> Result<Vec<(Hash, BlobFormat, Option<String>)>> {
        let mut stream = self.store.tags().list().await?;
        let mut blobs = Vec::new();
        while let Some(item) = stream.next().await {
            let info = item?;
            let name = std::str::from_utf8(&info.name.0).ok().map(|s| s.to_owned());
            blobs.push((info.hash, info.format, name));
        }
        Ok(blobs)
    }

    /// Remove a blob from the store. Ring tags must be removed separately via the registry.
    /// Actual disk reclamation happens on the next GC cycle (during `rdrop share`).
    pub async fn delete_blob(&self, hash: Hash) -> Result<()> {
        let mut stream = self.store.tags().list().await?;
        let mut to_delete = Vec::new();
        while let Some(item) = stream.next().await {
            let info = item?;
            if info.hash == hash {
                to_delete.push(info.name.0.clone());
            }
        }
        drop(stream);
        if to_delete.is_empty() {
            bail!("no tag found for hash {hash}");
        }
        for name in to_delete {
            self.store
                .tags()
                .delete(name)
                .await
                .context("removing blob tag")?;
        }
        Ok(())
    }

    pub fn make_ticket(&self, hash: Hash, format: BlobFormat, name: Option<String>) -> ShareTicket {
        let addr = self.node_addr();
        match format {
            BlobFormat::HashSeq => ShareTicket::new_collection(addr, hash, name),
            _ => ShareTicket::new(addr, hash, name),
        }
    }

    /// Download a blob, resuming automatically from any prior partial transfer.
    ///
    /// 1. Read local bitfield — which 16 KiB chunk groups do we already have?
    /// 2. Send only the *missing* range set in the request header.
    /// 3. Receive the bao-encoded stream for those ranges only.
    /// 4. Decode and verify each chunk group before writing it to the FsStore.
    ///    The `.bitfield` file is updated after each verified group.
    /// 5. On crash or disconnect, repeat from step 1 — no verified data
    ///    is retransferred.
    pub async fn download(&self, ticket: &ShareTicket, dest: impl AsRef<Path>) -> Result<()> {
        self.download_impl(ticket, dest, |_, _| {}).await
    }

    /// Like [`download`] but calls `on_progress(bytes_read, total_wire)` after
    /// each received chunk so the caller can drive a progress indicator.
    pub async fn download_with_progress<F: Fn(u64, u64) + Send>(
        &self,
        ticket: &ShareTicket,
        dest: impl AsRef<Path>,
        on_progress: F,
    ) -> Result<()> {
        self.download_impl(ticket, dest, on_progress).await
    }

    async fn download_impl<F: Fn(u64, u64) + Send>(
        &self,
        ticket: &ShareTicket,
        dest: impl AsRef<Path>,
        on_progress: F,
    ) -> Result<()> {
        let dest = dest.as_ref().to_path_buf();
        let hash = ticket.hash();
        let format = ticket.format();
        let node_addr = ticket.node_addr().clone();

        info!(hash = %hash, from = %node_addr.id, "starting download");

        let already_have: ChunkRanges = match self.store.blobs().status(hash).await {
            Ok(BlobStatus::Complete { .. }) => ChunkRanges::all(),
            _ => ChunkRanges::default(),
        };

        let missing = ChunkRanges::all() & !already_have.clone();
        if missing.is_empty() {
            info!(hash = %hash, "all chunks present — skipping download");
            return self.export(hash, format, &ticket.name, &dest).await;
        }

        // Hold a temp tag for the duration of the download so GC doesn't unlink
        // the partial .data file while we're writing it (large files take > 30s).
        let _batch = self
            .store
            .blobs()
            .batch()
            .await
            .context("creating download scope")?;
        let _tt = _batch
            .temp_tag(HashAndFormat { hash, format })
            .await
            .context("creating temp tag")?;

        info!(hash = %hash, "sending range request");

        let conn = self
            .endpoint
            .connect(node_addr, SC_ALPN)
            .await
            .context("connecting to sender")?;

        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(hash.as_bytes()).await?;
        send.write_all(&encode_ranges_wire(&already_have)).await?;
        send.finish()?;

        let mut status_byte = [0u8; 1];
        recv.read_exact(&mut status_byte)
            .await
            .context("reading status")?;

        match Status::try_from(status_byte[0])? {
            Status::Denied => bail!(
                "access denied — not in a ring for this blob.\n\
                 Your PeerId: {}",
                self.endpoint.id()
            ),
            Status::Allowed => {}
        }

        // First 8 bytes of the bao stream are the u64-le content size.
        let mut size_buf = [0u8; BAO_SIZE_HEADER];
        recv.read_exact(&mut size_buf)
            .await
            .context("reading bao size header")?;
        let content_size = u64::from_le_bytes(size_buf);

        on_progress(0, content_size);

        // Drive the bao decoder manually so we can report content-byte progress
        // via BaoContentItem::Leaf offsets rather than raw wire bytes.
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
                                on_progress(leaf.offset + leaf.data.len() as u64, content_size);
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

        info!(hash = %hash, "all chunks verified and written");

        self.export(hash, format, &ticket.name, &dest).await
    }

    async fn export(
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

    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        // FsStore batches writes; the RPC ack for set/import arrives before
        // the redb transaction commits.  sync_db() returns only after all
        // pending batches are committed, so data is durable before we exit.
        self.store
            .sync_db()
            .await
            .context("flushing blob store to disk")?;
        Ok(())
    }
}
