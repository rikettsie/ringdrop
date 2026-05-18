//! The ringdrop node.
//!
//! Wraps:
//!  - an iroh `Endpoint`        — QUIC, NAT traversal, relay fallback
//!  - an iroh-blobs `FsStore`   — BLAKE3 chunking, outboard, bitfield tracking
//!  - a `RingGate`              — custom ALPN with per-blob access control
//!  - a `Registry`              — ring membership and file tagging

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use futures_lite::StreamExt;
use iroh::{endpoint::presets, protocol::Router, Endpoint, EndpointAddr};
use iroh_blobs::{
    api::blobs::{AddPathOptions, BlobStatus, ImportMode},
    format::collection::Collection,
    store::{fs::FsStore, GcConfig},
    BlobFormat, Hash, HashAndFormat,
};
use tracing::info;
use walkdir::WalkDir;

use super::protocol::{RingGate, RingReceiver, SC_ALPN};
use super::ticket::ShareTicket;
use crate::config::Config;
use iroh_rings::FsTransfer;
use iroh_rings::Registry;

/// A ringdrop P2P node.
///
/// Owns an iroh QUIC [`Endpoint`], an iroh-blobs [`FsStore`], a `RingGate`
/// that enforces ring-based access control, and the [`Registry`] that tracks
/// ring membership.
///
/// Create a node with [`Node::start`]; shut it down cleanly with
/// [`Node::shutdown`] so the blob store is flushed to disk before the process
/// exits.
pub struct Node<R> {
    pub endpoint: Endpoint,
    pub store: FsStore,
    pub registry: R,
    router: Router,
}

impl<R: Registry + Clone + Send + Sync + 'static> Node<R> {
    /// Start a node, binding a QUIC endpoint and loading the blob store from `data_dir`.
    ///
    /// The node is immediately reachable for inbound connections once this
    /// returns. The blob store is created under `data_dir/blobs/` if it does
    /// not yet exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the QUIC endpoint cannot bind, or if the blob store
    /// cannot be opened or created.
    pub async fn start(data_dir: impl AsRef<Path>, cfg: Config, registry: R) -> Result<Self> {
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

        let gate = RingGate::new(
            registry.clone(),
            FsTransfer::new(store.clone(), registry.clone()),
        );

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

    /// Returns the network address of this node (relay URL + node ID).
    pub fn node_addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Import a single file into the blob store.
    ///
    /// The file is chunked, BLAKE3-hashed, and pinned with a named tag (the
    /// filename). Returns the root hash and `BlobFormat::Raw`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the store operation fails.
    pub async fn import_file(&self, path: impl AsRef<Path>) -> Result<(Hash, BlobFormat)> {
        let path = std::path::absolute(path.as_ref())?;
        info!(path = %path.display(), "importing file");
        let tag_name = path.file_name().map(|n| n.to_string_lossy().into_owned());
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

    /// Import a directory into the blob store as an iroh-blobs collection.
    ///
    /// Each file under `dir` is imported individually; the resulting hashes are
    /// assembled into a `HashSeq` collection pinned under the directory name.
    /// Returns the collection root hash and `BlobFormat::HashSeq`.
    ///
    /// # Errors
    ///
    /// Returns an error if any file cannot be read or the store operation fails.
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

    /// List all blobs in the local store as `(hash, format, tag_name)` triples.
    ///
    /// # Errors
    ///
    /// Returns an error if the tag store cannot be read.
    pub async fn list_blobs(&self) -> Result<Vec<(Hash, BlobFormat, String)>> {
        let mut stream = self.store.tags().list().await?;
        let mut blobs = Vec::new();
        while let Some(item) = stream.next().await {
            let info = item?;
            let name = String::from_utf8_lossy(&info.name.0).into_owned();
            blobs.push((info.hash, info.format, name));
        }
        Ok(blobs)
    }

    /// Remove a blob from the local store by deleting its named tags.
    ///
    /// Ring tags in the registry must be removed separately. Disk space is
    /// reclaimed on the next GC cycle (every 30 s while the node is running).
    ///
    /// # Errors
    ///
    /// Returns an error if no tag is found for the given hash, or if the tag
    /// deletion fails.
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
            anyhow::bail!("no tag found for hash {hash}");
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

    /// Build a [`ShareTicket`] for a locally-stored blob.
    ///
    /// The ticket embeds the relay URL and node ID but omits direct IP
    /// addresses — tickets remain valid across daemon restarts and IP changes.
    pub fn make_ticket(&self, hash: Hash, format: BlobFormat, name: Option<String>) -> ShareTicket {
        let full_addr = self.node_addr();
        // Omit direct IP addresses: tickets are long-lived and may be used after the
        // daemon has restarted (new random port, possibly different IP). The relay URL
        // + node ID are always valid; iroh still negotiates a direct connection via
        // hole-punching during the relay handshake when both peers are on the same LAN.
        let addr = full_addr
            .relay_urls()
            .fold(EndpointAddr::new(full_addr.id), |a, url| {
                a.with_relay_url(url.clone())
            });
        match format {
            BlobFormat::HashSeq => ShareTicket::new_collection(addr, hash, name),
            _ => ShareTicket::new(addr, hash, name),
        }
    }

    /// Import a file or directory, dispatching to [`Node::import_file`] or
    /// [`Node::import_directory`] based on whether `path` is a file or a dir.
    ///
    /// # Errors
    ///
    /// Returns an error if the import fails (see the individual methods for details).
    pub async fn import_path(&self, path: &std::path::Path) -> Result<(Hash, BlobFormat)> {
        if path.is_dir() {
            self.import_directory(path).await
        } else {
            self.import_file(path).await
        }
    }

    /// Download the blob described by `ticket` and export it under `dest`.
    ///
    /// If the blob is already complete in the local store the download is
    /// skipped entirely. For collections, each member blob is fetched
    /// individually before the directory is reassembled on disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the remote peer denies access, the connection
    /// cannot be established, or the export to disk fails.
    pub async fn download(&self, ticket: &ShareTicket, dest: impl AsRef<Path>) -> Result<()> {
        self.download_impl(ticket, dest, |_, _| {}).await
    }

    /// Like [`Node::download`], but calls `on_progress(bytes_done, total_bytes)`
    /// as each chunk is received, suitable for driving a progress bar.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Node::download`].
    pub async fn download_with_progress<F: Fn(u64, u64) + Send + Sync>(
        &self,
        ticket: &ShareTicket,
        dest: impl AsRef<Path>,
        on_progress: F,
    ) -> Result<()> {
        self.download_impl(ticket, dest, on_progress).await
    }

    async fn download_impl<F: Fn(u64, u64) + Send + Sync>(
        &self,
        ticket: &ShareTicket,
        dest: impl AsRef<Path>,
        on_progress: F,
    ) -> Result<()> {
        let dest = dest.as_ref().to_path_buf();
        let hash = ticket.hash();
        let format = ticket.format();
        let on_progress = Arc::new(on_progress);

        info!(hash = %hash, from = %ticket.node_addr().id, "starting download");

        let client = RingReceiver::new(self.store.clone());

        // Fast path: raw blob already complete — export without touching the network.
        if format == BlobFormat::Raw
            && matches!(
                self.store.blobs().status(hash).await,
                Ok(BlobStatus::Complete { .. })
            )
        {
            info!(hash = %hash, "all chunks present — skipping download");
            return client.export(hash, format, &ticket.name, &dest).await;
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

        let conn = self
            .endpoint
            .connect(ticket.node_addr().clone(), SC_ALPN)
            .await
            .context("connecting to sender")?;

        client
            .download(&conn, hash, format, &ticket.name, &dest, on_progress)
            .await
    }

    /// Shut down the node, flushing all pending blob-store writes to disk.
    ///
    /// Must be called before the process exits to avoid data loss: the
    /// iroh-blobs store batches redb transactions and `sync_db` waits for all
    /// of them to commit.
    ///
    /// # Errors
    ///
    /// Returns an error if the router shutdown or the blob-store flush fails.
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
