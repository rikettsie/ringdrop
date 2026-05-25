//! The ringdrop node.
//!
//! Wraps:
//!  - an iroh `Endpoint`        — QUIC, NAT traversal, relay fallback
//!  - an iroh-blobs `FsStore`   — BLAKE3 chunking, outboard, bitfield tracking
//!  - a `RingGate`              — custom ALPN with permission-typed access control
//!  - a `Registry`              — ring membership and permission-typed resource associations

use std::{
    collections::HashSet,
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

use super::grants::GrantStore;
use super::protocol::catalog::{
    decode_entries, CatalogEntry, CatalogHandler, ALLOWED, BLOB_LIST, CATALOG_ALPN,
};
use super::protocol::{RingGate, RingReceiver, ALPN};
use super::ticket::ShareTicket;
use crate::config::Config;
use iroh_rings::{FsTransfer, Permission, Registry, OPEN_RING_NAME};

use crate::util::parse_peer_id;

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
    /// The iroh QUIC endpoint — manages connections, NAT traversal, and relay fallback.
    pub endpoint: Endpoint,
    /// The iroh-blobs persistent store — holds all locally imported and received blobs.
    pub store: FsStore,
    /// The ring registry — tracks ring membership and permission-typed resource associations.
    pub registry: R,
    /// The grant store — controls which peers may invoke catalog operations.
    pub grants: GrantStore,
    router: Router,
}

/// Returns the set of ring names accessible to `peer`: all rings the peer
/// belongs to, plus the built-in open ring (which is accessible to everyone).
///
/// # Errors
///
/// Returns an error if `peer` is not a valid [`EndpointId`] encoding or if a
/// registry lookup fails.
///
/// [`EndpointId`]: iroh::EndpointId
fn peer_ring_set<R: Registry>(registry: &R, peer: &str) -> Result<HashSet<String>> {
    let peer_id = parse_peer_id(peer)?;
    let mut set: HashSet<String> = registry
        .list_rings()?
        .into_iter()
        .filter(|r| !r.is_open())
        .filter(|r| {
            registry
                .list_ring_peers(r.as_str())
                .unwrap_or_default()
                .iter()
                .any(|(id, _)| *id == peer_id)
        })
        .map(|r| r.as_str().to_owned())
        .collect();
    // open ring is accessible to every peer
    set.insert(OPEN_RING_NAME.to_owned());
    Ok(set)
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

        let grants =
            GrantStore::open(data_dir.join("grants.redb")).context("opening grants database")?;

        let gate = RingGate::new(
            registry.clone(),
            FsTransfer::new(store.clone(), registry.clone()),
        );
        let catalog = CatalogHandler::new(
            store.clone(),
            registry.clone(),
            grants.clone(),
            endpoint.clone(),
        );

        let router = Router::builder(endpoint.clone())
            .accept(ALPN, gate)
            .accept(CATALOG_ALPN, catalog)
            .spawn();

        endpoint.online().await;
        info!(peer_id = %endpoint.id(), "node online");

        Ok(Node {
            endpoint,
            store,
            registry,
            grants,
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
    /// leaf filename, i.e. `path.file_name()`). Returns the root hash and `BlobFormat::Raw`.
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

    /// List blobs in the local store as `(hash, format, tag_name)` triples.
    ///
    /// When `peer` is supplied only blobs accessible to that peer are returned
    /// (i.e. blobs associated with at least one ring that both includes the peer
    /// and grants [`Permission::Read`]).
    /// When `rings` is supplied only blobs tagged with at least one of those
    /// ring names are returned (OR semantics).  When both are given the
    /// filters are combined: a blob must satisfy both independently.
    ///
    /// # Errors
    ///
    /// Returns an error if the tag store cannot be read, if `peer` is not a
    /// valid [`EndpointId`] encoding, or if a registry lookup fails.
    ///
    /// [`EndpointId`]: iroh::EndpointId
    pub async fn list_blobs(
        &self,
        peer: Option<&str>,
        rings: Option<Vec<String>>,
    ) -> Result<Vec<(Hash, BlobFormat, String)>> {
        let mut stream = self.store.tags().list().await?;
        let mut blobs = Vec::new();
        while let Some(item) = stream.next().await {
            let info = item?;
            let name = String::from_utf8_lossy(&info.name.0).into_owned();
            blobs.push((info.hash, info.format, name));
        }

        let peer_rings: Option<HashSet<String>> =
            peer.map(|s| peer_ring_set(&self.registry, s)).transpose()?;
        let ring_names: Option<HashSet<String>> = rings.map(|rs| rs.into_iter().collect());

        if peer_rings.is_some() || ring_names.is_some() {
            let mut filtered = Vec::with_capacity(blobs.len());
            for (hash, format, name) in blobs {
                let blob_rings = self.registry.list_resource_rings(*hash.as_bytes())?;
                if let Some(ref rset) = ring_names {
                    if !blob_rings.iter().any(|(r, _)| rset.contains(r.as_str())) {
                        continue;
                    }
                }
                if let Some(ref pset) = peer_rings {
                    if !blob_rings.iter().any(|(r, perms)| {
                        pset.contains(r.as_str()) && perms.contains(&Permission::Read)
                    }) {
                        continue;
                    }
                }
                filtered.push((hash, format, name));
            }
            blobs = filtered;
        }

        Ok(blobs)
    }

    /// Remove a blob from the local store by deleting its named tags.
    ///
    /// Ring tags in the registry must be removed separately by the caller
    /// before invoking this method. Disk space is reclaimed on the next GC
    /// cycle (every 30 s while the node is running).
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
        let addr = crate::util::relay_only_addr(self.node_addr());
        ShareTicket::from_format(addr, hash, format, name)
    }

    /// Connect to a remote node and fetch its catalog — the list of blobs the
    /// caller is allowed to download.
    ///
    /// The remote node must have granted [`crate::core::grants::Privilege::BlobList`] to
    /// this node's identity. Only blobs that the calling peer has
    /// [`iroh_rings::Permission::Read`] on (via ring membership) will be included.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails, the remote node denies access,
    /// or the response stream is malformed.
    pub async fn catalog_connect(&self, peer_addr: EndpointAddr) -> Result<Vec<CatalogEntry>> {
        let conn = self
            .endpoint
            .connect(peer_addr, CATALOG_ALPN)
            .await
            .map_err(|e| anyhow::anyhow!("connecting for catalog: {e}"))?;
        let (mut send, mut recv) = conn.open_bi().await.context("opening catalog stream")?;
        send.write_all(&[BLOB_LIST])
            .await
            .context("sending catalog command")?;
        send.finish()?;

        let mut status = [0u8; 1];
        recv.read_exact(&mut status)
            .await
            .context("reading catalog status")?;
        if status[0] != ALLOWED {
            anyhow::bail!("catalog access denied by remote node");
        }
        decode_entries(&mut recv).await
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

        // Hold a temporary tag for the duration of the download so GC doesn't unlink
        // the partial .data file while we're writing it (large files take > 30s).
        let blob_batch = self
            .store
            .blobs()
            .batch()
            .await
            .context("creating download scope")?;
        let _gc_guard = blob_batch
            .temp_tag(HashAndFormat { hash, format })
            .await
            .context("creating temp tag")?;

        let conn = self
            .endpoint
            .connect(ticket.node_addr().clone(), ALPN)
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

#[cfg(test)]
mod tests {
    use iroh::SecretKey;
    use iroh_rings::{InMemoryRegistry, Permission, OPEN_RING_NAME};
    use tempfile::TempDir;

    use super::*;
    use crate::config::Config;
    use crate::core::grants::Privilege;

    async fn start_test_node() -> (Node<InMemoryRegistry>, TempDir) {
        let dir = TempDir::new().unwrap();
        let cfg = Config {
            secret_key: SecretKey::generate(),
            daemon_port: 60001,
        };
        let node = Node::start(dir.path(), cfg, InMemoryRegistry::default())
            .await
            .unwrap();
        (node, dir)
    }

    fn make_registry() -> InMemoryRegistry {
        InMemoryRegistry::default()
    }

    fn make_peer_str() -> (iroh::EndpointId, String) {
        let id = SecretKey::generate().public();
        let s = id.to_string();
        (id, s)
    }

    #[test]
    fn peer_ring_set_includes_explicit_rings_and_open() {
        let reg = make_registry();
        reg.create_ring("friends").unwrap();
        reg.create_ring("work").unwrap();
        let (peer_id, peer_str) = make_peer_str();
        reg.add_peer_to_ring("friends", peer_id, None).unwrap();

        let set = peer_ring_set(&reg, &peer_str).unwrap();
        assert!(set.contains("friends"));
        assert!(set.contains(OPEN_RING_NAME));
        assert!(!set.contains("work"));
    }

    #[test]
    fn peer_ring_set_always_includes_open_even_with_no_memberships() {
        let reg = make_registry();
        let (_, peer_str) = make_peer_str();

        let set = peer_ring_set(&reg, &peer_str).unwrap();
        assert_eq!(set, std::iter::once(OPEN_RING_NAME.to_owned()).collect());
    }

    #[test]
    fn peer_ring_set_rejects_invalid_peer_string() {
        let reg = make_registry();
        assert!(peer_ring_set(&reg, "not-a-valid-peer-id").is_err());
    }

    #[tokio::test]
    async fn catalog_connect_denied_when_no_grant() {
        let (remote, _dir1) = start_test_node().await;
        let (local, _dir2) = start_test_node().await;

        let result = local.catalog_connect(remote.endpoint.addr()).await;
        assert!(result.is_err(), "expected denial error");
    }

    #[tokio::test]
    async fn catalog_connect_returns_entries_with_grant_and_ring_access() {
        let (remote, remote_dir) = start_test_node().await;
        let (local, _dir2) = start_test_node().await;
        let local_id = local.endpoint.id();

        let file_path = remote_dir.path().join("catalog_test.txt");
        tokio::fs::write(&file_path, b"catalog entry content")
            .await
            .unwrap();
        let (hash, _) = remote.import_file(&file_path).await.unwrap();

        remote.registry.create_ring("access").unwrap();
        remote
            .registry
            .add_peer_to_ring("access", local_id, None)
            .unwrap();
        remote
            .registry
            .add_ring_to_resource(*hash.as_bytes(), "access", &[Permission::Read])
            .unwrap();
        remote.grants.grant(Privilege::BlobList, local_id).unwrap();

        let entries = local.catalog_connect(remote.endpoint.addr()).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].hash, hash);
        assert_eq!(entries[0].name, "catalog_test.txt");
    }
}
