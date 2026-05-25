//! Catalog protocol handler and client decoder: lets a privileged peer apply
//! commands on a remote node.
//!
//! # Protocol (`/ringdrop/catalog/0`)
//!
//! ```text
//! Caller      → Remote node:   [1B: command]
//!
//! Remote node (denied):        [0x00]
//! Remote node (allowed):       [0x01]
//!                              response payload (command-specific, streamed until EOF)
//! ```
//!
//! ## Command: [`BLOB_LIST`] (`0x01`)
//!
//! Lists blobs the caller may download.  The remote node streams entries until EOF:
//!
//! ```text
//!   {
//!     [32B: hash]
//!     [1B:  format  0=Raw  1=HashSeq]
//!     [u16-le: name_len][name utf-8]
//!     [u16-le: ticket_len][ticket uri utf-8]
//!   }
//! ```
//!
//! [`BLOB_LIST`] maps to the [`crate::core::grants::Privilege::BlobList`] grant.
//! Future command bytes will map to future privileges.

use std::fmt;

use anyhow::{Context, Result};
use futures_lite::StreamExt;
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
    Endpoint, EndpointId,
};
use iroh_blobs::{store::fs::FsStore, BlobFormat, Hash};
use iroh_rings::{Permission, Registry};

use crate::core::grants::{GrantStore, Privilege};
use crate::core::ShareTicket;

/// ALPN for the catalog protocol.
pub(crate) const CATALOG_ALPN: &[u8] = b"/ringdrop/catalog/0";

/// Command byte: request the full list of blobs the caller may download.
///
/// Maps to [`crate::core::grants::Privilege::BlobList`].
pub(crate) const BLOB_LIST: u8 = 0x01;

/// Wire byte the remote node sends when the request is denied.
pub(crate) const DENIED: u8 = 0x00;

/// Wire byte the remote node sends when the request is granted, followed by the
/// command-specific response payload.
pub(crate) const ALLOWED: u8 = 0x01;

/// A single entry returned by a [`BLOB_LIST`] command.
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    /// BLAKE3 root hash of the blob or collection.
    pub hash: Hash,
    /// Whether the blob is a raw file or a [`BlobFormat::HashSeq`] collection.
    pub format: BlobFormat,
    /// Human-readable display name (typically the original filename).
    pub name: String,
    /// Transfer ticket that can be passed to the local node to download this blob.
    pub ticket: ShareTicket,
}

/// iroh [`ProtocolHandler`] for the catalog protocol.
///
/// Accepts connections on [`CATALOG_ALPN`], reads a one-byte command, verifies
/// that the connecting peer holds the required grant, and either responds with
/// [`DENIED`] or executes the command and streams the response.
#[derive(Clone)]
pub(crate) struct CatalogHandler<R> {
    store: FsStore,
    registry: R,
    grants: GrantStore,
    endpoint: Endpoint,
}

impl<R> fmt::Debug for CatalogHandler<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogHandler").finish_non_exhaustive()
    }
}

impl<R: Registry + Clone + Send + Sync + 'static> CatalogHandler<R> {
    /// Create a new handler backed by the given store, registry, grant store, and endpoint.
    pub(crate) fn new(store: FsStore, registry: R, grants: GrantStore, endpoint: Endpoint) -> Self {
        CatalogHandler {
            store,
            registry,
            grants,
            endpoint,
        }
    }

    async fn handle(&self, conn: Connection) -> Result<()> {
        let peer = conn.remote_id();
        let (mut send, mut recv) = conn.accept_bi().await.context("accepting bi stream")?;

        let mut cmd = [0u8; 1];
        recv.read_exact(&mut cmd)
            .await
            .context("reading command byte")?;

        match cmd[0] {
            BLOB_LIST => self.handle_blob_list(peer, &mut send).await?,
            _ => {
                send.write_all(&[DENIED]).await?;
                send.finish()?;
            }
        }

        // The catalog protocol uses one stream per connection.  Hold `conn`
        // alive until the peer closes so that in-flight stream data is fully
        // delivered before the QUIC connection tears down.
        while conn.accept_bi().await.is_ok() {}

        Ok(())
    }

    async fn handle_blob_list(
        &self,
        peer: EndpointId,
        send: &mut iroh::endpoint::SendStream,
    ) -> Result<()> {
        if !self.grants.has_grant(Privilege::BlobList, &peer)? {
            send.write_all(&[DENIED]).await?;
            send.finish()?;
            return Ok(());
        }

        send.write_all(&[ALLOWED]).await?;

        let addr = crate::util::relay_only_addr(self.endpoint.addr());
        let mut stream = self.store.tags().list().await?;
        while let Some(item) = stream.next().await {
            let info = item?;
            if !self
                .registry
                .has_permission(&peer, info.hash.as_bytes(), Permission::Read)
                .unwrap_or(false)
            {
                continue;
            }
            let name = String::from_utf8_lossy(&info.name.0).into_owned();
            let ticket =
                ShareTicket::from_format(addr.clone(), info.hash, info.format, Some(name.clone()));
            let ticket_uri = ticket.to_uri()?;
            write_entry(send, info.hash, info.format, &name, &ticket_uri).await?;
        }

        send.finish()?;
        Ok(())
    }
}

impl<R: Registry + Clone + Send + Sync + 'static> ProtocolHandler for CatalogHandler<R> {
    fn accept(
        &self,
        conn: Connection,
    ) -> impl std::future::Future<Output = Result<(), AcceptError>> + Send {
        let handler = self.clone();
        async move {
            handler
                .handle(conn)
                .await
                .map_err(|e| AcceptError::from_boxed(e.into()))
        }
    }
}

/// Decode [`BLOB_LIST`] entries streamed by the server after the ALLOWED byte.
///
/// Reads until the remote sender closes the stream, returning all decoded entries.
///
/// # Errors
///
/// Returns an error if any entry is malformed or a ticket URI cannot be parsed.
pub(crate) async fn decode_entries(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<Vec<CatalogEntry>> {
    let mut entries = Vec::new();
    loop {
        // Probe for the first byte of the next entry.  None = server closed cleanly.
        let mut probe = [0u8; 1];
        match recv.read(&mut probe).await.context("reading entry probe")? {
            None => break,
            Some(1) => {}
            Some(_) => unreachable!("single-byte probe buffer"),
        }

        let mut hash_buf = [0u8; 32];
        hash_buf[0] = probe[0];
        recv.read_exact(&mut hash_buf[1..])
            .await
            .context("reading hash")?;
        let hash = Hash::from_bytes(hash_buf);

        let mut fmt_buf = [0u8; 1];
        recv.read_exact(&mut fmt_buf)
            .await
            .context("reading format byte")?;
        let format = match fmt_buf[0] {
            0 => BlobFormat::Raw,
            1 => BlobFormat::HashSeq,
            b => anyhow::bail!("unknown format byte {b}"),
        };

        let name_bytes = read_length_prefixed(recv, "name").await?;
        let name = String::from_utf8(name_bytes).context("entry name is not UTF-8")?;

        let ticket_bytes = read_length_prefixed(recv, "ticket").await?;
        let ticket_uri = String::from_utf8(ticket_bytes).context("entry ticket is not UTF-8")?;
        let ticket = ShareTicket::from_uri(&ticket_uri)?;

        entries.push(CatalogEntry {
            hash,
            format,
            name,
            ticket,
        });
    }
    Ok(entries)
}

async fn write_entry(
    send: &mut iroh::endpoint::SendStream,
    hash: Hash,
    format: BlobFormat,
    name: &str,
    ticket_uri: &str,
) -> Result<()> {
    send.write_all(hash.as_bytes())
        .await
        .context("writing hash")?;
    let fmt_byte: u8 = match format {
        BlobFormat::HashSeq => 1,
        _ => 0,
    };
    send.write_all(&[fmt_byte])
        .await
        .context("writing format")?;
    write_length_prefixed(send, name.as_bytes())
        .await
        .context("writing name")?;
    write_length_prefixed(send, ticket_uri.as_bytes())
        .await
        .context("writing ticket")?;
    Ok(())
}

async fn write_length_prefixed(send: &mut iroh::endpoint::SendStream, data: &[u8]) -> Result<()> {
    let len = u16::try_from(data.len()).context("field too long for u16 length prefix")?;
    send.write_all(&len.to_le_bytes()).await?;
    send.write_all(data).await?;
    Ok(())
}

async fn read_length_prefixed(
    recv: &mut iroh::endpoint::RecvStream,
    field: &str,
) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 2];
    recv.read_exact(&mut len_buf)
        .await
        .with_context(|| format!("reading {field} length"))?;
    let len = u16::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .with_context(|| format!("reading {field} data"))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{endpoint::presets, protocol::Router};
    use iroh_blobs::{
        api::blobs::{AddPathOptions, ImportMode},
        HashAndFormat,
    };
    use iroh_rings::InMemoryRegistry;
    use tempfile::TempDir;

    struct TestServer {
        endpoint: Endpoint,
        store: FsStore,
        registry: InMemoryRegistry,
        grants: GrantStore,
        _router: Router,
        _dir: TempDir,
    }

    impl TestServer {
        async fn start() -> Self {
            let dir = TempDir::new().unwrap();
            let store = FsStore::load_with_opts(
                dir.path().join("blobs.db"),
                iroh_blobs::store::fs::options::Options::new(&dir.path().join("blobs")),
            )
            .await
            .unwrap();
            let registry = InMemoryRegistry::default();
            let grants = GrantStore::open(dir.path().join("grants.redb")).unwrap();
            let endpoint = Endpoint::builder(presets::N0).bind().await.unwrap();
            let handler = CatalogHandler::new(
                store.clone(),
                registry.clone(),
                grants.clone(),
                endpoint.clone(),
            );
            let router = Router::builder(endpoint.clone())
                .accept(CATALOG_ALPN, handler)
                .spawn();
            endpoint.online().await;
            TestServer {
                endpoint,
                store,
                registry,
                grants,
                _router: router,
                _dir: dir,
            }
        }

        async fn import_blob(&self, content: &[u8], name: &str) -> Hash {
            let path = self._dir.path().join(name);
            tokio::fs::write(&path, content).await.unwrap();
            let tag = self
                .store
                .blobs()
                .add_path_with_opts(AddPathOptions {
                    path: std::path::absolute(&path).unwrap(),
                    mode: ImportMode::Copy,
                    format: BlobFormat::Raw,
                })
                .temp_tag()
                .await
                .unwrap();
            let hash = tag.hash();
            self.store
                .tags()
                .set(
                    name.to_string(),
                    HashAndFormat {
                        hash,
                        format: BlobFormat::Raw,
                    },
                )
                .await
                .unwrap();
            hash
        }
    }

    async fn client_endpoint() -> Endpoint {
        let ep = Endpoint::builder(presets::N0).bind().await.unwrap();
        ep.online().await;
        ep
    }

    async fn send_blob_list(client: &Endpoint, server: &TestServer) -> (u8, Vec<CatalogEntry>) {
        let conn = client
            .connect(server.endpoint.addr(), CATALOG_ALPN)
            .await
            .unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(&[BLOB_LIST]).await.unwrap();
        send.finish().unwrap();

        let mut status = [0u8; 1];
        recv.read_exact(&mut status).await.unwrap();
        let entries = if status[0] == ALLOWED {
            decode_entries(&mut recv).await.unwrap()
        } else {
            vec![]
        };
        (status[0], entries)
    }

    #[tokio::test]
    async fn non_granted_peer_is_denied() {
        let server = TestServer::start().await;
        let client = client_endpoint().await;

        let (status, entries) = send_blob_list(&client, &server).await;

        assert_eq!(status, DENIED);
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn granted_peer_receives_only_readable_blobs() {
        let server = TestServer::start().await;
        let client = client_endpoint().await;
        let client_id = client.id();

        let hash_a = server.import_blob(b"blob a content", "a.txt").await;
        let _hash_b = server.import_blob(b"blob b content", "b.txt").await;

        server.registry.create_ring("test-ring").unwrap();
        server
            .registry
            .add_peer_to_ring("test-ring", client_id, None)
            .unwrap();
        server
            .registry
            .add_ring_to_resource(*hash_a.as_bytes(), "test-ring", &[Permission::Read])
            .unwrap();

        server.grants.grant(Privilege::BlobList, client_id).unwrap();

        let (status, entries) = send_blob_list(&client, &server).await;

        assert_eq!(status, ALLOWED);
        assert_eq!(entries.len(), 1, "only a.txt should be visible");
        assert_eq!(entries[0].hash, hash_a);
    }

    #[tokio::test]
    async fn unknown_command_byte_receives_denied_status() {
        let server = TestServer::start().await;
        let client = client_endpoint().await;
        server
            .grants
            .grant(Privilege::BlobList, client.id())
            .unwrap();

        let conn = client
            .connect(server.endpoint.addr(), CATALOG_ALPN)
            .await
            .unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(&[0xFF]).await.unwrap();
        send.finish().unwrap();

        let mut status = [0u8; 1];
        recv.read_exact(&mut status).await.unwrap();
        assert_eq!(status[0], DENIED, "unknown command should be denied");
    }

    #[tokio::test]
    async fn entry_contains_hash_name_and_valid_ticket() {
        let server = TestServer::start().await;
        let client = client_endpoint().await;
        let client_id = client.id();

        let hash = server.import_blob(b"catalog test", "test.txt").await;

        server.registry.create_ring("r").unwrap();
        server
            .registry
            .add_peer_to_ring("r", client_id, None)
            .unwrap();
        server
            .registry
            .add_ring_to_resource(*hash.as_bytes(), "r", &[Permission::Read])
            .unwrap();
        server.grants.grant(Privilege::BlobList, client_id).unwrap();

        let (status, entries) = send_blob_list(&client, &server).await;

        assert_eq!(status, ALLOWED);
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.hash, hash);
        assert_eq!(entry.name, "test.txt");
        assert_eq!(entry.ticket.hash(), hash);
        assert_eq!(entry.ticket.format(), BlobFormat::Raw);
        assert_eq!(entry.ticket.peer_id(), server.endpoint.id());
    }
}
