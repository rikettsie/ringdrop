//! Local peer address book: [`EndpointId`] → optional nickname.
//!
//! Nicknames are a ringdrop-level concern — a human-readable label you attach
//! to a peer for your own convenience. The iroh-rings registry is unaware of
//! them: [`add_peer_to_ring`] is always called with `label: None`; display
//! resolution happens entirely through this store.
//!
//! Storage is a dedicated redb database (`peers.redb`) with a single table:
//!
//! ```text
//! PEERS   peer_id_bytes[32] → nickname (UTF-8; empty string = no nickname set)
//! ```
//!
//! [`EndpointId`]: iroh::EndpointId
//! [`add_peer_to_ring`]: iroh_rings::Registry::add_peer_to_ring

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::EndpointId;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

/// Table mapping `peer_id_bytes[32]` to a UTF-8 nickname.
///
/// An empty string value means the peer is known but has no nickname set.
const PEERS: TableDefinition<'_, &[u8], &str> = TableDefinition::new("peers");

/// Coerce a peer's 32-byte fixed-size array into a `&[u8]` slice for redb key ops.
fn peer_key(peer: &EndpointId) -> &[u8] {
    peer.as_bytes()
}

/// Persistent local peer address book, backed by a redb database.
///
/// Maps each known [`EndpointId`] to an optional human-readable nickname.
/// Cheaply cloneable via internal [`Arc`].
///
/// [`EndpointId`]: iroh::EndpointId
#[derive(Clone)]
pub struct PeerStore {
    db: Arc<Database>,
}

impl PeerStore {
    /// Open (or create) the peer store at `path`, backed by a dedicated database.
    ///
    /// Convenience wrapper around [`Self::from_db`] for standalone and test use.
    ///
    /// # Errors
    ///
    /// Returns an error if the database file cannot be opened or the initial
    /// table setup fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Arc::new(Database::create(path).context("opening peers database")?);
        Self::from_db(db)
    }

    /// Attach the peer store to an existing shared database.
    ///
    /// Creates the `PEERS` table if it does not yet exist. Use this when
    /// `PeerStore` and `GrantStore` share the same `local.redb` file.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created or the init transaction
    /// fails.
    pub fn from_db(db: Arc<Database>) -> Result<Self> {
        let write = db
            .begin_write()
            .context("starting peers init transaction")?;
        write.open_table(PEERS).context("creating peers table")?;
        write.commit().context("committing peers init")?;
        Ok(Self { db })
    }

    /// Add `peer` to the store with an optional nickname, or update the
    /// nickname if the peer is already known.
    ///
    /// Passing `nickname: None` clears any existing nickname.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub fn upsert(&self, peer: EndpointId, nickname: Option<&str>) -> Result<()> {
        let write = self.db.begin_write().context("beginning peer upsert")?;
        {
            let mut table = write.open_table(PEERS).context("opening peers table")?;
            table
                .insert(peer_key(&peer), nickname.unwrap_or(""))
                .context("inserting peer")?;
        }
        write.commit().context("committing peer upsert")?;
        Ok(())
    }

    /// Ensure `peer` is in the store.
    ///
    /// If the peer is already present the existing nickname is preserved. If
    /// absent the peer is added with no nickname.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read or write fails.
    pub fn ensure(&self, peer: EndpointId) -> Result<()> {
        {
            let read = self.db.begin_read().context("beginning peer ensure read")?;
            let table = read.open_table(PEERS).context("opening peers table")?;
            if table
                .get(peer_key(&peer))
                .context("querying peer")?
                .is_some()
            {
                return Ok(());
            }
        }
        self.upsert(peer, None)
    }

    /// Set or update the nickname for an already-known peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer is not in the store, or if the database
    /// write fails.
    pub fn set_nickname(&self, peer: EndpointId, nickname: &str) -> Result<()> {
        let write = self
            .db
            .begin_write()
            .context("beginning peer nick update")?;
        {
            let mut table = write.open_table(PEERS).context("opening peers table")?;
            anyhow::ensure!(
                table
                    .get(peer_key(&peer))
                    .context("querying peer")?
                    .is_some(),
                "peer not found: {peer}"
            );
            table
                .insert(peer_key(&peer), nickname)
                .context("updating peer nickname")?;
        }
        write.commit().context("committing peer nick update")?;
        Ok(())
    }

    /// Look up a peer by identity.
    ///
    /// Returns `None` if the peer is not in the store, `Some(None)` if the
    /// peer is known but has no nickname, and `Some(Some(nickname))` if a
    /// nickname is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read fails.
    pub fn get(&self, peer: &EndpointId) -> Result<Option<Option<String>>> {
        let read = self.db.begin_read().context("beginning peer get")?;
        let table = read.open_table(PEERS).context("opening peers table")?;
        match table.get(peer_key(peer)).context("querying peer")? {
            None => Ok(None),
            Some(v) => {
                let nick = v.value();
                Ok(Some(if nick.is_empty() {
                    None
                } else {
                    Some(nick.to_owned())
                }))
            }
        }
    }

    /// Returns all known peers as `(EndpointId, Option<nickname>)` pairs.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read or key decoding fails.
    pub fn list(&self) -> Result<Vec<(EndpointId, Option<String>)>> {
        let read = self.db.begin_read().context("beginning peers list read")?;
        let table = read.open_table(PEERS).context("opening peers table")?;
        let mut out = Vec::new();
        for item in table.iter().context("iterating peers")? {
            let (k, v) = item.context("reading peer entry")?;
            let bytes: [u8; 32] = k.value().try_into().context("peer key not 32 bytes")?;
            let peer = EndpointId::from_bytes(&bytes).context("invalid peer id in store")?;
            let nick = v.value();
            out.push((
                peer,
                if nick.is_empty() {
                    None
                } else {
                    Some(nick.to_owned())
                },
            ));
        }
        Ok(out)
    }

    /// Remove `peer` from the store.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer is not found, or if the database write fails.
    pub fn remove(&self, peer: EndpointId) -> Result<()> {
        let write = self.db.begin_write().context("beginning peer remove")?;
        {
            let mut table = write.open_table(PEERS).context("opening peers table")?;
            let removed = table.remove(peer_key(&peer)).context("removing peer")?;
            anyhow::ensure!(removed.is_some(), "peer not found: {peer}");
        }
        write.commit().context("committing peer remove")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_store() -> (PeerStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = PeerStore::open(dir.path().join("peers.redb")).unwrap();
        (store, dir)
    }

    fn peer() -> EndpointId {
        iroh::SecretKey::generate().public()
    }

    #[test]
    fn upsert_with_nickname_stores_it() {
        let (store, _dir) = open_store();
        let p = peer();
        store.upsert(p, Some("alice")).unwrap();
        assert_eq!(store.get(&p).unwrap(), Some(Some("alice".to_owned())));
    }

    #[test]
    fn upsert_with_none_stores_peer_without_nickname() {
        let (store, _dir) = open_store();
        let p = peer();
        store.upsert(p, None).unwrap();
        assert_eq!(store.get(&p).unwrap(), Some(None));
    }

    #[test]
    fn upsert_on_existing_peer_updates_nickname() {
        let (store, _dir) = open_store();
        let p = peer();
        store.upsert(p, Some("alice")).unwrap();
        store.upsert(p, Some("alice2")).unwrap();
        assert_eq!(store.get(&p).unwrap(), Some(Some("alice2".to_owned())));
    }

    #[test]
    fn upsert_with_none_clears_existing_nickname() {
        let (store, _dir) = open_store();
        let p = peer();
        store.upsert(p, Some("alice")).unwrap();
        store.upsert(p, None).unwrap();
        assert_eq!(store.get(&p).unwrap(), Some(None));
    }

    #[test]
    fn ensure_adds_absent_peer_with_no_nickname() {
        let (store, _dir) = open_store();
        let p = peer();
        store.ensure(p).unwrap();
        assert_eq!(store.get(&p).unwrap(), Some(None));
    }

    #[test]
    fn ensure_does_not_clear_existing_nickname() {
        let (store, _dir) = open_store();
        let p = peer();
        store.upsert(p, Some("alice")).unwrap();
        store.ensure(p).unwrap();
        assert_eq!(store.get(&p).unwrap(), Some(Some("alice".to_owned())));
    }

    #[test]
    fn set_nickname_updates_existing_peer() {
        let (store, _dir) = open_store();
        let p = peer();
        store.upsert(p, None).unwrap();
        store.set_nickname(p, "bob").unwrap();
        assert_eq!(store.get(&p).unwrap(), Some(Some("bob".to_owned())));
    }

    #[test]
    fn set_nickname_on_unknown_peer_errors() {
        let (store, _dir) = open_store();
        assert!(store.set_nickname(peer(), "ghost").is_err());
    }

    #[test]
    fn get_returns_none_for_unknown_peer() {
        let (store, _dir) = open_store();
        assert_eq!(store.get(&peer()).unwrap(), None);
    }

    #[test]
    fn remove_existing_peer_succeeds() {
        let (store, _dir) = open_store();
        let p = peer();
        store.upsert(p, Some("alice")).unwrap();
        store.remove(p).unwrap();
        assert_eq!(store.get(&p).unwrap(), None);
    }

    #[test]
    fn remove_missing_peer_errors() {
        let (store, _dir) = open_store();
        assert!(store.remove(peer()).is_err());
    }

    #[test]
    fn list_returns_all_peers_with_nicknames() {
        let (store, _dir) = open_store();
        let p1 = peer();
        let p2 = peer();
        store.upsert(p1, Some("alice")).unwrap();
        store.upsert(p2, None).unwrap();
        let entries = store.list().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries
            .iter()
            .any(|(p, n)| *p == p1 && n.as_deref() == Some("alice")));
        assert!(entries.iter().any(|(p, n)| *p == p2 && n.is_none()));
    }

    #[test]
    fn list_on_empty_store_returns_empty_vec() {
        let (store, _dir) = open_store();
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    fn peer_store_persists_across_close_and_reopen() {
        let dir = TempDir::new().unwrap();
        let p = peer();
        {
            let store = PeerStore::open(dir.path().join("peers.redb")).unwrap();
            store.upsert(p, Some("alice")).unwrap();
        }
        let store = PeerStore::open(dir.path().join("peers.redb")).unwrap();
        assert_eq!(store.get(&p).unwrap(), Some(Some("alice".to_owned())));
    }
}
