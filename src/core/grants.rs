//! Privilege-based grant store: which peers may invoke which catalog operations.
//!
//! Grants are stored in a dedicated redb database (`grants.redb`) separate
//! from the ring registry. The data model is a single table keyed by a
//! composite of privilege name and peer identity:
//!
//! ```text
//! GRANTS   privilege\0peer_id_bytes[32] → ()
//! ```
//!
//! The NUL separator between the privilege string and the 32-byte peer key
//! is unambiguous because privilege names are validated to contain no NUL.
//!
//! # Current privileges
//!
//! | Privilege | Value | Grants |
//! |-----------|-------|--------|
//! | [`Privilege::BlobList`] | `"blob-list"` | Query the peer's catalog via `/ringdrop/catalog/0` |

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use iroh::EndpointId;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

/// Table mapping `privilege\0peer_id_bytes[32]` to `()`.
const GRANTS: TableDefinition<'_, &[u8], ()> = TableDefinition::new("grants");

/// A named capability that can be granted to a remote peer.
///
/// Validated at write time so unknown privilege strings are rejected before
/// they reach the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Privilege {
    /// Allows the peer to query the local catalog via `/ringdrop/catalog/0`
    /// and receive the list of blobs they can download.
    BlobList,
}

impl Privilege {
    /// Returns the canonical wire/storage string for this privilege.
    pub fn as_str(self) -> &'static str {
        match self {
            Privilege::BlobList => "blob-list",
        }
    }
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for Privilege {
    type Error = anyhow::Error;

    /// Parse a privilege from its canonical string representation.
    ///
    /// # Errors
    ///
    /// Returns an error if the string does not match any known privilege.
    fn try_from(s: &str) -> Result<Self> {
        match s {
            "blob-list" => Ok(Privilege::BlobList),
            _ => anyhow::bail!("unknown privilege: {s:?}"),
        }
    }
}

/// Persistent store for peer privilege grants, backed by a redb database.
///
/// Cheaply cloneable via internal [`Arc`].
#[derive(Clone)]
pub struct GrantStore {
    db: Arc<Database>,
}

impl GrantStore {
    /// Open (or create) the grant store at `path`.
    ///
    /// Creates the `GRANTS` table on first use.
    ///
    /// # Errors
    ///
    /// Returns an error if the database file cannot be opened or the initial
    /// table setup fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path).context("opening grants database")?;
        let write = db
            .begin_write()
            .context("starting grants init transaction")?;
        write.open_table(GRANTS).context("creating grants table")?;
        write.commit().context("committing grants init")?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Grant `privilege` to `peer`.
    ///
    /// Idempotent: granting an already-granted privilege is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub fn grant(&self, privilege: Privilege, peer: EndpointId) -> Result<()> {
        let key = grant_key(privilege, &peer);
        let write = self.db.begin_write().context("beginning grant write")?;
        {
            let mut table = write.open_table(GRANTS).context("opening grants table")?;
            table
                .insert(key.as_slice(), ())
                .context("inserting grant")?;
        }
        write.commit().context("committing grant")?;
        Ok(())
    }

    /// Revoke `privilege` from `peer`.
    ///
    /// Idempotent: revoking a non-existent grant is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub fn revoke(&self, privilege: Privilege, peer: EndpointId) -> Result<()> {
        let key = grant_key(privilege, &peer);
        let write = self.db.begin_write().context("beginning revoke write")?;
        {
            let mut table = write.open_table(GRANTS).context("opening grants table")?;
            table.remove(key.as_slice()).context("removing grant")?;
        }
        write.commit().context("committing revoke")?;
        Ok(())
    }

    /// Returns `true` if `peer` currently holds `privilege`.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read fails.
    pub fn has_grant(&self, privilege: Privilege, peer: &EndpointId) -> Result<bool> {
        let key = grant_key(privilege, peer);
        let read = self.db.begin_read().context("beginning grant read")?;
        let table = read.open_table(GRANTS).context("opening grants table")?;
        Ok(table
            .get(key.as_slice())
            .context("querying grant")?
            .is_some())
    }

    /// Returns all current grants as `(privilege, peer_id)` pairs.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read or key decoding fails.
    pub fn list(&self) -> Result<Vec<(Privilege, EndpointId)>> {
        let read = self.db.begin_read().context("beginning grants list read")?;
        let table = read.open_table(GRANTS).context("opening grants table")?;
        let mut result = Vec::new();
        for item in table.iter().context("iterating grants")? {
            let (k, _) = item.context("reading grant entry")?;
            let (privilege, peer) = decode_grant_key(k.value())?;
            result.push((privilege, peer));
        }
        Ok(result)
    }
}

/// Composite key: `privilege_bytes + NUL + peer_id_bytes[32]`.
fn grant_key(privilege: Privilege, peer: &EndpointId) -> Vec<u8> {
    let priv_bytes = privilege.as_str().as_bytes();
    let mut key = Vec::with_capacity(priv_bytes.len() + 1 + 32);
    key.extend_from_slice(priv_bytes);
    key.push(b'\0');
    key.extend_from_slice(peer.as_bytes());
    key
}

/// Decode a raw key back into `(Privilege, EndpointId)`.
fn decode_grant_key(key: &[u8]) -> Result<(Privilege, EndpointId)> {
    let sep = key
        .iter()
        .position(|&b| b == b'\0')
        .context("grant key missing NUL separator")?;
    let priv_str = std::str::from_utf8(&key[..sep]).context("grant key privilege not UTF-8")?;
    let privilege = Privilege::try_from(priv_str)?;
    let peer_bytes: [u8; 32] = key[sep + 1..]
        .try_into()
        .context("grant key peer id not 32 bytes")?;
    let peer = EndpointId::from_bytes(&peer_bytes).context("grant key invalid peer id")?;
    Ok((privilege, peer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_store() -> (GrantStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = GrantStore::open(dir.path().join("grants.redb")).unwrap();
        (store, dir)
    }

    fn peer() -> EndpointId {
        iroh::SecretKey::generate().public()
    }

    #[test]
    fn grant_and_has_grant_returns_true() {
        let (store, _dir) = open_store();
        let peer = peer();
        store.grant(Privilege::BlobList, peer).unwrap();
        assert!(store.has_grant(Privilege::BlobList, &peer).unwrap());
    }

    #[test]
    fn has_grant_returns_false_for_unknown_peer() {
        let (store, _dir) = open_store();
        assert!(!store.has_grant(Privilege::BlobList, &peer()).unwrap());
    }

    #[test]
    fn revoke_removes_grant() {
        let (store, _dir) = open_store();
        let peer = peer();
        store.grant(Privilege::BlobList, peer).unwrap();
        store.revoke(Privilege::BlobList, peer).unwrap();
        assert!(!store.has_grant(Privilege::BlobList, &peer).unwrap());
    }

    #[test]
    fn revoke_non_existent_grant_is_idempotent() {
        let (store, _dir) = open_store();
        store.revoke(Privilege::BlobList, peer()).unwrap();
    }

    #[test]
    fn grant_is_idempotent() {
        let (store, _dir) = open_store();
        let peer = peer();
        store.grant(Privilege::BlobList, peer).unwrap();
        store.grant(Privilege::BlobList, peer).unwrap();
        assert!(store.has_grant(Privilege::BlobList, &peer).unwrap());
    }

    #[test]
    fn list_returns_all_current_grants() {
        let (store, _dir) = open_store();
        let p1 = peer();
        let p2 = peer();
        store.grant(Privilege::BlobList, p1).unwrap();
        store.grant(Privilege::BlobList, p2).unwrap();
        let grants = store.list().unwrap();
        assert_eq!(grants.len(), 2);
        assert!(grants
            .iter()
            .any(|(priv_, id)| *priv_ == Privilege::BlobList && *id == p1));
        assert!(grants
            .iter()
            .any(|(priv_, id)| *priv_ == Privilege::BlobList && *id == p2));
    }

    #[test]
    fn list_on_empty_store_returns_empty_vec() {
        let (store, _dir) = open_store();
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    fn privilege_display_matches_wire_string() {
        assert_eq!(Privilege::BlobList.to_string(), "blob-list");
    }

    #[test]
    fn privilege_round_trips_through_string() {
        let p = Privilege::BlobList;
        assert_eq!(Privilege::try_from(p.as_str()).unwrap(), p);
    }

    #[test]
    fn unknown_privilege_string_errors() {
        assert!(Privilege::try_from("admin").is_err());
    }

    #[test]
    fn grants_persist_across_close_and_reopen() {
        let dir = tempdir().unwrap();
        let peer = peer();
        {
            let store = GrantStore::open(dir.path().join("grants.redb")).unwrap();
            store.grant(Privilege::BlobList, peer).unwrap();
        }
        let store = GrantStore::open(dir.path().join("grants.redb")).unwrap();
        assert!(store.has_grant(Privilege::BlobList, &peer).unwrap());
    }
}
