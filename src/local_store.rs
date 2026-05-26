//! Unified local storage: [`GrantStore`] and [`PeerStore`] sharing a single
//! `local.redb` file.
//!
//! Open [`LocalStore`] once via [`LocalStore::open`]; the returned value owns
//! both stores and can be destructured into the fields expected by [`Node`].
//!
//! ## Migration
//!
//! Ringdrop < 0.10 kept grants and peers in separate `grants.redb` and
//! `peers.redb` files. [`LocalStore::open`] automatically migrates those files
//! into `local.redb` on the first startup after an upgrade, then deletes them.
//! The migration is a no-op on fresh installs and on subsequent startups.
//!
//! [`Node`]: crate::core::Node

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::info;

use crate::core::grants::GrantStore;
use crate::core::peers::PeerStore;

/// Holds both local ringdrop stores backed by the shared `local.redb` file.
pub(crate) struct LocalStore {
    /// Catalog privilege grants.
    pub grants: GrantStore,
    /// Peer address book.
    pub peers: PeerStore,
}

impl LocalStore {
    /// Open (or create) `local.redb` in `data_dir`, running migration from the
    /// old separate files if needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the migration or database open fails.
    pub(crate) fn open(data_dir: &Path) -> Result<Self> {
        migrate_if_needed(data_dir).context("migrating to local.redb")?;
        let db =
            Arc::new(Database::create(data_dir.join("local.redb")).context("opening local.redb")?);
        Ok(Self {
            grants: GrantStore::from_db(Arc::clone(&db)).context("initialising grant store")?,
            peers: PeerStore::from_db(db).context("initialising peer store")?,
        })
    }
}

// ── Migration ──────────────────────────────────────────────────────────────

/// Schema mirrors — must stay in sync with the definitions in `grants.rs` and
/// `peers.rs`.
const GRANTS: TableDefinition<'_, &[u8], ()> = TableDefinition::new("grants");
const PEERS: TableDefinition<'_, &[u8], &str> = TableDefinition::new("peers");

/// Migrate `grants.redb` and `peers.redb` into `local.redb` when needed.
///
/// Exits immediately when `local.redb` already exists or neither old file is
/// present (fresh install). Otherwise:
///
/// 1. Copies all rows into `local.redb.tmp`.
/// 2. Atomically renames `.tmp` → `local.redb`.
/// 3. Deletes the old files.
///
/// A leftover `.tmp` from a previous failed attempt is cleaned up before
/// starting. If any step fails the old files are left untouched so the next
/// startup can retry.
fn migrate_if_needed(data_dir: &Path) -> Result<()> {
    let local_path = data_dir.join("local.redb");
    let grants_path = data_dir.join("grants.redb");
    let peers_path = data_dir.join("peers.redb");

    if local_path.exists() {
        return Ok(());
    }

    let has_grants = grants_path.exists();
    let has_peers = peers_path.exists();

    if !has_grants && !has_peers {
        return Ok(());
    }

    info!("migrating grants.redb / peers.redb → local.redb");

    let tmp_path = data_dir.join("local.redb.tmp");

    if tmp_path.exists() {
        std::fs::remove_file(&tmp_path).context("removing stale local.redb.tmp")?;
    }

    // Phase 1 — write everything into the tmp file.
    {
        let db = Database::create(&tmp_path).context("creating local.redb.tmp")?;
        let write = db.begin_write().context("initialising local.redb.tmp")?;
        write.open_table(GRANTS).context("creating grants table")?;
        write.open_table(PEERS).context("creating peers table")?;
        write.commit().context("committing local.redb.tmp init")?;

        if has_grants {
            copy_grants(&db, &grants_path)?;
        }
        if has_peers {
            copy_peers(&db, &peers_path)?;
        }
        // `db` is dropped here — file lock released before the rename below.
    }

    // Phase 2 — atomic rename (src and dst are in the same directory).
    std::fs::rename(&tmp_path, &local_path).context("renaming local.redb.tmp → local.redb")?;

    // Phase 3 — delete old files now that local.redb is durable.
    if has_grants {
        std::fs::remove_file(&grants_path).context("deleting grants.redb")?;
        info!("grants.redb deleted");
    }
    if has_peers {
        std::fs::remove_file(&peers_path).context("deleting peers.redb")?;
        info!("peers.redb deleted");
    }

    info!("migration to local.redb complete");
    Ok(())
}

fn copy_grants(dst: &Database, src_path: &Path) -> Result<()> {
    let src = Database::open(src_path).context("opening grants.redb")?;
    let read = src.begin_read().context("reading grants.redb")?;
    let Ok(old) = read.open_table(GRANTS) else {
        return Ok(());
    };
    let write = dst
        .begin_write()
        .context("writing grants to local.redb.tmp")?;
    {
        let mut new = write.open_table(GRANTS).context("opening grants table")?;
        for item in old.iter().context("iterating grants")? {
            let (k, _) = item.context("reading grant row")?;
            new.insert(k.value(), ()).context("inserting grant row")?;
        }
    }
    write.commit().context("committing grants migration")?;
    Ok(())
}

fn copy_peers(dst: &Database, src_path: &Path) -> Result<()> {
    let src = Database::open(src_path).context("opening peers.redb")?;
    let read = src.begin_read().context("reading peers.redb")?;
    let Ok(old) = read.open_table(PEERS) else {
        return Ok(());
    };
    let write = dst
        .begin_write()
        .context("writing peers to local.redb.tmp")?;
    {
        let mut new = write.open_table(PEERS).context("opening peers table")?;
        for item in old.iter().context("iterating peers")? {
            let (k, v) = item.context("reading peer row")?;
            new.insert(k.value(), v.value())
                .context("inserting peer row")?;
        }
    }
    write.commit().context("committing peers migration")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    use tempfile::TempDir;

    fn peer_bytes() -> [u8; 32] {
        *SecretKey::generate().public().as_bytes()
    }

    fn grant_key(privilege: &str, peer: &[u8; 32]) -> Vec<u8> {
        let mut k = Vec::with_capacity(privilege.len() + 1 + 32);
        k.extend_from_slice(privilege.as_bytes());
        k.push(b'\0');
        k.extend_from_slice(peer);
        k
    }

    fn write_old_grants(dir: &Path, peer: &[u8; 32]) {
        let db = Database::create(dir.join("grants.redb")).unwrap();
        let write = db.begin_write().unwrap();
        let mut t = write.open_table(GRANTS).unwrap();
        t.insert(grant_key("blob-list", peer).as_slice(), ())
            .unwrap();
        drop(t);
        write.commit().unwrap();
    }

    fn write_old_peers(dir: &Path, peer: &[u8; 32], nick: &str) {
        let db = Database::create(dir.join("peers.redb")).unwrap();
        let write = db.begin_write().unwrap();
        let mut t = write.open_table(PEERS).unwrap();
        t.insert(peer.as_slice(), nick).unwrap();
        drop(t);
        write.commit().unwrap();
    }

    #[test]
    fn no_op_when_local_already_exists() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("local.redb"), b"sentinel").unwrap();
        migrate_if_needed(dir.path()).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("local.redb")).unwrap(),
            b"sentinel"
        );
    }

    #[test]
    fn no_op_when_no_old_files_exist() {
        let dir = TempDir::new().unwrap();
        migrate_if_needed(dir.path()).unwrap();
        assert!(!dir.path().join("local.redb").exists());
    }

    #[test]
    fn migrates_grants_and_deletes_old_file() {
        let dir = TempDir::new().unwrap();
        let peer = peer_bytes();
        write_old_grants(dir.path(), &peer);

        migrate_if_needed(dir.path()).unwrap();

        assert!(dir.path().join("local.redb").exists());
        assert!(!dir.path().join("grants.redb").exists());

        let db = Database::open(dir.path().join("local.redb")).unwrap();
        let read = db.begin_read().unwrap();
        let t = read.open_table(GRANTS).unwrap();
        assert!(t
            .get(grant_key("blob-list", &peer).as_slice())
            .unwrap()
            .is_some());
    }

    #[test]
    fn migrates_peers_and_deletes_old_file() {
        let dir = TempDir::new().unwrap();
        let peer = peer_bytes();
        write_old_peers(dir.path(), &peer, "alice");

        migrate_if_needed(dir.path()).unwrap();

        assert!(dir.path().join("local.redb").exists());
        assert!(!dir.path().join("peers.redb").exists());

        let db = Database::open(dir.path().join("local.redb")).unwrap();
        let read = db.begin_read().unwrap();
        let t = read.open_table(PEERS).unwrap();
        assert_eq!(t.get(peer.as_slice()).unwrap().unwrap().value(), "alice");
    }

    #[test]
    fn migrates_both_files_together() {
        let dir = TempDir::new().unwrap();
        let peer = peer_bytes();
        write_old_grants(dir.path(), &peer);
        write_old_peers(dir.path(), &peer, "bob");

        migrate_if_needed(dir.path()).unwrap();

        assert!(dir.path().join("local.redb").exists());
        assert!(!dir.path().join("grants.redb").exists());
        assert!(!dir.path().join("peers.redb").exists());

        let db = Database::open(dir.path().join("local.redb")).unwrap();
        let read = db.begin_read().unwrap();
        let gt = read.open_table(GRANTS).unwrap();
        let pt = read.open_table(PEERS).unwrap();
        assert!(gt
            .get(grant_key("blob-list", &peer).as_slice())
            .unwrap()
            .is_some());
        assert_eq!(pt.get(peer.as_slice()).unwrap().unwrap().value(), "bob");
    }

    #[test]
    fn cleans_up_stale_tmp_before_migrating() {
        let dir = TempDir::new().unwrap();
        let peer = peer_bytes();
        write_old_peers(dir.path(), &peer, "carol");
        std::fs::write(dir.path().join("local.redb.tmp"), b"stale").unwrap();

        migrate_if_needed(dir.path()).unwrap();

        assert!(dir.path().join("local.redb").exists());
        assert!(!dir.path().join("local.redb.tmp").exists());
    }
}
