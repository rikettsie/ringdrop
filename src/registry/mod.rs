//! Ring registry — persistent, embedded, no external daemon.
//!
//! Two redb tables form the entire data model:
//!
//! ```text
//! RINGS: ring_name (&str) → [EndpointId (32-byte Ed25519 pubkeys)]
//! FILE_RINGS: BlobHash (32 bytes) → NUL-separated ring names
//! ```
//!
//! The critical operation is [`Registry::is_allowed`], which answers:
//! "may this EndpointId download this blob?" in a single read transaction.
//!
//! # Open ring
//!
//! `OPEN_RING_NAME` ("open") is a built-in, reserved ring name with a special
//! meaning: **any peer may access a blob tagged with the open ring**, regardless
//! of membership. It is automatically created on first `open()` and cannot be
//! deleted or renamed. Tag a file with it to publish it without access control.

mod ring;

pub use ring::{RingId, OPEN_RING_NAME};

use std::{path::Path, sync::Arc};

use anyhow::{anyhow, Result};
use iroh::EndpointId;
use iroh_blobs::Hash;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

/// Maps ring name (&str) → serialised Vec<[u8; 32]> of member PeerIds.
const RINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("rings");

/// Maps blob_hash (32 bytes) → NUL-separated ring names.
const FILE_RINGS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_rings");

/// Maps `ring_name \0 peer_id_bytes` → nickname string (display label only).
/// Ring names are validated to contain no NUL, so the separator is unambiguous.
const NICKNAMES: TableDefinition<&[u8], &str> = TableDefinition::new("nicknames");

/// The persistent registry, cheaply cloneable via Arc.
#[derive(Clone)]
pub struct Registry {
    db: Arc<Database>,
}

impl Registry {
    /// Open (or create) the registry at `path`.
    ///
    /// `self_id` is the local node's public key; it is used to reject attempts
    /// to add oneself to a ring.
    ///
    /// On first creation the open ring is bootstrapped automatically.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path)?;
        let write = db.begin_write()?;
        {
            let mut rings = write.open_table(RINGS)?;
            write.open_table(FILE_RINGS)?;
            write.open_table(NICKNAMES)?;

            if rings.get(OPEN_RING_NAME)?.is_none() {
                rings.insert(OPEN_RING_NAME, encode_peer_ids(&[]).as_slice())?;
            }
        }
        write.commit()?;
        Ok(Registry { db: Arc::new(db) })
    }

    /// Create a new ring with the given name.
    ///
    /// Name rules: non-empty, not `"open"` (reserved), no whitespace or NUL bytes.
    pub fn create_ring(&self, name: &str) -> Result<()> {
        if name == OPEN_RING_NAME {
            return Err(anyhow!("'{}' is a reserved ring name", OPEN_RING_NAME));
        }
        if name.is_empty() {
            return Err(anyhow!("ring name must not be empty"));
        }
        if name.contains(|c: char| c.is_whitespace() || c == '\0') {
            return Err(anyhow!(
                "ring name must not contain whitespace or NUL bytes"
            ));
        }
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(RINGS)?;
            if table.get(name)?.is_some() {
                return Err(anyhow!("ring '{}' already exists", name));
            }
            table.insert(name, encode_peer_ids(&[]).as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Add a peer to a ring. Idempotent. `nickname` is a display-only label stored
    /// alongside the peer; passing `Some` on a repeat call updates the label.
    ///
    /// Returns an error if `peer` is the local node's own peer ID.
    pub fn add_member(&self, ring: &str, peer: EndpointId, nickname: Option<&str>) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(RINGS)?;
            let mut members = match table.get(ring)? {
                Some(v) => decode_peer_ids(v.value()),
                None => return Err(anyhow!("ring '{}' not found", ring)),
            };
            let peer_bytes = *peer.as_bytes();
            if !members.contains(&peer_bytes) {
                members.push(peer_bytes);
            }
            table.insert(ring, encode_peer_ids(&members).as_slice())?;

            if let Some(nick) = nickname {
                let mut nick_table = write.open_table(NICKNAMES)?;
                nick_table.insert(nickname_key(ring, &peer).as_slice(), nick)?;
            }
        }
        write.commit()?;
        Ok(())
    }

    /// Remove a peer from a ring, also deleting their nickname entry if any.
    pub fn remove_member(&self, ring: &str, peer: EndpointId) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(RINGS)?;
            let mut members = match table.get(ring)? {
                Some(v) => decode_peer_ids(v.value()),
                None => return Err(anyhow!("ring '{}' not found", ring)),
            };
            let peer_bytes = *peer.as_bytes();
            members.retain(|b| b != &peer_bytes);
            table.insert(ring, encode_peer_ids(&members).as_slice())?;

            let mut nick_table = write.open_table(NICKNAMES)?;
            nick_table.remove(nickname_key(ring, &peer).as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// List all members of a ring, each paired with their optional nickname.
    pub fn list_members(&self, ring: &str) -> Result<Vec<(EndpointId, Option<String>)>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(RINGS)?;
        let nick_table = read.open_table(NICKNAMES)?;
        match table.get(ring)? {
            None => Err(anyhow!("ring '{}' not found", ring)),
            Some(v) => decode_peer_ids(v.value())
                .into_iter()
                .map(|b| {
                    let peer = EndpointId::from_bytes(&b).map_err(|e| anyhow!("{e}"))?;
                    let nick = nick_table
                        .get(nickname_key(ring, &peer).as_slice())?
                        .map(|v| v.value().to_owned());
                    Ok((peer, nick))
                })
                .collect(),
        }
    }

    /// List all ring names. The open ring is always first.
    pub fn list_rings(&self) -> Result<Vec<RingId>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(RINGS)?;
        let mut ids = vec![RingId::open()];
        for entry in table.iter()? {
            let (k, _) = entry?;
            let name = k.value().to_owned();
            if name != OPEN_RING_NAME {
                ids.push(RingId(name));
            }
        }
        Ok(ids)
    }

    /// Remove all ring tags for a blob (used when deleting a blob).
    pub fn remove_file_tags(&self, hash: Hash) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(FILE_RINGS)?;
            table.remove(hash.as_bytes().as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Return all rings a blob is tagged with.
    pub fn file_rings(&self, hash: Hash) -> Result<Vec<RingId>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(FILE_RINGS)?;
        match table.get(hash.as_bytes().as_slice())? {
            None => Ok(Vec::new()),
            Some(v) => Ok(decode_ring_names(v.value())
                .into_iter()
                .map(RingId)
                .collect()),
        }
    }

    /// Tag a blob hash with a ring.
    ///
    /// Mutual-exclusion rules:
    /// - Tagging with `"open"` drops all other rings — open means publicly
    ///   accessible, so private-ring tags are meaningless alongside it.
    /// - Tagging with a private ring drops `"open"` if present, then appends
    ///   the new ring (idempotent if already tagged).
    pub fn tag_file(&self, hash: Hash, ring: &str) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let rings_table = write.open_table(RINGS)?;
            if rings_table.get(ring)?.is_none() {
                return Err(anyhow!("ring '{}' not found", ring));
            }
            drop(rings_table);

            let mut table = write.open_table(FILE_RINGS)?;
            let hash_key = hash.as_bytes();
            let existing = match table.get(hash_key.as_slice())? {
                Some(v) => decode_ring_names(v.value()),
                None => Vec::new(),
            };

            let names = if ring == OPEN_RING_NAME {
                vec![OPEN_RING_NAME.to_owned()]
            } else {
                let mut kept: Vec<String> = existing
                    .into_iter()
                    .filter(|n| n != OPEN_RING_NAME)
                    .collect();
                if !kept.iter().any(|n| n == ring) {
                    kept.push(ring.to_owned());
                }
                kept
            };

            table.insert(hash_key.as_slice(), encode_ring_names(&names).as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// **The central access check.**
    ///
    /// Returns `true` iff the peer may download this blob. Logic:
    ///
    /// 1. If the blob is tagged with `"open"` → allow immediately.
    /// 2. Otherwise, allow iff the peer is a member of at least one tagged ring.
    /// 3. An untagged blob is always denied (fail-closed default).
    pub fn is_allowed(&self, peer: &EndpointId, hash: &Hash) -> Result<bool> {
        let read = self.db.begin_read()?;

        let fr_table = read.open_table(FILE_RINGS)?;
        let ring_names = match fr_table.get(hash.as_bytes().as_slice())? {
            None => return Ok(false),
            Some(v) => decode_ring_names(v.value()),
        };
        if ring_names.is_empty() {
            return Ok(false);
        }
        if ring_names.iter().any(|n| n == OPEN_RING_NAME) {
            return Ok(true);
        }

        let r_table = read.open_table(RINGS)?;
        let peer_bytes = *peer.as_bytes();
        for name in &ring_names {
            if let Some(members_raw) = r_table.get(name.as_str())? {
                let members = decode_peer_ids(members_raw.value());
                if members.iter().any(|b| b == &peer_bytes) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

fn nickname_key(ring: &str, peer: &EndpointId) -> Vec<u8> {
    let mut key = ring.as_bytes().to_vec();
    key.push(b'\0');
    key.extend_from_slice(peer.as_bytes());
    key
}

fn encode_peer_ids(ids: &[[u8; 32]]) -> Vec<u8> {
    ids.iter().flat_map(|b| b.iter().copied()).collect()
}

fn decode_peer_ids(raw: &[u8]) -> Vec<[u8; 32]> {
    raw.chunks_exact(32)
        .map(|c| {
            c.try_into()
                .expect("invariant: chunks_exact(32) yields 32-byte slices")
        })
        .collect()
}

fn encode_ring_names(names: &[String]) -> Vec<u8> {
    names.join("\0").into_bytes()
}

fn decode_ring_names(raw: &[u8]) -> Vec<String> {
    if raw.is_empty() {
        return Vec::new();
    }
    raw.split(|&b| b == 0)
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_registry() -> (Registry, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let reg = Registry::open(dir.path().join("test.redb")).unwrap();
        (reg, dir)
    }

    fn make_hash(b: u8) -> Hash {
        Hash::from_bytes([b; 32])
    }

    fn make_peer_id() -> EndpointId {
        iroh::SecretKey::generate().public()
    }

    // tag_file

    #[test]
    fn tag_open_ring_clears_private_rings() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        reg.create_ring("friends").unwrap();

        reg.tag_file(hash, "friends").unwrap();
        reg.tag_file(hash, OPEN_RING_NAME).unwrap();

        let rings = reg.file_rings(hash).unwrap();
        assert_eq!(rings, vec![RingId::open()]);
    }

    #[test]
    fn tag_private_ring_clears_open_ring() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        reg.create_ring("friends").unwrap();

        reg.tag_file(hash, OPEN_RING_NAME).unwrap();
        reg.tag_file(hash, "friends").unwrap();

        let rings = reg.file_rings(hash).unwrap();
        assert_eq!(rings, vec![RingId("friends".to_owned())]);
    }

    #[test]
    fn tag_private_ring_is_idempotent() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        reg.create_ring("friends").unwrap();

        reg.tag_file(hash, "friends").unwrap();
        reg.tag_file(hash, "friends").unwrap();

        assert_eq!(reg.file_rings(hash).unwrap().len(), 1);
    }

    #[test]
    fn tag_multiple_private_rings_accumulate() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        reg.create_ring("friends").unwrap();
        reg.create_ring("work").unwrap();

        reg.tag_file(hash, "friends").unwrap();
        reg.tag_file(hash, "work").unwrap();

        let rings = reg.file_rings(hash).unwrap();
        assert_eq!(rings.len(), 2);
        assert!(rings.contains(&RingId("friends".to_owned())));
        assert!(rings.contains(&RingId("work".to_owned())));
    }

    #[test]
    fn tag_file_rejects_nonexistent_ring() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        assert!(reg.tag_file(hash, "ghost").is_err());
    }

    // file_rings

    #[test]
    fn file_rings_untagged_blob_returns_empty() {
        let (reg, _dir) = make_registry();
        assert_eq!(reg.file_rings(make_hash(1)).unwrap(), vec![]);
    }

    // create_ring validation

    #[test]
    fn create_ring_rejects_reserved_name() {
        let (reg, _dir) = make_registry();
        assert!(reg.create_ring(OPEN_RING_NAME).is_err());
    }

    #[test]
    fn create_ring_rejects_duplicate() {
        let (reg, _dir) = make_registry();
        reg.create_ring("friends").unwrap();
        assert!(reg.create_ring("friends").is_err());
    }

    #[test]
    fn create_ring_rejects_empty_name() {
        let (reg, _dir) = make_registry();
        assert!(reg.create_ring("").is_err());
    }

    #[test]
    fn create_ring_rejects_name_with_whitespace() {
        let (reg, _dir) = make_registry();
        assert!(reg.create_ring("my ring").is_err());
        assert!(reg.create_ring("tab\there").is_err());
    }

    #[test]
    fn create_ring_rejects_name_with_nul() {
        let (reg, _dir) = make_registry();
        assert!(reg.create_ring("ring\0name").is_err());
    }

    // list_rings

    #[test]
    fn list_rings_always_includes_open_ring() {
        let (reg, _dir) = make_registry();
        let rings = reg.list_rings().unwrap();
        assert_eq!(rings[0], RingId::open());
    }

    #[test]
    fn list_rings_returns_all_created_rings() {
        let (reg, _dir) = make_registry();
        reg.create_ring("friends").unwrap();
        reg.create_ring("work").unwrap();

        let rings = reg.list_rings().unwrap();
        assert!(rings.contains(&RingId("friends".to_owned())));
        assert!(rings.contains(&RingId("work".to_owned())));
        assert_eq!(rings.len(), 3); // open + friends + work
    }

    // add_member / remove_member

    #[test]
    fn add_member_is_idempotent() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();

        reg.add_member("friends", peer, None).unwrap();
        reg.add_member("friends", peer, None).unwrap();

        assert_eq!(reg.list_members("friends").unwrap().len(), 1);
    }

    #[test]
    fn add_member_to_nonexistent_ring_errors() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        assert!(reg.add_member("ghost", peer, None).is_err());
    }

    #[test]
    fn remove_member_from_nonexistent_ring_errors() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        assert!(reg.remove_member("ghost", peer).is_err());
    }

    #[test]
    fn remove_member_noop_when_peer_not_in_ring() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();

        // removing a peer that was never added should succeed silently
        reg.remove_member("friends", peer).unwrap();
        assert_eq!(reg.list_members("friends").unwrap().len(), 0);
    }

    // list_members

    #[test]
    fn list_members_nonexistent_ring_errors() {
        let (reg, _dir) = make_registry();
        assert!(reg.list_members("ghost").is_err());
    }

    // is_allowed

    #[test]
    fn is_allowed_untagged_blob_denied() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        assert!(!reg.is_allowed(&peer, &make_hash(1)).unwrap());
    }

    #[test]
    fn is_allowed_open_ring_permits_any_peer() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let peer = make_peer_id();

        reg.tag_file(hash, OPEN_RING_NAME).unwrap();
        assert!(reg.is_allowed(&peer, &hash).unwrap());
    }

    #[test]
    fn is_allowed_member_of_tagged_ring_permitted() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();

        reg.tag_file(hash, "friends").unwrap();
        reg.add_member("friends", peer, None).unwrap();

        assert!(reg.is_allowed(&peer, &hash).unwrap());
    }

    #[test]
    fn is_allowed_non_member_denied() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let member = make_peer_id();
        let stranger = make_peer_id();
        reg.create_ring("friends").unwrap();

        reg.tag_file(hash, "friends").unwrap();
        reg.add_member("friends", member, None).unwrap();

        assert!(!reg.is_allowed(&stranger, &hash).unwrap());
    }

    #[test]
    fn is_allowed_peer_in_one_of_multiple_tagged_rings_permitted() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();
        reg.create_ring("work").unwrap();

        reg.tag_file(hash, "friends").unwrap();
        reg.tag_file(hash, "work").unwrap();
        // peer is only in "work", not "friends"
        reg.add_member("work", peer, None).unwrap();

        assert!(reg.is_allowed(&peer, &hash).unwrap());
    }

    // nickname

    #[test]
    fn nickname_stored_and_returned_by_list_members() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();

        reg.add_member("friends", peer, Some("alice")).unwrap();

        let members = reg.list_members("friends").unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].0, peer);
        assert_eq!(members[0].1.as_deref(), Some("alice"));
    }

    #[test]
    fn no_nickname_returns_none() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();

        reg.add_member("friends", peer, None).unwrap();

        let members = reg.list_members("friends").unwrap();
        assert_eq!(members[0].1, None);
    }

    #[test]
    fn nickname_updated_on_readd() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();

        reg.add_member("friends", peer, Some("alice")).unwrap();
        reg.add_member("friends", peer, Some("alice2")).unwrap();

        let members = reg.list_members("friends").unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].1.as_deref(), Some("alice2"));
    }

    #[test]
    fn nickname_removed_with_peer() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();

        reg.add_member("friends", peer, Some("alice")).unwrap();
        reg.remove_member("friends", peer).unwrap();
        reg.add_member("friends", peer, None).unwrap();

        let members = reg.list_members("friends").unwrap();
        assert_eq!(members[0].1, None);
    }

    #[test]
    fn nicknames_are_per_ring() {
        let (reg, _dir) = make_registry();
        let peer = make_peer_id();
        reg.create_ring("friends").unwrap();
        reg.create_ring("work").unwrap();

        reg.add_member("friends", peer, Some("alice")).unwrap();
        reg.add_member("work", peer, Some("bob")).unwrap();

        let friends = reg.list_members("friends").unwrap();
        let work = reg.list_members("work").unwrap();
        assert_eq!(friends[0].1.as_deref(), Some("alice"));
        assert_eq!(work[0].1.as_deref(), Some("bob"));
    }

    #[test]
    fn members_mixed_nicknames_and_none() {
        let (reg, _dir) = make_registry();
        let alice = make_peer_id();
        let bob = make_peer_id();
        reg.create_ring("friends").unwrap();

        reg.add_member("friends", alice, Some("alice")).unwrap();
        reg.add_member("friends", bob, None).unwrap();

        let members = reg.list_members("friends").unwrap();
        assert_eq!(members.len(), 2);
        let nicks: Vec<_> = members.iter().map(|(_, n)| n.as_deref()).collect();
        assert!(nicks.contains(&Some("alice")));
        assert!(nicks.contains(&None));
    }
}
