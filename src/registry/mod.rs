//! Ring registry — persistent, embedded, no external daemon.
//!
//! Two redb tables form the entire data model:
//!
//! ```
//! RINGS: RingId (UUID bytes) → [EndpointId (32-byte Ed25519 pubkeys)]
//! FILE_RINGS: BlobHash (32 bytes) → [RingId (16-byte UUIDs)]
//! ```
//!
//! The critical operation is [`Registry::is_allowed`], which answers:
//! "may this EndpointId download this blob?" in a single read transaction,
//! with no allocations beyond the stack if the answer is "no".
//!
//! # Open ring
//!
//! `OPEN_RING_ID` is a built-in, fixed ring with a special meaning:
//! **any peer may access a blob tagged with the open ring**, regardless of
//! membership. It is automatically created on first `open()` and cannot be
//! deleted. Tag a file with it to publish it without access control.

mod ring;

pub use ring::{RingId, OPEN_RING_ID, OPEN_RING_NAME};

use std::{path::Path, sync::Arc};

use anyhow::{anyhow, Result};
use iroh::EndpointId;
use iroh_blobs::Hash;
use redb::{Database, ReadableTable, TableDefinition};

/// Maps ring_id (16 bytes) → serialised Vec<[u8; 32]> of member PeerIds.
const RINGS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rings");

/// Maps blob_hash (32 bytes) → serialised Vec<[u8; 16]> of ring UUIDs.
const FILE_RINGS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_rings");

/// The persistent registry, cheaply cloneable via Arc.
#[derive(Clone)]
pub struct Registry(Arc<Database>);

impl Registry {
    /// Open (or create) the registry at `path`.
    ///
    /// On first creation, the open ring is bootstrapped automatically.
    /// On subsequent opens, it is guaranteed to already exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path)?;
        // Ensure both tables exist and the open ring is present.
        let write = db.begin_write()?;
        {
            let mut rings = write.open_table(RINGS)?;
            write.open_table(FILE_RINGS)?;

            // Bootstrap the open ring if it doesn't exist yet.
            // It has an empty member list — membership is never checked for it.
            let open_key = OPEN_RING_ID.as_bytes().as_slice();
            if rings.get(open_key)?.is_none() {
                rings.insert(open_key, encode_peer_ids(&[]).as_slice())?;
            }
        }
        write.commit()?;
        Ok(Registry(Arc::new(db)))
    }

    /// Create a new ring and return its id.
    /// The open ring (`OPEN_RING_ID`) is always present; this creates
    /// an additional named ring.
    pub fn create_ring(&self) -> Result<RingId> {
        let id = RingId::new();
        if id.is_open() {
            return Err(anyhow!("UUID::new_v4 returned nil UUID — this should never happen"));
        }
        let write = self.0.begin_write()?;
        {
            let mut table = write.open_table(RINGS)?;
            table.insert(id.as_bytes().as_slice(), encode_peer_ids(&[]).as_slice())?;
        }
        write.commit()?;
        Ok(id)
    }

    /// Add a peer to a ring. Idempotent.
    pub fn add_member(&self, ring: RingId, peer: EndpointId) -> Result<()> {
        let write = self.0.begin_write()?;
        {
            let mut table = write.open_table(RINGS)?;
            let mut members = match table.get(ring.as_bytes().as_slice())? {
                Some(v) => decode_peer_ids(v.value()),
                None => return Err(anyhow!("ring {} not found", ring)),
            };
            let peer_bytes = *peer.as_bytes();
            if !members.contains(&peer_bytes) {
                members.push(peer_bytes);
            }
            table.insert(
                ring.as_bytes().as_slice(),
                encode_peer_ids(&members).as_slice(),
            )?;
        }
        write.commit()?;
        Ok(())
    }

    /// Remove a peer from a ring.
    pub fn remove_member(&self, ring: RingId, peer: EndpointId) -> Result<()> {
        let write = self.0.begin_write()?;
        {
            let mut table = write.open_table(RINGS)?;
            let mut members = match table.get(ring.as_bytes().as_slice())? {
                Some(v) => decode_peer_ids(v.value()),
                None => return Err(anyhow!("ring {} not found", ring)),
            };
            let peer_bytes = *peer.as_bytes();
            members.retain(|b| b != &peer_bytes);
            table.insert(
                ring.as_bytes().as_slice(),
                encode_peer_ids(&members).as_slice(),
            )?;
        }
        write.commit()?;
        Ok(())
    }

    /// List all members of a ring.
    pub fn list_members(&self, ring: RingId) -> Result<Vec<EndpointId>> {
        let read = self.0.begin_read()?;
        let table = read.open_table(RINGS)?;
        match table.get(ring.as_bytes().as_slice())? {
            None => Err(anyhow!("ring {} not found", ring)),
            Some(v) => {
                let raw = decode_peer_ids(v.value());
                raw.into_iter()
                    .map(|b| EndpointId::from_bytes(&b).map_err(|e| anyhow!("{e}")))
                    .collect()
            }
        }
    }

    /// List all ring IDs. The open ring is always first.
    pub fn list_rings(&self) -> Result<Vec<RingId>> {
        let read = self.0.begin_read()?;
        let table = read.open_table(RINGS)?;
        let mut ids = vec![OPEN_RING_ID];
        for entry in table.iter()? {
            let (k, _) = entry?;
            let bytes: [u8; 16] = k.value().try_into()
                .map_err(|_| anyhow!("corrupt ring key"))?;
            let rid = RingId::from_bytes(bytes);
            if !rid.is_open() {
                ids.push(rid);
            }
        }
        Ok(ids)
    }

    /// Tag a blob hash with a ring.
    ///
    /// Mutual-exclusion rules (enforced here, not just at the CLI):
    /// - Tagging with the open-ring drops all other rings — open means
    ///   publicly accessible, so private-ring tags are meaningless.
    /// - Tagging with a private ring drops the open-ring if present, then
    ///   appends the new ring (idempotent if already tagged).
    pub fn tag_file(&self, hash: Hash, ring: RingId) -> Result<()> {
        let write = self.0.begin_write()?;
        {
            let mut table = write.open_table(FILE_RINGS)?;
            let hash_key = hash.as_bytes();
            let existing: Vec<[u8; 16]> = match table.get(hash_key.as_slice())? {
                Some(v) => decode_ring_ids(v.value()),
                None => Vec::new(),
            };

            let rings = if ring.is_open() {
                vec![*ring.as_bytes()]
            } else {
                let open_bytes = *OPEN_RING_ID.as_bytes();
                let mut kept: Vec<[u8; 16]> = existing
                    .into_iter()
                    .filter(|b| b != &open_bytes)
                    .collect();
                let rid_bytes = *ring.as_bytes();
                if !kept.contains(&rid_bytes) {
                    kept.push(rid_bytes);
                }
                kept
            };

            table.insert(hash_key.as_slice(), encode_ring_ids(&rings).as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Remove a ring tag from a blob.
    pub fn untag_file(&self, hash: Hash, ring: RingId) -> Result<()> {
        let write = self.0.begin_write()?;
        {
            let mut table = write.open_table(FILE_RINGS)?;
            let hash_key = hash.as_bytes();
            let mut rings = match table.get(hash_key.as_slice())? {
                Some(v) => decode_ring_ids(v.value()),
                None => return Ok(()),
            };
            let rid_bytes = *ring.as_bytes();
            rings.retain(|b| b != &rid_bytes);
            table.insert(
                hash_key.as_slice(),
                encode_ring_ids(&rings).as_slice(),
            )?;
        }
        write.commit()?;
        Ok(())
    }

    /// List all rings a blob is tagged with.
    pub fn file_rings(&self, hash: Hash) -> Result<Vec<RingId>> {
        let read = self.0.begin_read()?;
        let table = read.open_table(FILE_RINGS)?;
        match table.get(hash.as_bytes().as_slice())? {
            None => Ok(Vec::new()),
            Some(v) => {
                let raw = decode_ring_ids(v.value());
                Ok(raw.into_iter().map(RingId::from_bytes).collect())
            }
        }
    }

    /// **The central access check.**
    ///
    /// Returns `true` iff the peer may download this blob. Logic:
    ///
    /// 1. If the blob is tagged with `OPEN_RING_ID` → allow immediately.
    ///    No membership check. Any peer can access open-ring blobs.
    /// 2. Otherwise, allow iff the peer is a member of at least one ring
    ///    that the blob has been tagged with.
    /// 3. An untagged blob is always denied (fail-closed default).
    ///
    /// Runs in a single read transaction; no heap allocation on the deny path.
    ///
    /// # Security properties
    /// - `peer` is the iroh-authenticated `EndpointId` from the QUIC handshake —
    ///   it cannot be spoofed.
    /// - An untagged blob is always denied (fail-closed).
    /// - A blob tagged with multiple rings is accessible to members of any
    ///   of those rings (union semantics).
    /// - The open ring bypasses membership — it is intentionally public.
    pub fn is_allowed(&self, peer: &EndpointId, hash: &Hash) -> Result<bool> {
        let read = self.0.begin_read()?;

        let fr_table = read.open_table(FILE_RINGS)?;
        let ring_ids = match fr_table.get(hash.as_bytes().as_slice())? {
            None => return Ok(false),
            Some(v) => decode_ring_ids(v.value()),
        };
        if ring_ids.is_empty() {
            return Ok(false);
        }

        let open_bytes = *OPEN_RING_ID.as_bytes();
        if ring_ids.iter().any(|b| b == &open_bytes) {
            return Ok(true);
        }

        let r_table = read.open_table(RINGS)?;
        let peer_bytes = *peer.as_bytes();
        for rid_bytes in &ring_ids {
            if let Some(members_raw) = r_table.get(rid_bytes.as_slice())? {
                let members = decode_peer_ids(members_raw.value());
                if members.iter().any(|b| b == &peer_bytes) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

// EndpointId = 32 bytes, RingId = 16 bytes. Concatenated raw — no framing
// overhead, trivially indexable (multiply index by width).

fn encode_peer_ids(ids: &[[u8; 32]]) -> Vec<u8> {
    ids.iter().flat_map(|b| b.iter().copied()).collect()
}

fn decode_peer_ids(raw: &[u8]) -> Vec<[u8; 32]> {
    raw.chunks_exact(32)
        .map(|c| c.try_into().expect("invariant: chunks_exact(32) yields 32-byte slices"))
        .collect()
}

fn encode_ring_ids(ids: &[[u8; 16]]) -> Vec<u8> {
    ids.iter().flat_map(|b| b.iter().copied()).collect()
}

fn decode_ring_ids(raw: &[u8]) -> Vec<[u8; 16]> {
    raw.chunks_exact(16)
        .map(|c| c.try_into().expect("invariant: chunks_exact(16) yields 16-byte slices"))
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
        let rid = reg.create_ring().unwrap();

        reg.tag_file(hash, rid).unwrap();
        reg.tag_file(hash, OPEN_RING_ID).unwrap();

        let rings = reg.file_rings(hash).unwrap();
        assert_eq!(rings, vec![OPEN_RING_ID]);
    }

    #[test]
    fn tag_private_ring_clears_open_ring() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let rid = reg.create_ring().unwrap();

        reg.tag_file(hash, OPEN_RING_ID).unwrap();
        reg.tag_file(hash, rid).unwrap();

        let rings = reg.file_rings(hash).unwrap();
        assert_eq!(rings, vec![rid]);
    }

    #[test]
    fn tag_private_ring_is_idempotent() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let rid = reg.create_ring().unwrap();

        reg.tag_file(hash, rid).unwrap();
        reg.tag_file(hash, rid).unwrap();

        assert_eq!(reg.file_rings(hash).unwrap().len(), 1);
    }

    #[test]
    fn tag_multiple_private_rings_accumulate() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let rid1 = reg.create_ring().unwrap();
        let rid2 = reg.create_ring().unwrap();

        reg.tag_file(hash, rid1).unwrap();
        reg.tag_file(hash, rid2).unwrap();

        let rings = reg.file_rings(hash).unwrap();
        assert_eq!(rings.len(), 2);
        assert!(rings.contains(&rid1));
        assert!(rings.contains(&rid2));
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

        reg.tag_file(hash, OPEN_RING_ID).unwrap();
        assert!(reg.is_allowed(&peer, &hash).unwrap());
    }

    #[test]
    fn is_allowed_member_of_tagged_ring_permitted() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let peer = make_peer_id();
        let rid = reg.create_ring().unwrap();

        reg.tag_file(hash, rid).unwrap();
        reg.add_member(rid, peer).unwrap();

        assert!(reg.is_allowed(&peer, &hash).unwrap());
    }

    #[test]
    fn is_allowed_non_member_denied() {
        let (reg, _dir) = make_registry();
        let hash = make_hash(1);
        let member = make_peer_id();
        let stranger = make_peer_id();
        let rid = reg.create_ring().unwrap();

        reg.tag_file(hash, rid).unwrap();
        reg.add_member(rid, member).unwrap();

        assert!(!reg.is_allowed(&stranger, &hash).unwrap());
    }
}
