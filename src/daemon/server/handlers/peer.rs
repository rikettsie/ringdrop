//! Handlers for peer address-book ops: [`Op::PeerAdd`], [`Op::PeerList`],
//! and [`Op::PeerRemove`].
//!
//! [`Op::PeerAdd`]: crate::daemon::protocol::Op::PeerAdd
//! [`Op::PeerList`]: crate::daemon::protocol::Op::PeerList
//! [`Op::PeerRemove`]: crate::daemon::protocol::Op::PeerRemove

use anyhow::Result;
use iroh_rings::Registry;

use crate::core::grants::{GrantStore, Privilege};
use crate::core::peers::PeerStore;
use crate::util::{format_peer_entry, parse_peer_id};

/// Register a peer in the store, optionally setting or updating its nickname.
///
/// - With `nickname`: always writes the new nickname (idempotent when the same
///   value is passed twice).
/// - Without `nickname`: registers the peer if absent; preserves any existing
///   nickname if the peer is already known.
///
/// # Errors
///
/// Returns an error if the peer ID is invalid or the store write fails.
pub(crate) fn peer_add_lines(
    peer_store: &PeerStore,
    peer_str: &str,
    nickname: Option<&str>,
) -> Result<Vec<String>> {
    let peer_id = parse_peer_id(peer_str)?;
    let line = if let Some(nick) = nickname {
        peer_store.upsert(peer_id, Some(nick))?;
        format!("Peer {peer_id} added ({nick})")
    } else {
        peer_store.ensure(peer_id)?;
        format!("Peer {peer_id} added")
    };
    Ok(vec![line])
}

/// List all peers in the store.
///
/// # Errors
///
/// Returns an error if the store read fails.
pub(crate) fn peer_list_lines(peer_store: &PeerStore) -> Result<Vec<String>> {
    let peers = peer_store.list()?;
    if peers.is_empty() {
        return Ok(vec![
            "No peers yet.".to_owned(),
            "Add peers with: rdrop peer add <peer-id> [--nickname <name>]".to_owned(),
        ]);
    }
    let mut out = vec![format!("{} peers:", peers.len())];
    for (peer, nick) in peers {
        out.push(format!("  {}", format_peer_entry(&peer, nick.as_deref())));
    }
    Ok(out)
}

/// Remove a peer from the store, all rings, and all catalog grants.
///
/// Errors if the peer is not in the store, consistent with how `ring remove`
/// and `grant remove` behave.
///
/// # Errors
///
/// Returns an error if the peer is not found, a registry lookup fails, or a
/// store write fails.
pub(crate) fn peer_remove_lines<R: Registry>(
    peer_store: &PeerStore,
    grant_store: &GrantStore,
    registry: &R,
    peer_str: &str,
) -> Result<Vec<String>> {
    let peer_id = parse_peer_id(peer_str)?;
    anyhow::ensure!(
        peer_store.get(&peer_id)?.is_some(),
        "peer not found: {peer_id}"
    );

    let mut removed_from: Vec<String> = Vec::new();
    for ring in registry.list_rings()? {
        if ring.is_open() {
            continue;
        }
        let members = registry.list_ring_peers(ring.as_str())?;
        if members.iter().any(|(id, _)| *id == peer_id) {
            registry.remove_peer_from_ring(ring.as_str(), peer_id)?;
            removed_from.push(ring.as_str().to_owned());
        }
    }

    let mut revoked: Vec<&str> = Vec::new();
    for privilege in [Privilege::BlobList] {
        if grant_store.has_grant(privilege, &peer_id)? {
            grant_store.revoke(privilege, peer_id)?;
            revoked.push(privilege.as_str());
        }
    }

    peer_store.remove(peer_id)?;

    let mut out = vec![format!("Removed peer {peer_id}")];
    if !removed_from.is_empty() {
        out.push(format!(
            "  also removed from rings: {}",
            removed_from.join(", ")
        ));
    }
    if !revoked.is_empty() {
        out.push(format!("  also revoked grants: {}", revoked.join(", ")));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_rings::RedbRegistry;
    use tempfile::TempDir;

    fn setup(dir: &TempDir) -> (RedbRegistry, PeerStore, GrantStore) {
        let registry = RedbRegistry::open(dir.path().join("registry.redb")).unwrap();
        let peers = PeerStore::open(dir.path().join("peers.redb")).unwrap();
        let grants = GrantStore::open(dir.path().join("grants.redb")).unwrap();
        (registry, peers, grants)
    }

    fn new_peer() -> (iroh::EndpointId, String) {
        let id = iroh::SecretKey::generate().public();
        (id, id.to_string())
    }

    #[test]
    fn peer_add_without_nickname_adds_peer() {
        let dir = TempDir::new().unwrap();
        let (_, peers, _) = setup(&dir);
        let (peer_id, peer_str) = new_peer();

        let lines = peer_add_lines(&peers, &peer_str, None).unwrap();
        assert!(lines[0].contains(&peer_id.to_string()));
        assert_eq!(peers.get(&peer_id).unwrap(), Some(None));
    }

    #[test]
    fn peer_add_with_nickname_stores_nickname() {
        let dir = TempDir::new().unwrap();
        let (_, peers, _) = setup(&dir);
        let (peer_id, peer_str) = new_peer();

        peer_add_lines(&peers, &peer_str, Some("alice")).unwrap();
        assert_eq!(peers.get(&peer_id).unwrap(), Some(Some("alice".to_owned())));
    }

    #[test]
    fn peer_add_on_existing_peer_updates_nickname() {
        let dir = TempDir::new().unwrap();
        let (_, peers, _) = setup(&dir);
        let (peer_id, peer_str) = new_peer();

        peer_add_lines(&peers, &peer_str, Some("alice")).unwrap();
        peer_add_lines(&peers, &peer_str, Some("alice2")).unwrap();
        assert_eq!(
            peers.get(&peer_id).unwrap(),
            Some(Some("alice2".to_owned()))
        );
    }

    #[test]
    fn peer_list_on_empty_store_returns_no_peers_message() {
        let dir = TempDir::new().unwrap();
        let (_, peers, _) = setup(&dir);
        let lines = peer_list_lines(&peers).unwrap();
        assert_eq!(lines[0], "No peers yet.");
    }

    #[test]
    fn peer_list_returns_count_and_one_line_per_peer() {
        let dir = TempDir::new().unwrap();
        let (_, peers, _) = setup(&dir);
        let (id, _) = new_peer();
        peers.upsert(id, Some("alice")).unwrap();

        let lines = peer_list_lines(&peers).unwrap();
        assert_eq!(lines.len(), 2, "header + one entry");
        assert!(lines[0].contains("1 peers:"));
        assert!(lines[1].contains("alice"));
        assert!(lines[1].contains(&id.to_string()));
    }

    #[test]
    fn peer_add_without_nickname_preserves_existing_nickname() {
        let dir = TempDir::new().unwrap();
        let (_, peers, _) = setup(&dir);
        let (peer_id, peer_str) = new_peer();
        peer_add_lines(&peers, &peer_str, Some("alice")).unwrap();

        peer_add_lines(&peers, &peer_str, None).unwrap();
        assert_eq!(
            peers.get(&peer_id).unwrap(),
            Some(Some("alice".to_owned())),
            "nickname must be preserved when peer add is called without --nickname"
        );
    }

    #[test]
    fn peer_add_with_same_nickname_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let (_, peers, _) = setup(&dir);
        let (peer_id, peer_str) = new_peer();

        peer_add_lines(&peers, &peer_str, Some("alice")).unwrap();
        peer_add_lines(&peers, &peer_str, Some("alice")).unwrap();
        assert_eq!(peers.get(&peer_id).unwrap(), Some(Some("alice".to_owned())));
    }

    #[test]
    fn peer_remove_removes_from_store_and_all_rings() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, grants) = setup(&dir);
        let (peer_id, peer_str) = new_peer();
        peers.upsert(peer_id, Some("alice")).unwrap();
        registry.create_ring("friends").unwrap();
        registry.create_ring("work").unwrap();
        registry.add_peer_to_ring("friends", peer_id, None).unwrap();
        registry.add_peer_to_ring("work", peer_id, None).unwrap();

        let lines = peer_remove_lines(&peers, &grants, &registry, &peer_str).unwrap();

        assert!(peers.get(&peer_id).unwrap().is_none());
        assert!(registry
            .list_ring_peers("friends")
            .unwrap()
            .iter()
            .all(|(id, _)| *id != peer_id));
        assert!(registry
            .list_ring_peers("work")
            .unwrap()
            .iter()
            .all(|(id, _)| *id != peer_id));
        assert!(lines
            .iter()
            .any(|l| l.contains("friends") || l.contains("work")));
    }

    #[test]
    fn peer_remove_also_revokes_grants() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, grants) = setup(&dir);
        let (peer_id, peer_str) = new_peer();
        peers.upsert(peer_id, None).unwrap();
        grants.grant(Privilege::BlobList, peer_id).unwrap();

        let lines = peer_remove_lines(&peers, &grants, &registry, &peer_str).unwrap();

        assert!(!grants.has_grant(Privilege::BlobList, &peer_id).unwrap());
        assert!(lines.iter().any(|l| l.contains("blob-list")));
    }

    #[test]
    fn peer_remove_on_unknown_peer_errors() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, grants) = setup(&dir);
        let (_, peer_str) = new_peer();
        assert!(peer_remove_lines(&peers, &grants, &registry, &peer_str).is_err());
    }

    #[test]
    fn peer_remove_with_no_ring_memberships_succeeds() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, grants) = setup(&dir);
        let (peer_id, peer_str) = new_peer();
        peers.upsert(peer_id, None).unwrap();

        let lines = peer_remove_lines(&peers, &grants, &registry, &peer_str).unwrap();
        assert!(lines[0].contains(&peer_id.to_string()));
        assert_eq!(lines.len(), 1, "no extra lines expected");
    }
}
