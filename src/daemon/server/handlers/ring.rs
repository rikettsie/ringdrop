//! Handlers for ring management ops: [`Op::RingNew`], [`Op::RingList`],
//! [`Op::RingAdd`], [`Op::RingRemove`], and [`Op::RingMembers`].
//!
//! [`Op::RingNew`]: crate::daemon::protocol::Op::RingNew
//! [`Op::RingList`]: crate::daemon::protocol::Op::RingList
//! [`Op::RingAdd`]: crate::daemon::protocol::Op::RingAdd
//! [`Op::RingRemove`]: crate::daemon::protocol::Op::RingRemove
//! [`Op::RingMembers`]: crate::daemon::protocol::Op::RingMembers

use anyhow::Result;
use iroh_rings::{Registry, OPEN_RING_NAME};

use crate::core::peers::PeerStore;
use crate::util::{display_peer, parse_peer_id};

pub(crate) fn ring_new_lines(registry: &impl Registry, name: &str) -> Result<Vec<String>> {
    registry.create_ring(name)?;
    Ok(vec![
        format!("Ring created: {name}"),
        format!("Add peers: rdrop ring add {name} <peer-id>"),
    ])
}

pub(crate) fn ring_list_lines(registry: &impl Registry) -> Result<Vec<String>> {
    let rings = registry.list_rings()?;
    let mut out = vec![format!("{} rings:", rings.len())];
    for r in rings {
        if r.is_open() {
            out.push(format!(
                "  {}  — publicly accessible (no membership required)",
                r.as_str()
            ));
        } else {
            let members = registry.list_ring_peers(r.as_str())?;
            out.push(format!("  {}  ({} members)", r.as_str(), members.len()));
        }
    }
    Ok(out)
}

/// Add `peer` to `ring` and ensure the peer exists in the peer store.
///
/// Nicknames are managed independently via [`Op::PeerNick`] / [`Op::PeerAdd`].
/// The iroh-rings registry is always called with `label: None`.
///
/// [`Op::PeerNick`]: crate::daemon::protocol::Op::PeerNick
/// [`Op::PeerAdd`]: crate::daemon::protocol::Op::PeerAdd
pub(crate) fn ring_add_lines(
    registry: &impl Registry,
    peer_store: &PeerStore,
    public_id: iroh::EndpointId,
    ring: &str,
    peer: &str,
) -> Result<Vec<String>> {
    if ring == OPEN_RING_NAME {
        return Ok(vec![
            "The open ring has no membership list — everyone is welcome by default.".to_owned(),
        ]);
    }
    let peer_id = parse_peer_id(peer)?;
    if peer_id == public_id {
        anyhow::bail!("cannot add yourself to a ring");
    }
    registry.add_peer_to_ring(ring, peer_id, None)?;
    peer_store.ensure(peer_id)?;
    Ok(vec![format!("Added {peer_id} to ring {ring}")])
}

pub(crate) fn ring_remove_lines(
    registry: &impl Registry,
    ring: &str,
    peer: &str,
) -> Result<Vec<String>> {
    if ring == OPEN_RING_NAME {
        return Ok(vec![
            "The open ring has no membership list to remove from.".to_owned()
        ]);
    }
    let peer_id = parse_peer_id(peer)?;
    registry.remove_peer_from_ring(ring, peer_id)?;
    Ok(vec![format!("Removed {peer_id} from ring {ring}")])
}

/// List members of `ring`, resolving nicknames from the peer store.
pub(crate) fn ring_members_lines(
    registry: &impl Registry,
    peer_store: &PeerStore,
    ring: &str,
) -> Result<Vec<String>> {
    if ring == OPEN_RING_NAME {
        return Ok(vec![
            "The open ring is public — any peer may access blobs tagged with it.".to_owned(),
        ]);
    }
    let members = registry.list_ring_peers(ring)?;
    if members.is_empty() {
        return Ok(vec![
            format!("Ring '{ring}' has no members yet."),
            format!("Add peers: rdrop ring add {ring} <peer-id>"),
            "Peers print their peer-id with: rdrop id".to_owned(),
        ]);
    }
    let mut out = vec![format!("Ring '{ring}' — {} members:", members.len())];
    for (peer, _label) in members {
        out.push(format!("  {}", display_peer(&peer, peer_store)));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_rings::RedbRegistry;
    use tempfile::TempDir;

    fn setup(dir: &TempDir) -> (RedbRegistry, PeerStore, iroh::EndpointId) {
        let cfg = crate::config::Config::load_or_create(dir.path()).unwrap();
        let public_id = cfg.public_id();
        let registry = RedbRegistry::open(dir.path().join("registry.redb")).unwrap();
        let peers = PeerStore::open(dir.path().join("peers.redb")).unwrap();
        (registry, peers, public_id)
    }

    fn new_peer() -> (iroh::EndpointId, String) {
        let id = iroh::SecretKey::generate().public();
        (id, id.to_string())
    }

    #[test]
    fn ring_add_self_is_rejected() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, public_id) = setup(&dir);
        registry.create_ring("friends").unwrap();

        let err = ring_add_lines(
            &registry,
            &peers,
            public_id,
            "friends",
            &public_id.to_string(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("yourself"));
    }

    #[test]
    fn ring_add_to_open_ring_does_not_add_member() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, public_id) = setup(&dir);
        let (_, peer_str) = new_peer();

        ring_add_lines(&registry, &peers, public_id, OPEN_RING_NAME, &peer_str).unwrap();

        assert_eq!(registry.list_ring_peers(OPEN_RING_NAME).unwrap().len(), 0);
    }

    #[test]
    fn ring_add_registers_peer_in_peer_store() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, public_id) = setup(&dir);
        registry.create_ring("friends").unwrap();
        let (peer_id, peer_str) = new_peer();

        ring_add_lines(&registry, &peers, public_id, "friends", &peer_str).unwrap();

        assert!(peers.get(&peer_id).unwrap().is_some());
    }

    #[test]
    fn ring_add_does_not_clear_existing_nickname() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, public_id) = setup(&dir);
        registry.create_ring("friends").unwrap();
        let (peer_id, peer_str) = new_peer();
        peers.upsert(peer_id, Some("alice")).unwrap();

        ring_add_lines(&registry, &peers, public_id, "friends", &peer_str).unwrap();

        assert_eq!(peers.get(&peer_id).unwrap(), Some(Some("alice".to_owned())));
    }

    #[test]
    fn ring_members_shows_nickname_from_peer_store() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, public_id) = setup(&dir);
        registry.create_ring("friends").unwrap();
        let (peer_id, peer_str) = new_peer();

        ring_add_lines(&registry, &peers, public_id, "friends", &peer_str).unwrap();
        peers.set_nickname(peer_id, "alice").unwrap();

        let lines = ring_members_lines(&registry, &peers, "friends").unwrap();
        assert!(lines.iter().any(|l| l.contains("alice")));
    }

    #[test]
    fn ring_members_shows_raw_id_when_no_nickname() {
        let dir = TempDir::new().unwrap();
        let (registry, peers, public_id) = setup(&dir);
        registry.create_ring("friends").unwrap();
        let (peer_id, peer_str) = new_peer();

        ring_add_lines(&registry, &peers, public_id, "friends", &peer_str).unwrap();

        let lines = ring_members_lines(&registry, &peers, "friends").unwrap();
        assert!(lines.iter().any(|l| l.contains(&peer_id.to_string())));
    }
}
