use anyhow::Result;
use iroh_rings::{Registry, OPEN_RING_NAME};

use crate::util::parse_peer_id;

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

pub(crate) fn ring_add_lines(
    registry: &impl Registry,
    public_id: iroh::EndpointId,
    ring: &str,
    peer: &str,
    nickname: Option<&str>,
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
    registry.add_peer_to_ring(ring, peer_id, nickname)?;
    let line = match nickname {
        Some(nick) => format!("Added {peer_id} ({nick}) to ring {ring}"),
        None => format!("Added {peer_id} to ring {ring}"),
    };
    Ok(vec![line])
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

pub(crate) fn ring_members_lines(registry: &impl Registry, ring: &str) -> Result<Vec<String>> {
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
    for (peer, nick) in members {
        match nick {
            Some(n) => out.push(format!("  {peer}  ({n})")),
            None => out.push(format!("  {peer}")),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_rings::RedbRegistry;
    use tempfile::TempDir;

    fn setup(dir: &TempDir) -> (RedbRegistry, iroh::EndpointId) {
        let cfg = crate::config::Config::load_or_create(dir.path()).unwrap();
        let public_id = cfg.public_id();
        let registry = RedbRegistry::open(dir.path().join("registry.redb")).unwrap();
        (registry, public_id)
    }

    #[test]
    fn ring_add_self_is_rejected() {
        let dir = TempDir::new().unwrap();
        let (registry, public_id) = setup(&dir);
        registry.create_ring("friends").unwrap();

        let err = ring_add_lines(
            &registry,
            public_id,
            "friends",
            &public_id.to_string(),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("yourself"));
    }

    #[test]
    fn ring_add_to_open_ring_does_not_add_member() {
        let dir = TempDir::new().unwrap();
        let (registry, public_id) = setup(&dir);
        let peer = iroh::SecretKey::generate().public();

        ring_add_lines(
            &registry,
            public_id,
            OPEN_RING_NAME,
            &peer.to_string(),
            None,
        )
        .unwrap();

        assert_eq!(registry.list_ring_peers(OPEN_RING_NAME).unwrap().len(), 0);
    }
}
