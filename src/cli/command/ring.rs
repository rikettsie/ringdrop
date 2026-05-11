use std::path::Path;

use anyhow::{anyhow, Result};

use crate::registry::{RedbRegistry, Registry, OPEN_RING_NAME};
use crate::util::parse_peer_id;

use super::RingCmd;

pub fn run(cmd: RingCmd, data_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let cfg = crate::config::Config::load_or_create(data_dir)?;
    let public_id = cfg.public_id();
    let registry = RedbRegistry::open(data_dir.join("registry.redb"))?;

    match cmd {
        RingCmd::New { name } => {
            registry.create_ring(&name)?;
            println!("Ring created: {name}");
            println!("Add peers: rdrop ring add {name} <peer-id>");
        }
        RingCmd::List => {
            let rings = registry.list_rings()?;
            println!("{} rings:", rings.len());
            for r in rings {
                if r.is_open() {
                    println!(
                        "  {}  — publicly accessible (no membership required)",
                        r.as_str()
                    );
                } else {
                    let members = registry.list_ring_peers(r.as_str())?;
                    println!("  {}  ({} members)", r.as_str(), members.len());
                }
            }
        }
        RingCmd::Add {
            ring,
            peer,
            nickname,
        } => {
            if ring == OPEN_RING_NAME {
                println!("The open ring has no membership list — everyone is welcome by default.");
                return Ok(());
            }
            let peer = parse_peer_id(&peer)?;
            if peer == public_id {
                return Err(anyhow!("cannot add yourself to a ring"));
            }
            registry.add_peer_to_ring(&ring, peer, nickname.as_deref())?;
            match &nickname {
                Some(nick) => println!("Added {peer} ({nick}) to ring {ring}"),
                None => println!("Added {peer} to ring {ring}"),
            }
        }
        RingCmd::Remove { ring, peer } => {
            if ring == OPEN_RING_NAME {
                println!("The open ring has no membership list to remove from.");
                return Ok(());
            }
            let peer = parse_peer_id(&peer)?;
            registry.remove_peer_from_ring(&ring, peer)?;
            println!("Removed {peer} from ring {ring}");
        }
        RingCmd::Members { ring } => {
            if ring == OPEN_RING_NAME {
                println!("The open ring is public — any peer may access blobs tagged with it.");
                return Ok(());
            }
            let members = registry.list_ring_peers(&ring)?;
            if members.is_empty() {
                println!("Ring '{ring}' has no members yet.");
                println!("Add peers: rdrop ring add {ring} <peer-id>");
                println!("Peers print their peer-id with: rdrop id");
            } else {
                println!("Ring '{ring}' — {} members:", members.len());
                for (peer, nick) in members {
                    match nick {
                        Some(n) => println!("  {peer}  ({n})"),
                        None => println!("  {peer}"),
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn ring_add_self_is_rejected() {
        let dir = TempDir::new().unwrap();
        let cfg = crate::config::Config::load_or_create(dir.path()).unwrap();
        let public_id = cfg.public_id();
        RedbRegistry::open(dir.path().join("registry.redb"))
            .unwrap()
            .create_ring("friends")
            .unwrap();

        let err = run(
            RingCmd::Add {
                ring: "friends".into(),
                peer: public_id.to_string(),
                nickname: None,
            },
            dir.path(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("yourself"));
    }

    #[test]
    fn ring_add_to_open_ring_does_not_add_member() {
        let dir = TempDir::new().unwrap();
        crate::config::Config::load_or_create(dir.path()).unwrap();
        let peer = iroh::SecretKey::generate().public();

        run(
            RingCmd::Add {
                ring: OPEN_RING_NAME.into(),
                peer: peer.to_string(),
                nickname: None,
            },
            dir.path(),
        )
        .unwrap();

        assert_eq!(
            RedbRegistry::open(dir.path().join("registry.redb"))
                .unwrap()
                .list_ring_peers(OPEN_RING_NAME)
                .unwrap()
                .len(),
            0
        );
    }
}
