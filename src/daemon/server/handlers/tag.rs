use anyhow::Result;
use iroh_rings::{Permission, Registry, OPEN_RING_NAME};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::Node;
use crate::daemon::protocol::Event;

use super::{resolve_target, send};

pub(crate) async fn handle_tag<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    target: String,
    rings: Vec<String>,
    open: bool,
) -> Result<()> {
    if rings.is_empty() && !open {
        anyhow::bail!(
            "nothing to tag: specify at least one --ring <name> or --open\n\
             \n\
             Examples:\n  rdrop tag {target} --ring friends\n  rdrop tag {target} --open"
        );
    }

    let hash = resolve_target(node, &target).await?;
    for ring in &rings {
        node.registry
            .add_ring_to_resource(*hash.as_bytes(), ring, &[Permission::Read])?;
        send(
            tx,
            Event::line(req_id, format!("Tagged {hash} with ring '{ring}'")),
        )
        .await;
    }
    if open {
        node.registry.add_ring_to_resource(
            *hash.as_bytes(),
            OPEN_RING_NAME,
            &[Permission::Read],
        )?;
        send(
            tx,
            Event::line(
                req_id,
                format!("Tagged {hash} as open (publicly accessible)"),
            ),
        )
        .await;
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}

/// Removes ring associations from a blob.
///
/// - `all`: clear every ring association.
/// - `open`: remove only the open-ring association, keeping named rings.
/// - `rings` (non-empty): remove the listed named rings, keeping everything else.
///
/// For the selective cases the operation is a read-modify-write: current
/// associations are fetched, the target rings are dropped, all remaining
/// associations are re-written. Not atomic at the registry level, but the
/// daemon serialises requests so concurrent modification is not a concern.
pub(crate) async fn handle_untag<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    target: String,
    rings: Vec<String>,
    open: bool,
    all: bool,
) -> Result<()> {
    let hash = resolve_target(node, &target).await?;
    let hash_bytes = *hash.as_bytes();

    if all {
        node.registry.remove_ring_from_resource(hash_bytes)?;
        send(
            tx,
            Event::line(req_id, format!("Untagged {hash} from all rings")),
        )
        .await;
        send(tx, Event::done(req_id)).await;
        return Ok(());
    }

    let to_remove: Vec<&str> = if open {
        vec![OPEN_RING_NAME]
    } else {
        rings.iter().map(String::as_str).collect()
    };

    let current = node.registry.list_resource_rings(hash_bytes)?;

    let actually_removed: Vec<&str> = to_remove
        .iter()
        .copied()
        .filter(|name| current.iter().any(|(r, _)| r.as_str() == *name))
        .collect();

    if actually_removed.is_empty() {
        anyhow::bail!("blob {hash} is not tagged with: {}", to_remove.join(", "));
    }

    // Read-modify-write: keep associations not in the removal set.
    let remaining: Vec<_> = current
        .into_iter()
        .filter(|(ring, _)| !to_remove.contains(&ring.as_str()))
        .collect();

    node.registry.remove_ring_from_resource(hash_bytes)?;
    for (ring, perms) in &remaining {
        node.registry
            .add_ring_to_resource(hash_bytes, ring.as_str(), perms)?;
    }

    for name in &actually_removed {
        send(
            tx,
            Event::line(req_id, format!("Untagged {hash} from ring '{name}'")),
        )
        .await;
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}
