use anyhow::Result;
use iroh_rings::{Registry, OPEN_RING_NAME};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::Node;
use crate::daemon::protocol::Event;

use super::{format_ring, resolve_target, send};

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
        node.registry.add_ring_to_resource(*hash.as_bytes(), ring)?;
        send(
            tx,
            Event::line(req_id, format!("Tagged {hash} with ring '{ring}'")),
        )
        .await;
    }
    if open {
        node.registry
            .add_ring_to_resource(*hash.as_bytes(), OPEN_RING_NAME)?;
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

pub(crate) async fn handle_tags<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    target: String,
) -> Result<()> {
    let hash = resolve_target(node, &target).await?;
    let rings = node.registry.list_resource_rings(*hash.as_bytes())?;
    if rings.is_empty() {
        send(
            tx,
            Event::line(
                req_id,
                format!("{hash}: no rings (access denied to all peers)"),
            ),
        )
        .await;
    } else {
        send(
            tx,
            Event::line(req_id, format!("{}: {} rings:", hash, rings.len())),
        )
        .await;
        for ring in &rings {
            send(tx, Event::line(req_id, format_ring(ring))).await;
        }
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}
