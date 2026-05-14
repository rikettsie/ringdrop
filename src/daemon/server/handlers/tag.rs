use std::path::PathBuf;

use anyhow::Result;
use iroh_rings::{RedbRegistry, Registry, OPEN_RING_NAME};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::Node;
use crate::daemon::protocol::Event;
use crate::util::parse_hash;

use super::send;

pub async fn handle_tag(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
    target: String,
    rings: Vec<String>,
    open: bool,
) -> Result<()> {
    let path = PathBuf::from(&target);
    let hash = if path.exists() {
        let (hash, _) = node.import_path(&path).await?;
        hash
    } else {
        parse_hash(&target)?
    };
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

pub async fn handle_tags(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
    target: String,
) -> Result<()> {
    let path = PathBuf::from(&target);
    let hash = if path.exists() {
        let (hash, _) = node.import_path(&path).await?;
        hash
    } else {
        parse_hash(&target)?
    };
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
            if ring.is_open() {
                send(
                    tx,
                    Event::line(
                        req_id,
                        format!("  {}  (open — publicly accessible)", ring.as_str()),
                    ),
                )
                .await;
            } else {
                send(tx, Event::line(req_id, format!("  {}", ring.as_str()))).await;
            }
        }
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}
