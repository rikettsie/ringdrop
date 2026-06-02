use std::path::PathBuf;

use anyhow::Result;
use iroh_rings::{Permission, Registry, OPEN_RING_NAME};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::Node;
use crate::daemon::protocol::Event;

use super::{format_ring, resolve_target, send};

pub(crate) async fn handle_import<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    path: PathBuf,
    rings: Vec<String>,
    open: bool,
) -> Result<()> {
    let (hash, format) = node.import_path(&path).await?;

    let effective_rings: Vec<String> = if open {
        vec![OPEN_RING_NAME.to_owned()]
    } else {
        rings
    };

    if effective_rings.is_empty() {
        let existing = node.registry.list_resource_rings(*hash.as_bytes())?;
        if existing.is_empty() {
            send(
                tx,
                Event::line(
                    req_id,
                    "Warning: not tagged — this blob won't be served to any peer.",
                ),
            )
            .await;
            send(tx, Event::line(req_id, "Tag it with:")).await;
            send(
                tx,
                Event::line(req_id, format!("  rdrop tag {hash} --ring <ring-name>")),
            )
            .await;
            send(
                tx,
                Event::line(req_id, format!("  rdrop tag {hash} --open")),
            )
            .await;
        } else {
            send(tx, Event::line(req_id, "Already tagged:")).await;
            for (r, _) in &existing {
                send(tx, Event::line(req_id, format_ring(r))).await;
            }
        }
    } else {
        for ring in &effective_rings {
            node.registry
                .add_ring_to_resource(*hash.as_bytes(), ring, &[Permission::Read])?;
            if ring == OPEN_RING_NAME {
                send(
                    tx,
                    Event::line(req_id, "Tagged as open (publicly accessible)"),
                )
                .await;
            } else {
                send(
                    tx,
                    Event::line(req_id, format!("Tagged with ring '{ring}'")),
                )
                .await;
            }
        }
    }

    let display_name = path.file_name().map(|n| n.to_string_lossy().into_owned());
    let ticket = node.make_ticket(hash, format, display_name.clone());
    let ticket_str = ticket.to_uri()?;

    send(tx, Event::blank(req_id)).await;
    send(tx, Event::line(req_id, "Ticket:")).await;
    send(tx, Event::line(req_id, format!("  {ticket_str}"))).await;
    send(tx, Event::blank(req_id)).await;
    send(tx, Event::line(req_id, "Peers receive with:")).await;
    send(
        tx,
        Event::line(req_id, format!("  rdrop receive {ticket_str}")),
    )
    .await;
    let format_str = if format == iroh_blobs::BlobFormat::HashSeq {
        "hash_seq"
    } else {
        "raw"
    };
    send(
        tx,
        Event::record(
            req_id,
            serde_json::json!({
                "hash": hash.to_string(),
                "format": format_str,
                "name": display_name.as_deref().unwrap_or(""),
                "ticket": ticket_str,
            }),
        ),
    )
    .await;
    send(tx, Event::done(req_id)).await;
    Ok(())
}

pub(crate) async fn handle_blob_list<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    peer: Option<String>,
    rings: Option<Vec<String>>,
) -> Result<()> {
    let blobs = node.list_blobs(peer.as_deref(), rings).await?;
    if blobs.is_empty() {
        let peer_str = peer
            .as_deref()
            .map(|p| format!(" accessible by peer {p}"))
            .unwrap_or_default();
        send(
            tx,
            Event::line(req_id, format!("No blobs in local store{}.", peer_str)),
        )
        .await;
    } else {
        send(tx, Event::line(req_id, format!("{} blobs:", blobs.len()))).await;
        for (hash, format, name) in blobs {
            let rings = node.registry.list_resource_rings(*hash.as_bytes())?;
            let ticket = node.make_ticket(hash, format, Some(name.clone()));
            let ticket_str = ticket.to_uri()?;
            send(tx, Event::blank(req_id)).await;
            send(tx, Event::line(req_id, format!("  {hash}  ({name})"))).await;
            if rings.is_empty() {
                send(
                    tx,
                    Event::line(req_id, "    no rings:  (inaccessible for all peers)"),
                )
                .await;
            } else {
                let names: Vec<_> = rings.iter().map(|(r, _)| r.as_str().to_owned()).collect();
                send(
                    tx,
                    Event::line(req_id, format!("    rings:  {}", names.join(", "))),
                )
                .await;
            }
            send(tx, Event::line(req_id, format!("    ticket: {ticket_str}"))).await;
            let ring_names: Vec<_> = rings.iter().map(|(r, _)| r.as_str().to_owned()).collect();
            send(
                tx,
                Event::record(
                    req_id,
                    serde_json::json!({
                        "hash": hash.to_string(),
                        "name": name,
                        "rings": ring_names,
                        "ticket": ticket_str,
                    }),
                ),
            )
            .await;
        }
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}

pub(crate) async fn handle_blob_remove<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    target: String,
) -> Result<()> {
    let hash = resolve_target(node, &target).await?;
    node.registry.remove_ring_from_resource(*hash.as_bytes())?;
    node.delete_blob(hash).await?;
    send(tx, Event::line(req_id, format!("Removed {hash}"))).await;
    send(
        tx,
        Event::line(req_id, "Disk space will be reclaimed on the next GC cycle."),
    )
    .await;
    send(
        tx,
        Event::record(req_id, serde_json::json!({ "hash": hash.to_string() })),
    )
    .await;
    send(tx, Event::done(req_id)).await;
    Ok(())
}
