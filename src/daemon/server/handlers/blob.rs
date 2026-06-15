use std::path::PathBuf;

use anyhow::Result;
use iroh_rings::{Permission, Registry, OPEN_RING_NAME};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::Node;
use crate::daemon::protocol::Event;

use crate::util::format_size;

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
                    "Warning: not attached to any ring — this blob won't be served to any peer.",
                ),
            )
            .await;
            send(tx, Event::line(req_id, "Attach to a ring with:")).await;
            send(
                tx,
                Event::line(req_id, format!("  rdrop blob attach {hash} <ring-name>")),
            )
            .await;
            send(
                tx,
                Event::line(req_id, format!("  rdrop blob attach {hash} --open")),
            )
            .await;
        } else {
            send(tx, Event::line(req_id, "Already attached to:")).await;
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
                    Event::line(req_id, "Attached to open ring (publicly accessible)"),
                )
                .await;
            } else {
                send(
                    tx,
                    Event::line(req_id, format!("Attached to ring '{ring}'")),
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
        for entry in blobs {
            let blob_rings = node.registry.list_resource_rings(*entry.hash.as_bytes())?;
            let ticket = node.make_ticket(entry.hash, entry.format, Some(entry.name.clone()));
            let ticket_str = ticket.to_uri()?;

            let kind_str = match entry.format {
                iroh_blobs::BlobFormat::HashSeq => {
                    let count = entry
                        .file_count
                        .map(|n| format!("{n} files"))
                        .unwrap_or_else(|| "dir".into());
                    format!("dir, {count}")
                }
                _ => "file".into(),
            };
            let size_str = entry
                .total_size
                .map(format_size)
                .unwrap_or_else(|| "?".into());

            send(tx, Event::blank(req_id)).await;
            send(
                tx,
                Event::line(req_id, format!("  {}  ({})", entry.hash, entry.name)),
            )
            .await;
            send(
                tx,
                Event::line(req_id, format!("    kind:   {kind_str}  ({size_str})")),
            )
            .await;
            if blob_rings.is_empty() {
                send(
                    tx,
                    Event::line(req_id, "    rings:  (none — inaccessible to all peers)"),
                )
                .await;
            } else {
                let names: Vec<_> = blob_rings
                    .iter()
                    .map(|(r, _)| r.as_str().to_owned())
                    .collect();
                send(
                    tx,
                    Event::line(req_id, format!("    rings:  {}", names.join(", "))),
                )
                .await;
            }
            send(tx, Event::line(req_id, format!("    ticket: {ticket_str}"))).await;
            let ring_names: Vec<_> = blob_rings
                .iter()
                .map(|(r, _)| r.as_str().to_owned())
                .collect();
            send(
                tx,
                Event::record(
                    req_id,
                    serde_json::json!({
                        "hash": entry.hash.to_string(),
                        "name": entry.name,
                        "kind": kind_str,
                        "file_count": entry.file_count,
                        "size_bytes": entry.total_size,
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

pub(crate) async fn handle_attach<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    target: String,
    rings: Vec<String>,
    open: bool,
) -> Result<()> {
    if rings.is_empty() && !open {
        anyhow::bail!(
            "nothing to attach: specify at least one ring name or --open\n\
             \n\
             Examples:\n  rdrop blob attach {target} <ring-name>\n  rdrop blob attach {target} --open"
        );
    }

    let hash = resolve_target(node, &target).await?;
    for ring in &rings {
        node.registry
            .add_ring_to_resource(*hash.as_bytes(), ring, &[Permission::Read])?;
        send(
            tx,
            Event::line(req_id, format!("Attached {hash} to ring '{ring}'")),
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
                format!("Attached {hash} to open ring (publicly accessible)"),
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
pub(crate) async fn handle_detach<R: Registry + Clone + Send + Sync + 'static>(
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
            Event::line(req_id, format!("Detached {hash} from all rings")),
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
        anyhow::bail!("blob {hash} is not attached to: {}", to_remove.join(", "));
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
            Event::line(req_id, format!("Detached {hash} from ring '{name}'")),
        )
        .await;
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}
