use std::path::PathBuf;

use anyhow::Result;
use iroh_rings::RedbRegistry;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::{Node, ShareTicket};
use crate::daemon::protocol::Event;

use super::send;

pub async fn handle_receive(
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
    ticket_str: String,
    dest: PathBuf,
    force_overwrite: bool,
) -> Result<()> {
    let ticket = ShareTicket::from_uri(&ticket_str)?;
    let hash_hex = ticket.hash().to_string();

    let dest_path = if dest.is_dir() {
        dest.join(ticket.name.as_deref().unwrap_or(hash_hex.as_str()))
    } else {
        dest.clone()
    };
    if dest_path.exists() && !force_overwrite {
        anyhow::bail!(
            "destination '{}' already exists; \
             use --dest to choose a different location or --force-overwrite to replace it",
            dest_path.display()
        );
    }

    send(
        tx,
        Event::line(
            req_id,
            format!(
                "Fetching {} from {}{}",
                ticket.hash(),
                ticket.peer_id(),
                ticket
                    .name
                    .as_deref()
                    .map(|n| format!(" ({n})"))
                    .unwrap_or_default()
            ),
        ),
    )
    .await;
    send(
        tx,
        Event::line(req_id, format!("Destination: {}", dest_path.display())),
    )
    .await;
    send(
        tx,
        Event::line(
            req_id,
            "(If interrupted, re-run this command to resume from where it stopped.)",
        ),
    )
    .await;

    // Progress events are emitted by a separate task so they don't block the
    // download future — `on_progress` is `Fn` (not async), so it can't await
    // the channel send directly.
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64)>();
    let on_progress = move |done: u64, total: u64| {
        let _ = progress_tx.send((done, total));
    };

    let event_tx = tx.clone();
    let progress_task = tokio::spawn(async move {
        while let Some((done, total)) = progress_rx.recv().await {
            let _ = event_tx.send(Event::progress(req_id, done, total)).await;
        }
    });

    let result = node
        .download_with_progress(&ticket, &dest_path, on_progress)
        .await;

    // on_progress has been dropped (download finished), so progress_tx is gone
    // and progress_rx will return None — awaiting the task is instant.
    let _ = progress_task.await;

    match result {
        Ok(()) => {
            send(tx, Event::line(req_id, "Transfer complete.")).await;
            send(tx, Event::done(req_id)).await;
            Ok(())
        }
        Err(e) => {
            let mut msg = format!("Transfer failed: {e:#}");
            if e.to_string().contains("access denied") {
                let public_id = node.endpoint.id();
                msg.push_str(&format!(
                    "\n\nYour peer-id: {public_id}\n\
                     Ask the file owner to run:\n  rdrop ring add <ring-name> {public_id}"
                ));
            }
            anyhow::bail!(msg)
        }
    }
}
