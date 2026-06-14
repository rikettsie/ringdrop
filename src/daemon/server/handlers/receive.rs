use std::path::PathBuf;

use anyhow::Result;
use iroh_rings::Registry;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::{Node, ProgressEvent, ShareTicket};
use crate::daemon::protocol::Event;

use super::send;

pub(crate) async fn handle_receive<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
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
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();
    let on_progress = move |ev: ProgressEvent| {
        let _ = progress_tx.send(ev);
    };

    let event_tx = tx.clone();
    let progress_task = tokio::spawn(async move {
        let mut current_file: Option<(usize, usize, String)> = None;
        while let Some(ev) = progress_rx.recv().await {
            match ev {
                ProgressEvent::FileStart { index, total, name } => {
                    current_file = Some((index, total, name));
                }
                ProgressEvent::Bytes { done, total } => {
                    let ipc_ev = if let Some((fi, ft, ref fname)) = current_file {
                        Event::file_progress(req_id, fi, ft, fname.clone(), done, total)
                    } else {
                        Event::progress(req_id, done, total)
                    };
                    let _ = event_tx.send(ipc_ev).await;
                }
            }
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
