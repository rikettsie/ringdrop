//! Handler for [`Op::RemoteBlobList`].
//!
//! [`Op::RemoteBlobList`]: crate::daemon::protocol::Op::RemoteBlobList

use anyhow::Result;
use iroh::EndpointAddr;
use iroh_rings::Registry;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::Node;
use crate::daemon::protocol::Event;
use crate::util::parse_peer_id;

use super::send;

/// Connect to `peer` (identified by its base32 [`EndpointId`] string) and
/// stream its accessible blob catalog as [`Event::line`] events.
///
/// [`EndpointId`]: iroh::EndpointId
pub(crate) async fn handle_remote_blob_list<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    peer: String,
) -> Result<()> {
    let peer_id = parse_peer_id(&peer)?;
    let addr = EndpointAddr::new(peer_id);
    let entries = node.catalog_connect(addr).await?;
    if entries.is_empty() {
        send(
            tx,
            Event::line(req_id, "No accessible blobs on remote node."),
        )
        .await;
    } else {
        send(tx, Event::line(req_id, format!("{} blobs:", entries.len()))).await;
        for entry in entries {
            let ticket_str = entry.ticket.to_uri()?;
            send(tx, Event::blank(req_id)).await;
            send(
                tx,
                Event::line(req_id, format!("  {}  ({})", entry.hash, entry.name)),
            )
            .await;
            send(tx, Event::line(req_id, format!("    ticket: {ticket_str}"))).await;
            send(
                tx,
                Event::record(
                    req_id,
                    serde_json::json!({
                        "hash": entry.hash.to_string(),
                        "name": entry.name,
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
