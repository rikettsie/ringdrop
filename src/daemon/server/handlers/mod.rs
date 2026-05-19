pub(super) mod blob;
pub(super) mod receive;
pub(super) mod ring;
pub(super) mod tag;

use std::path::PathBuf;

use anyhow::Result;
use iroh_blobs::Hash;
use iroh_rings::Registry;
use tokio::sync::mpsc;

use crate::core::Node;
use crate::daemon::protocol::Event;
use crate::util::parse_hash;

pub(super) async fn send(tx: &mpsc::Sender<Event>, event: Event) {
    let _ = tx.send(event).await;
}

pub(super) async fn resolve_target<R: Registry + Clone + Send + Sync + 'static>(
    node: &Node<R>,
    target: &str,
) -> Result<Hash> {
    let path = PathBuf::from(target);
    if path.exists() {
        let (hash, _) = node.import_path(&path).await?;
        Ok(hash)
    } else {
        parse_hash(target)
    }
}
