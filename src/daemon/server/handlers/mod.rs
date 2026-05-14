pub(super) mod blob;
pub(super) mod receive;
pub(super) mod ring;
pub(super) mod tag;

use tokio::sync::mpsc;

use crate::daemon::protocol::Event;

pub(super) async fn send(tx: &mpsc::Sender<Event>, event: Event) {
    let _ = tx.send(event).await;
}
