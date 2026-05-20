//! Server-layer IPC tests using [`InMemoryRegistry`].
//!
//! These tests exercise the full [`DaemonServer`]→[`DaemonClient`] round-trip
//! without touching redb: the registry lives in memory, only the blob store
//! directory is on disk. They verify that `DaemonServer<R>` works correctly
//! with a non-redb registry and that typed errors from iroh-rings propagate
//! to the client correctly.
//!
//! [`InMemoryRegistry`]: iroh_rings::InMemoryRegistry
//! [`DaemonServer`]: ringdrop::daemon::server::DaemonServer
//! [`DaemonClient`]: ringdrop::daemon::client::DaemonClient

mod common;

use ringdrop::daemon::client::DaemonClient;
use ringdrop::daemon::protocol::{EventKind, Op};

#[tokio::test]
async fn node_id_returns_nonempty_string() {
    let daemon = common::TestDaemonMem::start().await;
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(Op::NodeId, |event| {
            if let EventKind::Line { text } = event.kind {
                lines.push(text);
            }
        })
        .await
        .unwrap();
    assert_eq!(lines.len(), 1, "expected exactly one line for NodeId");
    assert!(!lines[0].is_empty(), "node ID must not be empty");
    daemon.shutdown().await;
}

#[tokio::test]
async fn blob_list_on_empty_store_reports_no_blobs() {
    let daemon = common::TestDaemonMem::start().await;
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(Op::BlobList, |event| {
            if let EventKind::Line { text } = event.kind {
                lines.push(text);
            }
        })
        .await
        .unwrap();
    assert_eq!(lines, vec!["No blobs in local store."]);
    daemon.shutdown().await;
}

#[tokio::test]
async fn ring_new_then_ring_list_shows_created_ring() {
    let daemon = common::TestDaemonMem::start().await;
    daemon
        .client
        .run(Op::RingNew {
            name: "buddies".into(),
        })
        .await
        .unwrap();
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(Op::RingList, |event| {
            if let EventKind::Line { text } = event.kind {
                lines.push(text);
            }
        })
        .await
        .unwrap();
    assert!(
        lines.iter().any(|l| l.contains("buddies")),
        "ring list should include the newly created ring; got: {lines:?}"
    );
    daemon.shutdown().await;
}

// Tests that `Error::RingAlreadyExists` from the new iroh-rings typed-error
// branch surfaces as an Error event (not a panic or silent failure).
#[tokio::test]
async fn ring_new_duplicate_name_returns_error() {
    let daemon = common::TestDaemonMem::start().await;
    daemon
        .client
        .run(Op::RingNew {
            name: "alpha".into(),
        })
        .await
        .unwrap();
    let err = daemon
        .client
        .run(Op::RingNew {
            name: "alpha".into(),
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("alpha"),
        "error message should mention the ring name; got: {err}"
    );
    daemon.shutdown().await;
}

#[tokio::test]
async fn shutdown_stops_the_server() {
    let common::TestDaemonMem {
        port,
        client,
        handle,
        ..
    } = common::TestDaemonMem::start().await;
    client.run(Op::Shutdown).await.unwrap();
    handle.await.unwrap();
    assert!(
        !DaemonClient::new(port).is_running().await,
        "server should no longer be reachable after shutdown"
    );
}
