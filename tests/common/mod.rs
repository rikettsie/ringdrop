#![allow(dead_code)]

use std::path::Path;

use iroh_rings::{InMemoryRegistry, RedbRegistry};
use ringdrop::config::Config;
use ringdrop::core::Node;
use ringdrop::daemon::client::DaemonClient;
use ringdrop::daemon::protocol::{EventKind, Op};
use ringdrop::daemon::server::DaemonServer;
use tempfile::TempDir;
use tokio::fs;
use tokio::task::JoinHandle;

pub struct TestNode {
    pub node: Node<RedbRegistry>,
    _dir: TempDir,
}

impl TestNode {
    pub async fn start() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let cfg = Config::load_or_create(dir.path()).expect("config");
        let registry = RedbRegistry::open(dir.path().join("registry.redb")).expect("registry");
        let node = Node::start(dir.path(), cfg, registry)
            .await
            .expect("node start");
        TestNode { node, _dir: dir }
    }

    pub async fn shutdown(self) {
        self.node.shutdown().await.expect("node shutdown");
    }
}

pub struct TestDaemon {
    pub port: u16,
    pub client: DaemonClient,
    pub handle: JoinHandle<()>,
    _dir: TempDir,
}

impl TestDaemon {
    /// Daemon backed by [`RedbRegistry`].
    pub async fn start() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let cfg = Config::load_or_create(dir.path()).expect("config");
        let registry = RedbRegistry::open(dir.path().join("registry.redb")).expect("registry");
        let node = Node::start(dir.path(), cfg, registry)
            .await
            .expect("node start");
        let server = DaemonServer::bind(node, 0).await.expect("bind");
        let port = server.local_port();
        let handle = tokio::spawn(async move { server.run().await.expect("daemon server run") });
        TestDaemon {
            port,
            client: DaemonClient::new(port),
            handle,
            _dir: dir,
        }
    }

    /// Daemon backed by [`InMemoryRegistry`]: no redb file, only the blob
    /// store directory lives on disk.
    pub async fn start_mem() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let cfg = Config::load_or_create(dir.path()).expect("config");
        let registry = InMemoryRegistry::new();
        let node = Node::start(dir.path(), cfg, registry)
            .await
            .expect("node start");
        let server = DaemonServer::bind(node, 0).await.expect("bind");
        let port = server.local_port();
        let handle = tokio::spawn(async move { server.run().await.expect("daemon server run") });
        TestDaemon {
            port,
            client: DaemonClient::new(port),
            handle,
            _dir: dir,
        }
    }

    pub async fn shutdown(self) {
        let TestDaemon {
            client,
            handle,
            _dir,
            port: _,
        } = self;
        client.run(Op::Shutdown).await.expect("shutdown op");
        handle.await.expect("server task");
    }
}

/// Behavioural contract exercised against every [`TestDaemon`] variant.
///
/// Tests: node_id, blob_list on empty store, ring_new + ring_list, duplicate
/// ring name error, and clean shutdown. Both the redb and in-memory daemon
/// must pass this contract.
pub async fn daemon_contract(daemon: TestDaemon) {
    // node_id returns exactly one non-empty line
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(Op::NodeId { qr_code: false }, |event| {
            if let EventKind::Line { text } = event.kind {
                lines.push(text);
            }
        })
        .await
        .unwrap();
    assert_eq!(lines.len(), 1, "NodeId: expected exactly one line");
    assert!(!lines[0].is_empty(), "NodeId: must not be empty");

    // blob_list on an empty store
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(
            Op::BlobList {
                peer: None,
                rings: None,
            },
            |event| {
                if let EventKind::Line { text } = event.kind {
                    lines.push(text);
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(lines, vec!["No blobs in local store."]);

    // ring_new creates a ring visible in ring_list
    daemon
        .client
        .run(Op::RingNew {
            name: "contract_ring".into(),
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
        lines.iter().any(|l| l.contains("contract_ring")),
        "RingList should include the newly created ring; got: {lines:?}"
    );

    // duplicate ring name returns an error mentioning the name
    let err = daemon
        .client
        .run(Op::RingNew {
            name: "contract_ring".into(),
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("contract_ring"),
        "duplicate ring error should mention the ring name; got: {err}"
    );

    // shutdown stops the server
    let TestDaemon {
        port,
        client,
        handle,
        ..
    } = daemon;
    client.run(Op::Shutdown).await.unwrap();
    handle.await.unwrap();
    assert!(
        !DaemonClient::new(port).is_running().await,
        "server should no longer be reachable after shutdown"
    );
}

pub async fn write_file(dir: &Path, name: &str, content: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    fs::write(&path, content).await.expect("write test file");
    path
}
