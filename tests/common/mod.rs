#![allow(dead_code)]

use std::path::Path;

use iroh_rings::{InMemoryRegistry, RedbRegistry};
use ringdrop::config::Config;
use ringdrop::core::Node;
use ringdrop::daemon::client::DaemonClient;
use ringdrop::daemon::protocol::Op;
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

/// Daemon backed by [`InMemoryRegistry`]: no redb file, only the blob store
/// directory lives on disk. Use this in server-layer tests that care about
/// protocol behaviour rather than storage backend correctness.
pub struct TestDaemonMem {
    pub port: u16,
    pub client: DaemonClient,
    pub handle: JoinHandle<()>,
    _dir: TempDir,
}

impl TestDaemonMem {
    pub async fn start() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let cfg = Config::load_or_create(dir.path()).expect("config");
        let registry = InMemoryRegistry::new();
        let node = Node::start(dir.path(), cfg, registry)
            .await
            .expect("node start");
        let server = DaemonServer::bind(node, 0).await.expect("bind");
        let port = server.local_port();
        let handle = tokio::spawn(async move { server.run().await.expect("daemon server run") });
        TestDaemonMem {
            port,
            client: DaemonClient::new(port),
            handle,
            _dir: dir,
        }
    }

    pub async fn shutdown(self) {
        let TestDaemonMem {
            client,
            handle,
            _dir,
            port: _,
        } = self;
        client.run(Op::Shutdown).await.expect("shutdown op");
        handle.await.expect("server task");
    }
}

pub async fn write_file(dir: &Path, name: &str, content: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    fs::write(&path, content).await.expect("write test file");
    path
}
