use std::path::Path;

use ringdrop::config::Config;
use ringdrop::core::Node;
use ringdrop::registry::RedbRegistry;
use tempfile::TempDir;
use tokio::fs;

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

pub async fn write_file(dir: &Path, name: &str, content: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    fs::write(&path, content).await.expect("write test file");
    path
}
