use std::path::Path;

use anyhow::{Context, Result};

use crate::config::Config;
use crate::core::Node;
use crate::registry::RedbRegistry;

pub async fn run(data_dir: &Path) -> Result<()> {
    let cfg = Config::load_or_create(data_dir).context("loading config")?;
    let public_id = cfg.public_id();
    let registry =
        RedbRegistry::open(data_dir.join("registry.redb")).context("opening registry")?;
    let node = Node::start(data_dir, cfg, registry).await?;
    println!("Node online. Peer ID: {public_id}");
    println!("Sharing all authorised blobs — Ctrl-C to stop.");
    tokio::signal::ctrl_c().await?;
    node.shutdown().await
}
