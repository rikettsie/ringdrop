use std::path::Path;

use anyhow::{Context, Result};

use crate::config::Config;
use crate::core::Node;
use crate::registry::{RedbRegistry, Registry, OPEN_RING_NAME};
use crate::util::parse_hash;

use super::import_path;

async fn resolve_target(target: &str, data_dir: &Path) -> Result<(iroh_blobs::Hash, RedbRegistry)> {
    let path = std::path::PathBuf::from(target);
    if path.exists() {
        let cfg = Config::load_or_create(data_dir).context("loading config")?;
        let registry =
            RedbRegistry::open(data_dir.join("registry.redb")).context("opening registry")?;
        let node = Node::start(data_dir, cfg, registry).await?;
        let (hash, _) = import_path(&node, &path).await?;
        let registry = node.registry.clone();
        node.shutdown().await?;
        Ok((hash, registry))
    } else {
        let hash = parse_hash(target)?;
        let registry = RedbRegistry::open(data_dir.join("registry.redb"))?;
        Ok((hash, registry))
    }
}

pub async fn run_tag(
    target: String,
    rings: Vec<String>,
    open: bool,
    data_dir: &Path,
) -> Result<()> {
    tokio::fs::create_dir_all(data_dir).await?;
    let (hash, registry) = resolve_target(&target, data_dir).await?;

    for ring in &rings {
        registry.add_ring_to_prop(hash, ring)?;
        println!("Tagged {hash} with ring '{ring}'");
    }
    if open {
        registry.add_ring_to_prop(hash, OPEN_RING_NAME)?;
        println!("Tagged {hash} as open (publicly accessible)");
    }
    Ok(())
}

pub async fn run_tags(target: String, data_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(data_dir).await?;
    let (hash, registry) = resolve_target(&target, data_dir).await?;

    let rings = registry.list_prop_rings(hash)?;
    if rings.is_empty() {
        println!("{hash}: no rings (access denied to all peers)");
    } else {
        println!("{}: {} rings:", hash, rings.len());
        for ring in &rings {
            if ring.is_open() {
                println!("  {}  (open — publicly accessible)", ring.as_str());
            } else {
                println!("  {}", ring.as_str());
            }
        }
    }
    Ok(())
}
