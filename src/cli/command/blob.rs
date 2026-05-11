use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::Config;
use crate::core::Node;
use crate::registry::{RedbRegistry, Registry, OPEN_RING_NAME};
use crate::util::parse_hash;

use super::{import_path, BlobCmd};

pub async fn run_import(
    path: PathBuf,
    rings: Vec<String>,
    open: bool,
    data_dir: &Path,
) -> Result<()> {
    let cfg = Config::load_or_create(data_dir).context("loading config")?;
    let registry =
        RedbRegistry::open(data_dir.join("registry.redb")).context("opening registry")?;
    let node = Node::start(data_dir, cfg, registry).await?;
    run_import_with_node(&node, path, rings, open).await?;
    node.shutdown().await
}

pub(super) async fn run_import_with_node(
    node: &Node<RedbRegistry>,
    path: PathBuf,
    rings: Vec<String>,
    open: bool,
) -> Result<()> {
    let (hash, format) = import_path(node, &path).await?;

    let effective_rings: Vec<String> = if open {
        vec![OPEN_RING_NAME.to_owned()]
    } else {
        rings
    };

    if effective_rings.is_empty() {
        let existing = node.registry.list_prop_rings(hash)?;
        if existing.is_empty() {
            println!("Warning: not tagged — this blob won't be served to any peer.");
            println!("Tag it with:");
            println!("  rdrop tag {hash} --ring <ring-name>");
            println!("  rdrop tag {hash} --open");
        } else {
            println!("Already tagged:");
            for r in &existing {
                if r.is_open() {
                    println!("  {} (open — publicly accessible)", r.as_str());
                } else {
                    println!("  {}", r.as_str());
                }
            }
        }
    } else {
        for ring in &effective_rings {
            node.registry.add_ring_to_prop(hash, ring)?;
            if ring == OPEN_RING_NAME {
                println!("Tagged as open (publicly accessible)");
            } else {
                println!("Tagged with ring '{ring}'");
            }
        }
    }

    let display_name = path.file_name().map(|n| n.to_string_lossy().into_owned());
    let ticket = node.make_ticket(hash, format, display_name);
    let ticket_str = ticket.to_uri()?;

    println!("\nTicket:");
    println!("  {ticket_str}\n");
    println!("Run `rdrop share` to start accepting connections.");
    println!("Peers receive with:");
    println!("  rdrop receive {ticket_str}");

    Ok(())
}

pub async fn run(cmd: BlobCmd, data_dir: &Path) -> Result<()> {
    match cmd {
        BlobCmd::Import { path, rings, open } => {
            run_import(path, rings, open, data_dir).await?;
        }

        BlobCmd::Remove { target } => {
            let cfg = Config::load_or_create(data_dir).context("loading config")?;
            let registry =
                RedbRegistry::open(data_dir.join("registry.redb")).context("opening registry")?;
            let node = Node::start(data_dir, cfg, registry).await?;
            let path = PathBuf::from(&target);
            let hash = if path.exists() {
                let (hash, _) = import_path(&node, &path).await?;
                hash
            } else {
                parse_hash(&target)?
            };
            node.registry.remove_ring_from_prop(hash)?;
            node.delete_blob(hash).await?;
            println!("Removed {hash}");
            println!("Disk space will be reclaimed on the next `rdrop share` run.");
            node.shutdown().await?;
        }

        BlobCmd::List => {
            let cfg = Config::load_or_create(data_dir).context("loading config")?;
            let registry =
                RedbRegistry::open(data_dir.join("registry.redb")).context("opening registry")?;
            let node = Node::start(data_dir, cfg, registry).await?;
            let blobs = node.list_blobs().await?;
            if blobs.is_empty() {
                println!("No blobs in local store.");
            } else {
                println!("{} Blobs:", blobs.len());
                for (hash, format, name) in blobs {
                    let rings = node.registry.list_prop_rings(hash)?;
                    let ticket = node.make_ticket(hash, format, Some(name.clone()));
                    let ticket_str = ticket.to_uri()?;
                    println!("\n  {hash}  ({name})");
                    if rings.is_empty() {
                        println!("    no rings:  (inaccessible for all peers)");
                    } else {
                        let names: Vec<_> = rings.iter().map(|r| r.as_str().to_owned()).collect();
                        println!("    rings:  {}", names.join(", "));
                    }
                    println!("    ticket: {ticket_str}");
                }
            }
            node.shutdown().await?;
        }
    }
    Ok(())
}
