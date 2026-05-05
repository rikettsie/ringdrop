//! `rdrop` — ringdrop CLI
//!
//! # Usage
//!
//! ```text
//! # Print your PeerId so others can add you to their rings
//! rdrop id
//!
//! # Manage rings
//! rdrop ring new friends               # create a ring named "friends"
//! rdrop ring list                      # list all rings
//! rdrop ring add friends <peer-id>     # add a peer to a ring
//! rdrop ring members friends
//!
//! # Import a file and get a ticket (shortcut)
//! rdrop import file.txt                   # untagged — warns until tagged
//! rdrop import file.txt --open            # publicly accessible
//! rdrop import file.txt --tag friends     # restrict to a ring
//!
//! # Manage blobs
//! rdrop blob import file.txt --open
//! rdrop blob list
//! rdrop blob remove file.txt
//! rdrop blob remove <hash>
//!
//! # Re-tag a blob at any time
//! rdrop tag file.txt --ring friends
//! rdrop tag <hash> --open
//!
//! # Start serving all authorised blobs
//! rdrop serve
//!
//! # Receive — resumes automatically if interrupted
//! rdrop receive rdrop://ABCDEF... [--dest ./downloads]
//! ```

mod command;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use iroh_blobs::{BlobFormat, Hash};
use tracing_subscriber::{fmt, EnvFilter};

use crate::config::Config;
use crate::core::Node;
use crate::registry::{Registry, OPEN_RING_NAME};
use crate::ticket::ShareTicket;
use crate::util::{default_data_dir, parse_hash};
use command::{BlobCmd, Cmd};

#[derive(Parser)]
#[command(
    name = "rdrop",
    about = "P2P file transfer with ring-based access control\n\
             Powered by iroh-blobs BLAKE3 verified streaming with crash-safe resumption",
    version
)]
struct Cli {
    /// Directory for blob store + registry (default: ~/.ringdrop)
    #[arg(long, env = "RINGDROP_DATA_DIR")]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Cmd,
}

async fn import_path(node: &Node, path: &std::path::Path) -> Result<(Hash, BlobFormat)> {
    if path.is_dir() {
        node.import_directory(path).await
    } else {
        node.import_file(path).await
    }
}

async fn resolve_target(target: &str, data_dir: &std::path::Path) -> Result<(Hash, Registry)> {
    tokio::fs::create_dir_all(data_dir).await?;
    let path = PathBuf::from(target);
    if path.exists() {
        let node = Node::start(data_dir).await?;
        let (hash, _) = import_path(&node, &path).await?;
        let registry = node.registry.clone();
        node.shutdown().await?;
        Ok((hash, registry))
    } else {
        let hash = parse_hash(target)?;
        let cfg = Config::load_or_create(data_dir).context("loading config")?;
        let registry = Registry::open(data_dir.join("registry.redb"), cfg.secret_key.public())?;
        Ok((hash, registry))
    }
}

async fn run_import(node: &Node, path: PathBuf, tag: Option<String>, open: bool) -> Result<()> {
    let (hash, format) = import_path(node, &path).await?;

    let tag = if open {
        Some(OPEN_RING_NAME.to_owned())
    } else {
        tag
    };

    if let Some(ref ring) = tag {
        node.registry.tag_file(hash, ring)?;
        if ring == OPEN_RING_NAME {
            println!("Tagged as open (publicly accessible)");
        } else {
            println!("Tagged with ring '{ring}'");
        }
    } else {
        let rings = node.registry.file_rings(hash)?;
        if rings.is_empty() {
            println!("Warning: not tagged — this blob won't be served to any peer.");
            println!("Tag it with:");
            println!("  rdrop tag {hash} --ring <ring-name>");
            println!("  rdrop tag {hash} --open");
        } else {
            println!("Already tagged:");
            for r in &rings {
                if r.is_open() {
                    println!("  {} (open — publicly accessible)", r.as_str());
                } else {
                    println!("  {}", r.as_str());
                }
            }
        }
    }

    let display_name = path.file_name().map(|n| n.to_string_lossy().into_owned());
    let ticket = node.make_ticket(hash, format, display_name);
    let ticket_str = ticket.to_uri()?;

    println!();
    println!("Ticket:");
    println!("  {ticket_str}");
    println!();
    println!("Run `rdrop serve` to start accepting connections.");
    println!("Peers receive with:");
    println!("  rdrop receive {ticket_str}");

    Ok(())
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_target(false)
        .compact()
        .init();

    let data_dir = cli.data_dir.unwrap_or_else(default_data_dir);

    match cli.command {
        Cmd::Ring(ring_cmd) => {
            tokio::fs::create_dir_all(&data_dir).await?;
            let cfg = Config::load_or_create(&data_dir).context("loading config")?;
            let registry = Registry::open(data_dir.join("registry.redb"), cfg.secret_key.public())?;
            command::run_ring(ring_cmd, registry)?;
        }

        Cmd::Blob(blob_cmd) => match blob_cmd {
            BlobCmd::Import { path, tag, open } => {
                let node = Node::start(&data_dir).await?;
                run_import(&node, path, tag, open).await?;
                node.shutdown().await?;
            }

            BlobCmd::Remove { target } => {
                let node = Node::start(&data_dir).await?;
                let path = PathBuf::from(&target);
                let hash = if path.exists() {
                    let (hash, _) = import_path(&node, &path).await?;
                    hash
                } else {
                    parse_hash(&target)?
                };
                node.registry.remove_file_tags(hash)?;
                node.delete_blob(hash).await?;
                println!("Removed {hash}");
                println!("Disk space will be reclaimed on the next `rdrop serve` run.");
                node.shutdown().await?;
            }

            BlobCmd::List => {
                let node = Node::start(&data_dir).await?;
                let blobs = node.list_blobs().await?;
                if blobs.is_empty() {
                    println!("No blobs in local store.");
                } else {
                    println!("{} blob(s):", blobs.len());
                    for (hash, format) in blobs {
                        let rings = node.registry.file_rings(hash)?;
                        let ticket = node.make_ticket(hash, format, None);
                        let ticket_str = ticket.to_uri()?;
                        println!();
                        println!("  {hash}");
                        if rings.is_empty() {
                            println!("    rings:  (none — not accessible to any peer)");
                        } else {
                            let names: Vec<_> =
                                rings.iter().map(|r| r.as_str().to_owned()).collect();
                            println!("    rings:  {}", names.join(", "));
                        }
                        println!("    ticket: {ticket_str}");
                    }
                }
                node.shutdown().await?;
            }
        },

        Cmd::Import { path, tag, open } => {
            let node = Node::start(&data_dir).await?;
            run_import(&node, path, tag, open).await?;
            node.shutdown().await?;
        }

        Cmd::Serve => {
            let node = Node::start(&data_dir).await?;
            println!("Node online. Peer ID: {}", node.peer_id());
            println!("Serving all authorised blobs — Ctrl-C to stop.");
            tokio::signal::ctrl_c().await?;
            node.shutdown().await?;
        }

        Cmd::Receive { ticket, dest } => {
            let ticket = ShareTicket::from_uri(&ticket)?;
            let node = Node::start(&data_dir).await?;

            println!(
                "Fetching {} from {}{}",
                ticket.hash(),
                ticket.peer_id(),
                ticket
                    .name
                    .as_deref()
                    .map(|n| format!(" ({n})"))
                    .unwrap_or_default()
            );
            println!("Destination: {}", dest.display());
            println!("(If interrupted, re-run this command to resume from where it stopped.)");

            match node.download(&ticket, &dest).await {
                Ok(()) => println!("Transfer complete."),
                Err(e) => {
                    eprintln!("Transfer failed: {e:#}");
                    if e.to_string().contains("access denied") {
                        eprintln!();
                        eprintln!("Your PeerId: {}", node.peer_id());
                        eprintln!("Ask the file owner to run:");
                        eprintln!("  rdrop ring add <ring-name> {}", node.peer_id());
                    }
                    std::process::exit(1);
                }
            }

            node.shutdown().await?;
        }

        Cmd::Tag {
            target,
            rings,
            open,
        } => {
            let (hash, registry) = resolve_target(&target, &data_dir).await?;

            for ring in &rings {
                registry.tag_file(hash, ring)?;
                println!("Tagged {hash} with ring '{ring}'");
            }
            if open {
                registry.tag_file(hash, OPEN_RING_NAME)?;
                println!("Tagged {hash} as open (publicly accessible)");
            }
        }

        Cmd::Tags { target } => {
            let (hash, registry) = resolve_target(&target, &data_dir).await?;

            let rings = registry.file_rings(hash)?;
            if rings.is_empty() {
                println!("{hash}: no rings (access denied to all peers)");
            } else {
                println!("{}: {} ring(s):", hash, rings.len());
                for ring in &rings {
                    if ring.is_open() {
                        println!("  {}  (open — publicly accessible)", ring.as_str());
                    } else {
                        println!("  {}", ring.as_str());
                    }
                }
            }
        }

        Cmd::Id => {
            let node = Node::start(&data_dir).await?;
            println!("{}", node.peer_id());
            node.shutdown().await?;
        }
    }

    Ok(())
}
