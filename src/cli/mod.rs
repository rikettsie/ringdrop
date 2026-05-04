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
//! # Share a file and get a ticket
//! rdrop share file.txt
//! rdrop share file.txt --name "my report"
//!
//! # Receive — resumes automatically if interrupted
//! rdrop receive rdrop://ABCDEF... [--dest ./downloads]
//!
//! # Grant access to a shared file (by path or hash)
//! rdrop tag file.txt --ring friends
//! rdrop tag file.txt --open
//! rdrop tag <hash> --ring friends
//! rdrop tag <hash> --open
//! ```

mod command;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use iroh_blobs::{BlobFormat, Hash};

use crate::core::Node;
use crate::registry::{Registry, OPEN_RING_NAME};
use crate::ticket::ShareTicket;
use crate::util::{default_data_dir, parse_hash};
use command::Cmd;

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
        let registry = Registry::open(data_dir.join("registry.redb"))?;
        Ok((hash, registry))
    }
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = cli.data_dir.unwrap_or_else(default_data_dir);

    match cli.command {
        Cmd::Ring(ring_cmd) => {
            tokio::fs::create_dir_all(&data_dir).await?;
            let registry = Registry::open(data_dir.join("registry.redb"))?;
            command::run_ring(ring_cmd, registry)?;
        }

        Cmd::Share { path, name } => {
            let node = Node::start(&data_dir).await?;
            let (hash, format) = import_path(&node, &path).await?;

            let display_name =
                name.or_else(|| path.file_name().map(|n| n.to_string_lossy().into_owned()));
            let ticket = node.make_ticket(hash, format, display_name);
            let ticket_str = ticket.to_uri()?;

            println!();
            println!("Ticket (give this to peers):");
            println!("  {ticket_str}");
            println!();
            println!("They can receive the file with:");
            println!("  rdrop receive {ticket_str}");
            println!();
            println!("Grant access with:");
            println!("  rdrop tag {} --ring <ring-name>  # private ring", path.display());
            println!("  rdrop tag {} --open               # anyone", path.display());
            println!();
            println!("Serving… (Ctrl-C to stop)");

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
