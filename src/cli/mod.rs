//! `rdrop` — ringdrop CLI
//!
//! # Usage
//!
//! ```text
//! # Print your peer-id so others can add you to their rings
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
//! rdrop share
//!
//! # Receive — resumes automatically if interrupted
//! rdrop receive rdrop://ABCDEF... [--dest ./downloads]
//! ```

mod command;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
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
    about = "P2P streamed file transfer with ring-based access control.\n\
             Built on iroh and bao protocols.",
    version
)]
struct Cli {
    /// Directory for blob store + registry (default: ~/.ringdrop)
    #[arg(long, env = "RINGDROP_DATA_DIR")]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Cmd,
}

/// Returns the final output path, or an error if it already exists and `force_overwrite` is false.
///
/// When `dest` is an existing directory the output lands at `dest/<name>` (or `dest/<hash>` when
/// the ticket carries no name). When `dest` is not an existing directory it is used as-is.
fn check_dest(
    dest: &Path,
    name: Option<&str>,
    hash_hex: &str,
    force_overwrite: bool,
) -> Result<PathBuf> {
    let expected = if dest.is_dir() {
        dest.join(name.unwrap_or(hash_hex))
    } else {
        dest.to_path_buf()
    };
    if expected.exists() && !force_overwrite {
        anyhow::bail!(
            "destination '{}' already exists; \
             use --dest to choose a different location or --force-overwrite to replace it",
            expected.display()
        );
    }
    Ok(expected)
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
        let cfg = Config::load_or_create(data_dir).context("loading config")?;
        let node = Node::start(data_dir, cfg).await?;
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

    println!("\nTicket:");
    println!("  {ticket_str}\n");
    println!("Run `rdrop share` to start accepting connections.");
    println!("Peers receive with:");
    println!("  rdrop receive {ticket_str}");

    Ok(())
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    let default_filter = if matches!(cli.command, Cmd::Share) {
        "ringdrop=info"
    } else {
        "warn"
    };

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter)),
        )
        .with_target(false)
        .compact()
        .init();

    let data_dir = cli.data_dir.unwrap_or_else(default_data_dir);

    match cli.command {
        Cmd::Ring(ring_cmd) => {
            tokio::fs::create_dir_all(&data_dir).await?;
            let cfg = Config::load_or_create(&data_dir).context("loading config")?;
            let public_id = cfg.public_id();
            let registry = Registry::open(data_dir.join("registry.redb"))?;
            command::run_ring(ring_cmd, registry, public_id)?;
        }

        Cmd::Blob(blob_cmd) => match blob_cmd {
            BlobCmd::Import { path, tag, open } => {
                let cfg = Config::load_or_create(&data_dir).context("loading config")?;
                let node = Node::start(&data_dir, cfg).await?;
                run_import(&node, path, tag, open).await?;
                node.shutdown().await?;
            }

            BlobCmd::Remove { target } => {
                let cfg = Config::load_or_create(&data_dir).context("loading config")?;
                let node = Node::start(&data_dir, cfg).await?;
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
                println!("Disk space will be reclaimed on the next `rdrop share` run.");
                node.shutdown().await?;
            }

            BlobCmd::List => {
                let cfg = Config::load_or_create(&data_dir).context("loading config")?;
                let node = Node::start(&data_dir, cfg).await?;
                let blobs = node.list_blobs().await?;
                if blobs.is_empty() {
                    println!("No blobs in local store.");
                } else {
                    println!("{} Blobs:", blobs.len());
                    for (hash, format, name) in blobs {
                        let rings = node.registry.file_rings(hash)?;
                        let ticket = node.make_ticket(hash, format, Some(name.clone()));
                        let ticket_str = ticket.to_uri()?;
                        println!("\n  {hash}  ({name})");
                        if rings.is_empty() {
                            println!("    no rings:  (inaccessible for all peers)");
                        } else {
                            let names: Vec<_> =
                                rings.iter().map(|r| r.as_str().to_owned()).collect();
                            println!("    rings:  {}", names.join(", "));
                        }
                        println!("    ticket: {ticket_str}");
                    }
                    println!("\nNote that the ticket link may change between sessions, but the blob is always uniquely identified and addressed by the protocol.");
                }
                node.shutdown().await?;
            }
        },

        Cmd::Import { path, tag, open } => {
            let cfg = Config::load_or_create(&data_dir).context("loading config")?;
            let node = Node::start(&data_dir, cfg).await?;
            run_import(&node, path, tag, open).await?;
            node.shutdown().await?;
        }

        Cmd::Share => {
            let cfg = Config::load_or_create(&data_dir).context("loading config")?;
            let public_id = cfg.public_id();
            let node = Node::start(&data_dir, cfg).await?;
            println!("Node online. Peer ID: {public_id}");
            println!("Sharing all authorised blobs — Ctrl-C to stop.");
            tokio::signal::ctrl_c().await?;
            node.shutdown().await?;
        }

        Cmd::Receive {
            ticket,
            dest,
            force_overwrite,
        } => {
            let ticket = ShareTicket::from_uri(&ticket)?;

            // Check for destination conflict before starting any network activity.
            let hash_hex = ticket.hash().to_string();
            if let Err(e) = check_dest(&dest, ticket.name.as_deref(), &hash_hex, force_overwrite) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }

            let cfg = Config::load_or_create(&data_dir).context("loading config")?;
            let public_id = cfg.public_id();
            let node = Node::start(&data_dir, cfg).await?;

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

            let pb = ProgressBar::new(0);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] \
                         {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
                    )
                    .unwrap()
                    .progress_chars("#>-"),
            );
            let on_progress = {
                let pb = pb.clone();
                move |bytes: u64, total: u64| {
                    pb.set_length(total);
                    pb.set_position(bytes);
                }
            };

            match node
                .download_with_progress(&ticket, &dest, on_progress)
                .await
            {
                Ok(()) => {
                    pb.finish_and_clear();
                    println!("Transfer complete.");
                }
                Err(e) => {
                    pb.finish_and_clear();
                    eprintln!("Transfer failed: {e:#}");
                    if e.to_string().contains("access denied") {
                        eprintln!("\nYour peer-id: {public_id}");
                        eprintln!("Ask the file owner to run:");
                        eprintln!("  rdrop ring add <ring-name> {public_id}");
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
                println!("{}: {} rings:", hash, rings.len());
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
            tokio::fs::create_dir_all(&data_dir).await?;
            let cfg = Config::load_or_create(&data_dir).context("loading config")?;
            println!("{}", cfg.public_id());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn dest_does_not_exist_is_accepted() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("output.txt");
        assert!(check_dest(&dest, Some("output.txt"), "deadbeef", false).is_ok());
    }

    #[test]
    fn existing_dest_without_force_overwrite_is_rejected() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("output.txt");
        std::fs::write(&dest, b"old").unwrap();
        let err = check_dest(&dest, Some("output.txt"), "deadbeef", false).unwrap_err();
        assert!(err.to_string().contains("already exists"));
        assert!(err.to_string().contains("--force-overwrite"));
    }

    #[test]
    fn existing_dest_with_force_overwrite_is_accepted() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("output.txt");
        std::fs::write(&dest, b"old").unwrap();
        assert!(check_dest(&dest, Some("output.txt"), "deadbeef", true).is_ok());
    }

    #[test]
    fn dest_is_dir_and_named_file_does_not_exist_is_accepted() {
        let dir = TempDir::new().unwrap();
        let result = check_dest(dir.path(), Some("fox.txt"), "deadbeef", false).unwrap();
        assert_eq!(result, dir.path().join("fox.txt"));
    }

    #[test]
    fn dest_is_dir_and_named_file_exists_without_force_is_rejected() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("fox.txt"), b"old").unwrap();
        let err = check_dest(dir.path(), Some("fox.txt"), "deadbeef", false).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn dest_is_dir_and_named_file_exists_with_force_is_accepted() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("fox.txt"), b"old").unwrap();
        assert!(check_dest(dir.path(), Some("fox.txt"), "deadbeef", true).is_ok());
    }

    #[test]
    fn dest_is_dir_and_no_ticket_name_falls_back_to_hash() {
        let dir = TempDir::new().unwrap();
        let hash_hex = "abc123";
        std::fs::write(dir.path().join(hash_hex), b"old").unwrap();
        let err = check_dest(dir.path(), None, hash_hex, false).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }
}
