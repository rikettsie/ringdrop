use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgGroup, Subcommand};

use crate::registry::{Registry, OPEN_RING_NAME};
use crate::util::parse_peer_id;

#[derive(Subcommand)]
pub enum Cmd {
    /// Manage rings
    #[command(subcommand)]
    Ring(RingCmd),

    /// Manage blobs (import, list, remove)
    #[command(subcommand)]
    Blob(BlobCmd),

    /// Import a file/directory into the blob store and print a ticket (shortcut for `blob import`)
    Import {
        /// Path to import (file or directory)
        path: PathBuf,
        /// Ring to tag the blob with; if omitted the blob won't be served until tagged
        #[arg(long, conflicts_with = "open")]
        tag: Option<String>,
        /// Tag as publicly accessible (anyone can download); shorthand for --tag open
        #[arg(long, conflicts_with = "tag")]
        open: bool,
    },

    /// Start the node and serve all authorised blobs until Ctrl-C
    Serve,

    /// Download a file from a ringdrop ticket (automatically resumes if interrupted)
    Receive {
        /// Ticket string (rdrop://...)
        ticket: String,
        /// Destination path (directory or file path)
        #[arg(long, default_value = ".")]
        dest: PathBuf,
    },

    /// Grant access to a file by tagging it with a ring
    #[command(group(ArgGroup::new("access").required(true).args(["rings", "open"])))]
    Tag {
        /// File path or BLAKE3 hash (hex)
        target: String,
        /// Tag with a named ring (repeat for multiple)
        #[arg(long = "ring", conflicts_with = "open")]
        rings: Vec<String>,
        /// Tag as publicly accessible (anyone can download)
        #[arg(long, conflicts_with = "rings")]
        open: bool,
    },

    /// Show which rings a file is tagged with
    Tags {
        /// File path or BLAKE3 hash (hex)
        target: String,
    },

    /// Print your PeerId so others can add you to their rings
    Id,
}

#[derive(Subcommand)]
pub enum BlobCmd {
    /// Import a file/directory into the blob store and print a ticket
    Import {
        /// Path to import (file or directory)
        path: PathBuf,
        /// Ring to tag the blob with; if omitted the blob won't be served until tagged
        #[arg(long, conflicts_with = "open")]
        tag: Option<String>,
        /// Tag as publicly accessible (anyone can download); shorthand for --tag open
        #[arg(long, conflicts_with = "tag")]
        open: bool,
    },
    /// Remove a blob from the local store and all its ring tags
    Remove {
        /// File path or BLAKE3 hash (hex)
        target: String,
    },
    /// List all local blobs with their ring tags and share ticket
    List,
}

#[derive(Subcommand)]
pub enum RingCmd {
    /// Create a new ring with the given name
    New {
        /// Name for the ring (e.g. "friends", "work-team")
        name: String,
    },
    /// List all rings
    List,
    /// Add a peer to a ring
    Add {
        ring: String,
        #[arg(value_name = "PEER-ID")]
        peer: String,
        /// Optional display label for this peer
        #[arg(long)]
        nickname: Option<String>,
    },
    /// Remove a peer from a ring
    Remove {
        ring: String,
        #[arg(value_name = "PEER-ID")]
        peer: String,
    },
    /// List members of a ring
    Members { ring: String },
}

pub fn run_ring(cmd: RingCmd, registry: Registry) -> Result<()> {
    match cmd {
        RingCmd::New { name } => {
            registry.create_ring(&name)?;
            println!("Ring created: {name}");
            println!("Add peers: rdrop ring add {name} <peer-id>");
        }
        RingCmd::List => {
            let rings = registry.list_rings()?;
            println!("{} ring(s):", rings.len());
            for r in rings {
                if r.is_open() {
                    println!(
                        "  {}  — publicly accessible (no membership required)",
                        r.as_str()
                    );
                } else {
                    let members = registry.list_members(r.as_str())?;
                    println!("  {}  ({} member(s))", r.as_str(), members.len());
                }
            }
        }
        RingCmd::Add {
            ring,
            peer,
            nickname,
        } => {
            if ring == OPEN_RING_NAME {
                println!("The open ring has no membership list — everyone is welcome.");
                return Ok(());
            }
            let peer = parse_peer_id(&peer)?;
            registry.add_member(&ring, peer, nickname.as_deref())?;
            match &nickname {
                Some(nick) => println!("Added {peer} ({nick}) to ring {ring}"),
                None => println!("Added {peer} to ring {ring}"),
            }
        }
        RingCmd::Remove { ring, peer } => {
            if ring == OPEN_RING_NAME {
                println!("The open ring has no membership list to remove from.");
                return Ok(());
            }
            let peer = parse_peer_id(&peer)?;
            registry.remove_member(&ring, peer)?;
            println!("Removed {peer} from ring {ring}");
        }
        RingCmd::Members { ring } => {
            if ring == OPEN_RING_NAME {
                println!("The open ring is public — any peer may access blobs tagged with it.");
                return Ok(());
            }
            let members = registry.list_members(&ring)?;
            if members.is_empty() {
                println!("Ring '{ring}' has no members yet.");
                println!("Add peers: rdrop ring add {ring} <peer-id>");
                println!("Peers print their PeerId with: rdrop id");
            } else {
                println!("Ring '{ring}' — {} member(s):", members.len());
                for (peer, nick) in members {
                    match nick {
                        Some(n) => println!("  {peer}  ({n})"),
                        None => println!("  {peer}"),
                    }
                }
            }
        }
    }
    Ok(())
}
