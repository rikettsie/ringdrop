use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;

use crate::registry::Registry;
use crate::util::{parse_ring_id, parse_peer_id};

#[derive(Subcommand)]
pub enum Cmd {
    /// Manage rings
    #[command(subcommand)]
    Ring(RingCmd),

    /// Import a file/directory and print a ringdrop ticket
    Share {
        /// Path to share (file or directory)
        path: PathBuf,
        /// Optional human-readable name embedded in the ticket
        #[arg(long)]
        name: Option<String>,
    },

    /// Download a file from a ringdrop ticket (automatically resumes if interrupted)
    Receive {
        /// Ticket string (rdrop://...)
        ticket: String,
        /// Destination path (directory or file path)
        #[arg(long, default_value = ".")]
        dest: PathBuf,
    },

    /// Grant access to a file by tagging it with a ring
    Tag {
        /// File path or BLAKE3 hash (hex)
        target: String,
        /// Tag with a private ring (repeat for multiple)
        #[arg(long = "ring", conflicts_with = "open")]
        rings: Vec<String>,
        /// Tag with the open-ring so anyone can download
        #[arg(long, conflicts_with = "rings")]
        open: bool,
    },

    /// Print your PeerId so others can add you to their rings
    Id,
}

#[derive(Subcommand)]
pub enum RingCmd {
    /// Create a new private ring and print its ID
    New,
    /// List all rings (open-ring is always listed first)
    List,
    /// Add a peer to a ring
    Add {
        ring: String,
        #[arg(value_name = "PEER-ID")]
        peer: String,
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
        RingCmd::New => {
            let id = registry.create_ring()?;
            println!("Ring created: {id}");
            println!("Add peers: rdrop ring add {id} <peer-id>");
        }
        RingCmd::List => {
            let rings = registry.list_rings()?;
            println!("{} ring(s):", rings.len());
            for r in rings {
                let members = registry.list_members(r)?;
                if r.is_open() {
                    println!("  {r}  — publicly accessible (no membership required)");
                } else {
                    println!("  {r}  ({} member(s))", members.len());
                }
            }
        }
        RingCmd::Add { ring, peer } => {
            let rid = parse_ring_id(&ring)?;
            if rid.is_open() {
                println!("The open-ring has no membership list — everyone is welcome.");
                return Ok(());
            }
            let peer = parse_peer_id(&peer)?;
            registry.add_member(rid, peer)?;
            println!("Added {peer} to ring {rid}");
        }
        RingCmd::Remove { ring, peer } => {
            let rid = parse_ring_id(&ring)?;
            if rid.is_open() {
                println!("The open-ring has no membership list to remove from.");
                return Ok(());
            }
            let peer = parse_peer_id(&peer)?;
            registry.remove_member(rid, peer)?;
            println!("Removed {peer} from ring {rid}");
        }
        RingCmd::Members { ring } => {
            let rid = parse_ring_id(&ring)?;
            if rid.is_open() {
                println!("The open-ring ({rid}) is public — any peer may access blobs tagged with it.");
                return Ok(());
            }
            let members = registry.list_members(rid)?;
            if members.is_empty() {
                println!("Ring {rid} has no members yet.");
                println!("Add peers: rdrop ring add {rid} <peer-id>");
                println!("Peers print their PeerId with: rdrop id");
            } else {
                println!("Ring {rid} — {} member(s):", members.len());
                for m in members {
                    println!("  {m}");
                }
            }
        }
    }
    Ok(())
}
