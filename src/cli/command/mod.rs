use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgGroup, Subcommand};
use iroh_blobs::{BlobFormat, Hash};

use crate::core::Node;
use crate::registry::RedbRegistry;

pub mod blob;
pub mod id;
pub mod receive;
pub mod ring;
pub mod share;
pub mod tag;

#[derive(Subcommand)]
pub enum Cmd {
    /// Manage rings
    #[command(subcommand)]
    Ring(RingCmd),

    /// Manage blobs (import, list, remove)
    #[command(subcommand)]
    Blob(BlobCmd),

    /// Import a file or directory into the blob store and print a ticket (shortcut for `blob import`)
    Import {
        /// Path to import (file or directory)
        path: PathBuf,

        /// Ring to tag the blob with (repeatable); if omitted the blob won't be downloadable until tagged
        #[arg(long = "ring", conflicts_with = "open")]
        rings: Vec<String>,

        /// Tag the blob as "publicly accessible" (anyone can download); shorthand for --ring open
        #[arg(long, conflicts_with = "rings")]
        open: bool,
    },

    /// Start the node and share all authorised blobs until Ctrl-C
    Share,

    /// Download a file from a ringdrop ticket (automatically resumes if interrupted)
    Receive {
        /// Ticket string (rdrop://...)
        ticket: String,

        /// Destination path (directory or file path)
        #[arg(long, default_value = ".")]
        dest: PathBuf,

        /// Overwrite an existing destination without warning
        #[arg(long)]
        force_overwrite: bool,
    },

    /// Grant access to a blob by tagging it with a ring
    #[command(group(ArgGroup::new("access").required(true).args(["rings", "open"])))]
    Tag {
        /// Path (file or directory) or BLAKE3 hash (hex)
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
        /// Path (file or directory) or BLAKE3 hash (hex)
        target: String,
    },

    /// Print your peer-id (i.e. this node public-id) so others can add you to their rings
    Id,
}

#[derive(Subcommand)]
pub enum BlobCmd {
    /// Import a file or directory into the blob store and print a ticket
    Import {
        /// Path to import (file or directory)
        path: PathBuf,

        /// Ring to tag the blob with (repeatable); if omitted the blob won't be served until tagged
        #[arg(long = "ring", conflicts_with = "open")]
        rings: Vec<String>,

        /// Tag the blob as publicly accessible (anyone can download); shorthand for --ring open
        #[arg(long, conflicts_with = "rings")]
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

async fn import_path(
    node: &Node<RedbRegistry>,
    path: &std::path::Path,
) -> Result<(Hash, BlobFormat)> {
    if path.is_dir() {
        node.import_directory(path).await
    } else {
        node.import_file(path).await
    }
}
