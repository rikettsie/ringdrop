use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{ArgGroup, Subcommand};

use crate::config::Config;
use crate::daemon::client::DaemonClient;

pub(crate) fn daemon_client(data_dir: &Path) -> Result<DaemonClient> {
    let port = Config::load_or_create(data_dir)?.daemon_port;
    Ok(DaemonClient::new(port))
}

pub(super) mod blob;
pub(super) mod daemon;
pub(super) mod grant;
pub(super) mod id;
pub(super) mod receive;
pub(super) mod remote;
pub(super) mod ring;
pub(super) mod tag;

#[derive(Subcommand)]
pub(super) enum Cmd {
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

    /// Manage the background daemon (serves all authorised blobs)
    #[command(subcommand)]
    Daemon(DaemonCmd),

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

    /// Manage catalog access grants (control who can query your blob list)
    #[command(subcommand)]
    Grant(GrantCmd),

    /// Query remote nodes
    #[command(subcommand)]
    Remote(RemoteCmd),

    /// Print your peer-id (i.e. this node public-id) so others can add you to their rings
    Id,
}

#[derive(Subcommand)]
pub(super) enum DaemonCmd {
    /// Start the daemon in the background
    Start,
    /// Stop a running daemon
    Stop,
    /// Show daemon status and node ID
    Status,
    /// Run the daemon in the foreground (used internally by `start`)
    #[command(hide = true)]
    Run,
}

#[derive(Subcommand)]
pub(super) enum BlobCmd {
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
    List {
        /// Only show blobs accessible by this peer (base32 node ID)
        #[arg(long)]
        peer: Option<String>,

        /// Only show blobs tagged with this ring (repeatable, OR semantics)
        #[arg(long = "ring")]
        rings: Option<Vec<String>>,
    },
}

#[derive(Subcommand)]
pub(super) enum GrantCmd {
    /// Grant a privilege to a peer (e.g. `blob-list`)
    Add {
        /// Base32 peer-id of the peer to grant access to
        #[arg(value_name = "PEER-ID")]
        peer: String,
        /// Privilege to grant (e.g. `blob-list`)
        #[arg(value_name = "PRIVILEGE")]
        privilege: String,
    },
    /// Revoke a privilege from a peer
    Remove {
        /// Base32 peer-id of the peer to revoke access from
        #[arg(value_name = "PEER-ID")]
        peer: String,
        /// Privilege to revoke (e.g. `blob-list`)
        #[arg(value_name = "PRIVILEGE")]
        privilege: String,
    },
    /// List grants, optionally filtered (filters compound in AND)
    List {
        /// Only show grants for this peer
        #[arg(long, value_name = "PEER-ID")]
        peer: Option<String>,
        /// Only show grants for this privilege
        #[arg(long, value_name = "PRIVILEGE")]
        privilege: Option<String>,
    },
}

#[derive(Subcommand)]
pub(super) enum RemoteCmd {
    /// List blobs accessible to you on a remote node
    BlobList {
        /// Base32 peer-id of the remote node to query
        #[arg(value_name = "PEER-ID")]
        peer: String,
    },
}

#[derive(Subcommand)]
pub(super) enum RingCmd {
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
