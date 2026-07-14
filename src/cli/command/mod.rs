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
pub(super) mod info;
pub(super) mod peer;
pub(super) mod receive;
pub(super) mod remote;
pub(super) mod ring;

#[derive(Subcommand)]
pub(super) enum Cmd {
    /// Manage rings
    #[command(subcommand)]
    Ring(RingCmd),

    /// Manage the local peer address book
    #[command(subcommand)]
    Peer(PeerCmd),

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

    /// Manage catalog access grants (control who can query your blob list)
    #[command(subcommand)]
    Grant(GrantCmd),

    /// Query remote nodes
    #[command(subcommand)]
    Remote(RemoteCmd),

    /// Print your peer-id (i.e. this node public-id) so others can add you to their rings
    Id {
        /// Show the ASCII QR-code below the peer-id
        #[arg(long)]
        qr_code: bool,
    },
    /// Decode a ticket and display its fields (hash, peer, relays, format, name)
    Info {
        /// Ticket string (rdrop://...)
        #[arg(value_name = "TICKET")]
        ticket: String,
    },
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

    /// Remove a blob from the local store and all its ring associations
    Remove {
        /// File path or BLAKE3 hash (hex)
        target: String,
    },

    /// Attach a blob to one or more rings (grants access to ring members)
    #[command(group(ArgGroup::new("access").required(true).args(["rings", "open"])))]
    Attach {
        /// Path (file or directory) or BLAKE3 hash (hex)
        target: String,

        /// Ring name(s) to attach to (repeat for multiple)
        #[arg(value_name = "RING")]
        rings: Vec<String>,

        /// Attach as publicly accessible (anyone can download)
        #[arg(long, conflicts_with = "rings")]
        open: bool,
    },

    /// Detach a blob from one or more rings (revokes access)
    #[command(group(ArgGroup::new("access").required(true).args(["rings", "open", "all"])))]
    Detach {
        /// Path (file or directory) or BLAKE3 hash (hex)
        target: String,

        /// Ring name(s) to detach from (repeat for multiple)
        #[arg(value_name = "RING")]
        rings: Vec<String>,

        /// Remove the open-ring association (revoke public access)
        #[arg(long, conflicts_with_all = ["rings", "all"])]
        open: bool,

        /// Remove all ring associations (blob becomes inaccessible)
        #[arg(long, conflicts_with_all = ["rings", "open"])]
        all: bool,
    },

    /// List all local blobs with their ring associations and share ticket
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

    /// Add a peer to a ring (registers the peer in the address book if not already present)
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

#[derive(Subcommand)]
pub(super) enum PeerCmd {
    /// Register a peer in the local address book, optionally with a nickname
    Add {
        /// Base32 peer-id to register
        #[arg(value_name = "PEER-ID")]
        peer: String,
        /// Human-readable label for this peer
        #[arg(long)]
        nickname: Option<String>,
    },

    /// List all peers in the local address book
    List,

    /// Remove a peer from the address book and from all rings
    Remove {
        /// Base32 peer-id to remove
        #[arg(value_name = "PEER-ID")]
        peer: String,
    },
}
