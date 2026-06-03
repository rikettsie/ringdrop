//! `rdrop` — ringdrop CLI
//!
//! # Usage
//!
//! ```text
//! # Manage the background daemon
//! rdrop daemon start                   # start daemon in the background
//! rdrop daemon stop                    # stop the running daemon
//! rdrop daemon status                  # show daemon status and node ID
//!
//! # Print your peer-id so others can add you to their rings
//! rdrop id
//!
//! # Manage rings
//! rdrop ring new friends               # create a ring named "friends"
//! rdrop ring list                      # list all rings
//! rdrop ring add friends <peer-id>     # add a peer to a ring (auto-registers in address book)
//! rdrop ring members friends
//!
//! # Manage the local peer address book
//! rdrop peer add <peer-id>                    # register a peer
//! rdrop peer add <peer-id> --nickname alice   # register with a nickname (idempotent; re-run to rename)
//! rdrop peer list                             # list all known peers
//! rdrop peer remove <peer-id>                 # remove peer from address book, rings, and grants
//!
//! # Import a file and get a ticket (shortcut)
//! rdrop import file.txt                       # untagged — warns until tagged
//! rdrop import file.txt --open                # publicly accessible
//! rdrop import file.txt --ring friends        # restrict to a ring
//! rdrop import file.txt --ring friends --ring work  # multiple rings
//!
//! # Manage blobs
//! rdrop blob import file.txt --ring friends
//! rdrop blob list
//! rdrop blob remove file.txt
//! rdrop blob remove <hash>
//!
//! # Re-tag or untag a blob at any time
//! rdrop tag file.txt --ring friends    # associate with a ring
//! rdrop tag <hash> --open              # associate wth the public ring
//! rdrop untag file.txt --ring friends  # remove one ring association
//! rdrop untag <hash> --open            # revoke public ring association
//! rdrop untag <hash> --all             # revoke all ring associations
//!
//! # Receive — resumes automatically if interrupted
//! rdrop receive rdrop://ABCDEF... [--dest ./downloads]
//!
//! # Inspect a ticket without downloading
//! rdrop info rdrop://ABCDEF...
//!
//! # Manage catalog access grants
//! rdrop grant add <peer-id> <privilege>           # e.g. blob-list
//! rdrop grant remove <peer-id> <privilege>
//! rdrop grant list [--peer <peer-id>] [--privilege <privilege>]
//!
//! # Query remote nodes
//! rdrop remote blob-list <peer-id>                # list blobs accessible to you on a remote node
//! ```

mod command;

use std::path::PathBuf;

const ABOUT: &str = "P2P streamed file transfer with ring-based access control.\n\
                     Built on iroh and bao protocols.";

const LONG_ABOUT: &str = concat!(
    "P2P streamed file transfer with ring-based access control.\n",
    "Built on iroh and bao protocols.\n\n",
    "Full CLI reference: https://github.com/rikettsie/ringdrop/blob/v",
    env!("CARGO_PKG_VERSION"),
    "/docs/cli.md"
);

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};

use crate::util::default_data_dir;
use command::{Cmd, DaemonCmd};

#[derive(Parser)]
#[command(
    name = "rdrop",
    about = ABOUT,
    long_about = LONG_ABOUT,
    version
)]
struct Cli {
    /// Directory for blob store + registry (default: ~/.ringdrop)
    #[arg(long, env = "RINGDROP_DATA_DIR")]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Cmd,
}

/// Parses CLI arguments and dispatches the requested command to the daemon.
///
/// # Errors
///
/// Returns an error if the command fails (connection refused, daemon not
/// running, invalid arguments, etc.).
pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    let default_filter = if matches!(cli.command, Cmd::Daemon(DaemonCmd::Run)) {
        "ringdrop=info,iroh_rings=info"
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
        Cmd::Ring(cmd) => command::ring::run(cmd, &data_dir).await?,
        Cmd::Peer(cmd) => command::peer::run(cmd, &data_dir).await?,
        Cmd::Blob(cmd) => command::blob::run(cmd, &data_dir).await?,
        Cmd::Import { path, rings, open } => {
            command::blob::run_import(path, rings, open, &data_dir).await?;
        }
        Cmd::Daemon(cmd) => match cmd {
            DaemonCmd::Start => command::daemon::run_start(&data_dir).await?,
            DaemonCmd::Stop => command::daemon::run_stop(&data_dir).await?,
            DaemonCmd::Status => command::daemon::run_status(&data_dir).await?,
            DaemonCmd::Run => command::daemon::run_serve(&data_dir).await?,
        },
        Cmd::Receive {
            ticket,
            dest,
            force_overwrite,
        } => {
            command::receive::run(&ticket, dest, force_overwrite, &data_dir).await?;
        }
        Cmd::Tag {
            target,
            rings,
            open,
        } => {
            command::tag::run_tag(target, rings, open, &data_dir).await?;
        }
        Cmd::Untag {
            target,
            rings,
            open,
            all,
        } => {
            command::tag::run_untag(target, rings, open, all, &data_dir).await?;
        }
        Cmd::Grant(cmd) => command::grant::run(cmd, &data_dir).await?,
        Cmd::Remote(cmd) => command::remote::run(cmd, &data_dir).await?,
        Cmd::Id => command::id::run(&data_dir).await?,
        Cmd::Info { ticket } => command::info::run(&ticket)?,
    }

    Ok(())
}
