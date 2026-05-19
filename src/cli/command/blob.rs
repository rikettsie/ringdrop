use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::daemon::protocol::Op;

use super::BlobCmd;

pub(crate) async fn run_import(
    path: PathBuf,
    rings: Vec<String>,
    open: bool,
    data_dir: &Path,
) -> Result<()> {
    super::daemon_client(data_dir)?
        .run(Op::Import { path, rings, open })
        .await
}

pub(crate) async fn run(cmd: BlobCmd, data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;
    match cmd {
        BlobCmd::Import { path, rings, open } => client.run(Op::Import { path, rings, open }).await,
        BlobCmd::Remove { target } => client.run(Op::BlobRemove { target }).await,
        BlobCmd::List => client.run(Op::BlobList).await,
    }
}
