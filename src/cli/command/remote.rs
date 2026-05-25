use std::path::Path;

use anyhow::Result;

use crate::daemon::protocol::Op;

use super::RemoteCmd;

pub(crate) async fn run(cmd: RemoteCmd, data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;
    let op = match cmd {
        RemoteCmd::BlobList { peer } => Op::RemoteBlobList { peer },
    };
    client.run(op).await
}
