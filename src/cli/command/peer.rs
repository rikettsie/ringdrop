use std::path::Path;

use anyhow::Result;

use crate::daemon::protocol::Op;

use super::PeerCmd;

pub(crate) async fn run(cmd: PeerCmd, data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;
    let op = match cmd {
        PeerCmd::Add { peer, nickname } => Op::PeerAdd { peer, nickname },
        PeerCmd::List => Op::PeerList,
        PeerCmd::Remove { peer } => Op::PeerRemove { peer },
    };
    client.run(op).await
}
