use std::path::Path;

use anyhow::Result;

use crate::daemon::protocol::Op;

use super::GrantCmd;

pub(crate) async fn run(cmd: GrantCmd, data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;
    let op = match cmd {
        GrantCmd::Add { peer, privilege } => Op::Grant { peer, privilege },
        GrantCmd::Remove { peer, privilege } => Op::Revoke { peer, privilege },
        GrantCmd::List { peer, privilege } => Op::Grants { peer, privilege },
    };
    client.run(op).await
}
