use std::path::Path;

use anyhow::Result;

use crate::daemon::protocol::Op;

use super::RingCmd;

pub async fn run(cmd: RingCmd, data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;
    let op = match cmd {
        RingCmd::New { name } => Op::RingNew { name },
        RingCmd::List => Op::RingList,
        RingCmd::Add {
            ring,
            peer,
            nickname,
        } => Op::RingAdd {
            ring,
            peer,
            nickname,
        },
        RingCmd::Remove { ring, peer } => Op::RingRemove { ring, peer },
        RingCmd::Members { ring } => Op::RingMembers { ring },
    };
    client.run(op).await
}
