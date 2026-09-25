use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::daemon::protocol::Op;

use super::RingCmd;

pub(crate) async fn run(cmd: RingCmd, data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;
    let op = match cmd {
        RingCmd::New { name } => Op::RingNew { name },
        RingCmd::List => Op::RingList,
        RingCmd::Add {
            ring,
            peer,
            expires,
        } => Op::RingAdd {
            ring,
            peer,
            expires_at: expires.map(unix_secs_after).transpose()?,
        },
        RingCmd::Remove { ring, peer } => Op::RingRemove { ring, peer },
        RingCmd::Members { ring } => Op::RingMembers { ring },
    };
    client.run(op).await
}

fn unix_secs_after(duration: Duration) -> Result<u64> {
    let at = SystemTime::now()
        .checked_add(duration)
        .context("expiry is too far in the future")?;
    Ok(at
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs())
}
