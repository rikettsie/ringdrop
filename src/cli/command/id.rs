use std::path::Path;

use anyhow::Result;

use crate::daemon::protocol::Op;

pub(crate) async fn run(data_dir: &Path) -> Result<()> {
    super::daemon_client(data_dir)?.run(Op::NodeId).await
}
