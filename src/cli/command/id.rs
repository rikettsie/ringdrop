use std::path::Path;

use anyhow::Result;

use crate::daemon::protocol::Op;

pub(crate) async fn run(qr_code: bool, data_dir: &Path) -> Result<()> {
    super::daemon_client(data_dir)?
        .run(Op::NodeId { qr_code })
        .await
}
