use std::path::Path;

use anyhow::Result;

use crate::daemon::protocol::Op;

pub async fn run_tag(
    target: String,
    rings: Vec<String>,
    open: bool,
    data_dir: &Path,
) -> Result<()> {
    super::daemon_client(data_dir)?
        .run(Op::Tag {
            target,
            rings,
            open,
        })
        .await
}

pub async fn run_tags(target: String, data_dir: &Path) -> Result<()> {
    super::daemon_client(data_dir)?
        .run(Op::Tags { target })
        .await
}
