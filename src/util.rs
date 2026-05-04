use std::path::PathBuf;

use anyhow::Result;
use iroh::EndpointId;
use iroh_blobs::Hash;

pub fn default_data_dir() -> PathBuf {
    dirs_next::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".ringdrop")
}

pub fn parse_peer_id(s: &str) -> Result<EndpointId> {
    s.parse()
        .map_err(|e| anyhow::anyhow!("invalid peer id: {e}"))
}

pub fn parse_hash(s: &str) -> Result<Hash> {
    s.parse().map_err(|e| anyhow::anyhow!("invalid hash: {e}"))
}
