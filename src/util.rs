use std::path::PathBuf;

use anyhow::{Context, Result};
use iroh::EndpointId;
use iroh_blobs::Hash;
use uuid::Uuid;

use crate::registry::{RingId, OPEN_RING_ID, OPEN_RING_NAME};

pub fn default_data_dir() -> PathBuf {
    dirs_next::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".ringdrop")
}

pub fn parse_ring_id(s: &str) -> Result<RingId> {
    if s == OPEN_RING_NAME || s == "open" {
        return Ok(OPEN_RING_ID);
    }
    Ok(RingId(Uuid::parse_str(s).context("invalid ring id (expected UUID or 'open-ring')")?))
}

pub fn parse_peer_id(s: &str) -> Result<EndpointId> {
    s.parse().map_err(|e| anyhow::anyhow!("invalid peer id: {e}"))
}

pub fn parse_hash(s: &str) -> Result<Hash> {
    s.parse().map_err(|e| anyhow::anyhow!("invalid hash: {e}"))
}
