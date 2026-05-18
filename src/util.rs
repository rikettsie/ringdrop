//! Shared CLI/daemon utilities: default paths and argument parsers.

use std::path::PathBuf;

use anyhow::Result;
use iroh::EndpointId;
use iroh_blobs::Hash;

/// Returns `~/.ringdrop`, falling back to `.ringdrop` in the current directory
/// if the home directory cannot be determined.
pub fn default_data_dir() -> PathBuf {
    dirs_next::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".ringdrop")
}

/// Parse an [`EndpointId`] from its base32 string representation.
///
/// # Errors
///
/// Returns an error if `s` is not a valid [`EndpointId`] encoding.
///
/// [`EndpointId`]: iroh::EndpointId
pub fn parse_peer_id(s: &str) -> Result<EndpointId> {
    s.parse()
        .map_err(|e| anyhow::anyhow!("invalid peer id: {e}"))
}

/// Parse a BLAKE3 [`Hash`] from its hex string representation.
///
/// # Errors
///
/// Returns an error if `s` is not a valid BLAKE3 hex hash.
///
/// [`Hash`]: iroh_blobs::Hash
pub fn parse_hash(s: &str) -> Result<Hash> {
    s.parse().map_err(|e| anyhow::anyhow!("invalid hash: {e}"))
}
