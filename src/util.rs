//! Shared CLI/daemon utilities: default paths, argument parsers, and display helpers.

use std::path::PathBuf;

use anyhow::Result;
use iroh::{EndpointAddr, EndpointId};
use iroh_blobs::Hash;

use crate::core::peers::PeerStore;

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

/// Strip direct IP addresses from an endpoint address, keeping only relay URLs and the node ID.
///
/// Tickets and catalog entries use relay-only addresses so they remain valid
/// across daemon restarts and IP changes. iroh still negotiates a direct
/// connection via hole-punching during the relay handshake when both peers
/// are on the same LAN.
pub(crate) fn relay_only_addr(full: EndpointAddr) -> EndpointAddr {
    full.relay_urls()
        .fold(EndpointAddr::new(full.id), |a, url| {
            a.with_relay_url(url.clone())
        })
}

/// Format a peer ID with an optional nickname into a display string.
///
/// Returns `"peer_id  (nickname)"` when a nickname is provided, or the peer ID
/// alone when `nick` is `None`.
pub(crate) fn format_peer_entry(peer: &EndpointId, nick: Option<&str>) -> String {
    match nick {
        Some(n) => format!("{peer}  ({n})"),
        None => peer.to_string(),
    }
}

/// Format a peer for display, resolving its nickname from the peer store.
///
/// Delegates to [`format_peer_entry`] after looking up the nickname. Silently
/// falls back to the raw ID on store read errors.
pub(crate) fn display_peer(peer: &EndpointId, store: &PeerStore) -> String {
    let nick = store.get(peer).ok().flatten().flatten();
    format_peer_entry(peer, nick.as_deref())
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

#[cfg(test)]
mod tests {
    use iroh::SecretKey;
    use iroh_blobs::Hash;

    use super::*;

    #[test]
    fn parse_peer_id_accepts_valid_key_string() {
        let id = SecretKey::generate().public();
        let s = id.to_string();
        assert_eq!(parse_peer_id(&s).unwrap(), id);
    }

    #[test]
    fn parse_peer_id_rejects_garbage() {
        let err = parse_peer_id("not-a-valid-peer-id").unwrap_err();
        assert!(err.to_string().contains("invalid peer id"));
    }

    #[test]
    fn parse_hash_accepts_valid_hex() {
        let hash = Hash::from_bytes([0x42; 32]);
        let hex = hash.to_string();
        assert_eq!(parse_hash(&hex).unwrap(), hash);
    }

    #[test]
    fn parse_hash_rejects_invalid_hex_chars() {
        // 64-char input triggers hex decoding; 'z' is not a hex digit → Err
        let err = parse_hash(&"z".repeat(64)).unwrap_err();
        assert!(err.to_string().contains("invalid hash"));
    }

    #[test]
    fn relay_only_addr_preserves_node_id_when_no_relay() {
        let id = SecretKey::generate().public();
        let addr = EndpointAddr::new(id);
        let result = relay_only_addr(addr);
        assert_eq!(result.id, id);
    }
}
