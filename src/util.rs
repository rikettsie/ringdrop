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

/// Parses an [`EndpointId`] from its base32 string representation.
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

/// Strips direct IP addresses from an endpoint address, keeping only relay URLs and the node ID.
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

/// Formats a peer ID with an optional nickname into a display string.
///
/// Returns `"peer_id  (nickname)"` when a nickname is provided, or the peer ID
/// alone when `nick` is `None`.
pub(crate) fn format_peer_entry(peer: &EndpointId, nick: Option<&str>) -> String {
    match nick {
        Some(n) => format!("{peer}  ({n})"),
        None => peer.to_string(),
    }
}

/// Formats a peer for display, resolving its nickname from the peer store.
///
/// Delegates to [`format_peer_entry`] after looking up the nickname. Silently
/// falls back to the raw ID on store read errors.
pub(crate) fn display_peer(peer: &EndpointId, store: &PeerStore) -> String {
    let nick = store.get(peer).ok().flatten().flatten();
    format_peer_entry(peer, nick.as_deref())
}

/// Formats a byte count as a human-readable string (B / KiB / MiB / GiB).
pub(crate) fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Prints the ringdrop startup banner with version to stdout.
///
/// The giraffe mascot is rendered in yellow on the left; the "ringdrop" text
/// in a rainbow gradient starts alongside the giraffe body. Color output
/// requires a TTY on stdout, `NO_COLOR` unset, and `TERM` not `dumb`.
pub fn print_banner() {
    use std::io::IsTerminal as _;

    const GIRAFFE: &str = concat!(
        "     .          .l'.        .     .     \n",
        "      .   . .  ..lO'..                 .\n",
        "               .,lod..   ....           \n",
        "      .        ..ooO:.....Oood...  .    \n",
        ". .          ....,0xoodxlllk...      .. \n",
        "  .     ....cdlollllllooo0.....  .    ..\n",
        "       ..,dlll . oK . oKKK:..           \n",
        "     ..clolll__l,NX__lloll0..      . .  \n",
        "  ...0olollooloooxKKKKooxlO..          .\n",
        " .ooloooKKKKXlooooXKxllKKKx..      .    \n",
        "..llxllollkKkOloKKKKKolXKKl..           \n",
        "..xddXkkkkO0olloKXXdlllloK:.. .  .   .  \n",
        "..olkkkNKoKKKlx'....ddKK0l:.       ..   \n",
        " .cxdoll0:.......  .lllodoc.      ... . \n",
        "  ......... .    . .cK0oKKl..    .....  \n",
        "         .         .,KKkKKc..   ......  \n",
        "                 . .:loxolc........... .\n",
        "      .        .   ..odKKl:..  .......  \n",
        "        .     ..   ..KXlKKO..  .......  \n",
        "                  ..'KKdlXo..  ....... .\n",
    );

    const RINGDROP: &str = concat!(
        "      _                 _                 \n",
        "     (_)               | |                \n",
        " _ __ _ _ __   __ _  __| |_ __ ___  _ __  \n",
        "| '__| | '_ \\ / _` |/ _` | '__/ _ \\| '_ \\ \n",
        "| |  | | | | | (_| | (_| | | | (_) | |_) |\n",
        "|_|  |_|_| |_|\\__, |\\__,_|_|  \\___/| .__/ \n",
        "               __/ |               | |    \n",
        "              |___/                |_|    ",
    );

    // The ringdrop text starts at this giraffe line (0-indexed), placing it
    // alongside the body rather than the neck.
    const TEXT_START: usize = 6;
    // Giraffe is capped at 38 columns so that gap=0 + ringdrop(42) = 80.
    const GIRAFFE_COLS: usize = 38;
    const GAP: &str = "";
    const YELLOW: &str = "\x1b[93m";
    const RESET: &str = "\x1b[0m";

    let version = env!("CARGO_PKG_VERSION");
    let colored = std::io::stdout().is_terminal()
        && std::env::var_os("NO_COLOR").is_none()
        && std::env::var("TERM").as_deref() != Ok("dumb");

    let giraffe_lines: Vec<&str> = GIRAFFE.lines().collect();
    let ringdrop_lines: Vec<&str> = RINGDROP.lines().collect();

    for (i, giraffe_line) in giraffe_lines.iter().enumerate() {
        let giraffe_col = giraffe_line.len().min(GIRAFFE_COLS);
        let giraffe_display = &giraffe_line[..giraffe_col];
        let text = i
            .checked_sub(TEXT_START)
            .and_then(|j| ringdrop_lines.get(j));
        if colored {
            let padded = format!("{:<GIRAFFE_COLS$}", giraffe_display.trim_end());
            match text {
                Some(t) => println!("{YELLOW}{padded}{RESET}{GAP}{}", rainbow_line(t)),
                None => println!("{YELLOW}{giraffe_display}{RESET}"),
            }
        } else {
            match text {
                Some(t) => println!("{:<GIRAFFE_COLS$}{GAP}{t}", giraffe_display.trim_end()),
                None => println!("{giraffe_display}"),
            }
        }
    }

    if colored {
        println!("\n  {YELLOW}v{version}{RESET}\n");
    } else {
        println!("\n  v{version}\n");
    }
}

/// Applies a horizontal rainbow gradient to a single line using 256-color ANSI codes.
///
/// Each column maps to a color cycling red → orange → yellow → green → cyan →
/// blue → magenta. Spaces are left uncolored so the background shows through.
fn rainbow_line(line: &str) -> String {
    const PALETTE: &[u8] = &[
        196, 202, 208, 214, 220, 226, 190, 154, 118, 82, 46, 48, 50, 51, 45, 39, 33, 27, 21, 57,
        93, 129, 165, 201,
    ];

    let mut out = String::new();
    let mut active_color: Option<u8> = None;

    for (col, ch) in line.chars().enumerate() {
        if ch == ' ' {
            out.push(' ');
        } else {
            let color = PALETTE[col % PALETTE.len()];
            if active_color != Some(color) {
                out.push_str(&format!("\x1b[38;5;{color}m"));
                active_color = Some(color);
            }
            out.push(ch);
        }
    }

    if active_color.is_some() {
        out.push_str("\x1b[0m");
    }

    out
}

/// Parses a BLAKE3 [`Hash`] from its hex string representation.
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

    #[test]
    fn relay_only_addr_preserves_relay_url() {
        use iroh::RelayUrl;
        let id = SecretKey::generate().public();
        let url: RelayUrl = "https://relay.example.com".parse().unwrap();
        let addr = EndpointAddr::new(id).with_relay_url(url.clone());
        let result = relay_only_addr(addr);
        assert_eq!(result.id, id);
        assert!(result.relay_urls().any(|u| u == &url));
    }

    #[test]
    fn format_peer_entry_without_nickname_returns_raw_id() {
        let id = SecretKey::generate().public();
        let result = format_peer_entry(&id, None);
        assert_eq!(result, id.to_string());
    }

    #[test]
    fn format_peer_entry_with_nickname_includes_nickname_in_parens() {
        let id = SecretKey::generate().public();
        let result = format_peer_entry(&id, Some("alice"));
        assert!(result.contains(&id.to_string()));
        assert!(result.contains("(alice)"));
    }
}
