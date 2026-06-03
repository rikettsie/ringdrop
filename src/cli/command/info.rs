//! `rdrop info` — decode and display ticket fields without contacting the daemon.

use anyhow::Result;

use crate::core::ShareTicket;

/// Decodes `ticket` and prints its fields to stdout.
///
/// # Errors
///
/// Returns an error if `ticket` is not a valid `rdrop://` URI.
pub(crate) fn run(ticket: &str) -> Result<()> {
    let t = ShareTicket::from_uri(ticket)?;
    for line in info_lines(&t) {
        println!("{line}");
    }
    Ok(())
}

fn info_lines(ticket: &ShareTicket) -> Vec<String> {
    let mut lines = Vec::new();

    lines.push(format!("hash     {}", ticket.hash()));
    lines.push(format!("peer     {}", ticket.peer_id()));

    let mut relay_iter = ticket.node_addr().relay_urls().peekable();
    if relay_iter.peek().is_none() {
        lines.push("relays   (none)".to_string());
    } else {
        let first = relay_iter.next().expect("just peeked");
        lines.push(format!("relays   {first}"));
        for url in relay_iter {
            lines.push(format!("         {url}"));
        }
    }

    lines.push(format!("format   {}", ticket.format()));
    lines.push(format!(
        "name     {}",
        ticket.name.as_deref().unwrap_or("(none)")
    ));

    lines
}

#[cfg(test)]
mod tests {
    use iroh::{EndpointAddr, RelayUrl, SecretKey};
    use iroh_blobs::Hash;

    use super::*;
    use crate::core::ShareTicket;

    fn make_addr() -> EndpointAddr {
        EndpointAddr::new(SecretKey::generate().public())
    }

    #[test]
    fn info_lines_contains_all_fields_for_full_ticket() {
        let key = SecretKey::generate();
        let peer_id = key.public();
        let relay: RelayUrl = "https://relay.example.com".parse().unwrap();
        let addr = EndpointAddr::new(peer_id).with_relay_url(relay.clone());
        let hash = Hash::from_bytes([0xab; 32]);
        let ticket = ShareTicket::new(addr, hash, Some("file.txt".into()));

        let lines = info_lines(&ticket);

        assert!(lines[0].starts_with("hash     "));
        assert!(lines[0].contains(&hash.to_string()));
        assert!(lines[1].starts_with("peer     "));
        assert!(lines[1].contains(&peer_id.to_string()));
        assert!(lines[2].starts_with("relays   "));
        assert!(lines[2].contains(relay.as_str()));
        assert_eq!(lines[3], "format   Raw");
        assert_eq!(lines[4], "name     file.txt");
    }

    #[test]
    fn info_lines_shows_none_for_absent_name_and_empty_relays() {
        let ticket = ShareTicket::new_collection(make_addr(), Hash::from_bytes([0x01; 32]), None);

        let lines = info_lines(&ticket);

        assert_eq!(lines[2], "relays   (none)");
        assert_eq!(lines[3], "format   HashSeq");
        assert_eq!(lines[4], "name     (none)");
    }

    #[test]
    fn info_lines_indents_additional_relay_urls() {
        let relay1: RelayUrl = "https://relay1.example.com".parse().unwrap();
        let relay2: RelayUrl = "https://relay2.example.com".parse().unwrap();
        let addr = EndpointAddr::new(SecretKey::generate().public())
            .with_relay_url(relay1.clone())
            .with_relay_url(relay2.clone());
        let ticket = ShareTicket::new(addr, Hash::from_bytes([0x02; 32]), None);

        let lines = info_lines(&ticket);

        assert!(lines[2].starts_with("relays   "));
        assert!(lines[3].starts_with("         "));
    }
}
