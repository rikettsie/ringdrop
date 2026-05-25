//! Handlers for [`Op::Grant`], [`Op::Revoke`], and [`Op::Grants`].
//!
//! [`Op::Grant`]: crate::daemon::protocol::Op::Grant
//! [`Op::Revoke`]: crate::daemon::protocol::Op::Revoke
//! [`Op::Grants`]: crate::daemon::protocol::Op::Grants

use anyhow::Result;
use iroh_rings::Registry;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::grants::{GrantStore, Privilege};
use crate::core::Node;
use crate::daemon::protocol::Event;
use crate::util::parse_peer_id;

use super::send;

pub(crate) async fn handle_grant<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    peer: String,
    privilege: String,
) -> Result<()> {
    let lines = grant_lines(&node.grants, &peer, &privilege)?;
    for line in lines {
        send(tx, Event::line(req_id, line)).await;
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}

pub(crate) async fn handle_revoke<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    peer: String,
    privilege: String,
) -> Result<()> {
    let lines = revoke_lines(&node.grants, &peer, &privilege)?;
    for line in lines {
        send(tx, Event::line(req_id, line)).await;
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}

pub(crate) async fn handle_grants<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    peer: Option<String>,
    privilege: Option<String>,
) -> Result<()> {
    let lines = filtered_grant_lines(&node.grants, peer.as_deref(), privilege.as_deref())?;
    for line in lines {
        send(tx, Event::line(req_id, line)).await;
    }
    send(tx, Event::done(req_id)).await;
    Ok(())
}

fn grant_lines(grants: &GrantStore, peer_str: &str, privilege_str: &str) -> Result<Vec<String>> {
    let peer_id = parse_peer_id(peer_str)?;
    let priv_ = Privilege::try_from(privilege_str)?;
    grants.grant(priv_, peer_id)?;
    Ok(vec![format!("Granted {privilege_str} to {peer_id}")])
}

fn revoke_lines(grants: &GrantStore, peer_str: &str, privilege_str: &str) -> Result<Vec<String>> {
    let peer_id = parse_peer_id(peer_str)?;
    let priv_ = Privilege::try_from(privilege_str)?;
    grants.revoke(priv_, peer_id)?;
    Ok(vec![format!("Revoked {privilege_str} from {peer_id}")])
}

fn filtered_grant_lines(
    grants: &GrantStore,
    peer_filter: Option<&str>,
    privilege_filter: Option<&str>,
) -> Result<Vec<String>> {
    let peer_id_filter = peer_filter.map(parse_peer_id).transpose()?;
    let priv_filter = privilege_filter.map(Privilege::try_from).transpose()?;

    let matching: Vec<_> = grants
        .list()?
        .into_iter()
        .filter(|(priv_, peer)| {
            peer_id_filter.is_none_or(|f| *peer == f) && priv_filter.is_none_or(|f| *priv_ == f)
        })
        .collect();

    if matching.is_empty() {
        return Ok(vec!["No grants.".to_owned()]);
    }
    let mut out = vec![format!("{} grants:", matching.len())];
    for (priv_, peer) in matching {
        out.push(format!("  {}  {}", priv_, peer));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    use tempfile::TempDir;

    fn open_grants() -> (GrantStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let gs = GrantStore::open(dir.path().join("grants.redb")).unwrap();
        (gs, dir)
    }

    fn peer_str() -> (iroh::EndpointId, String) {
        let id = SecretKey::generate().public();
        (id, id.to_string())
    }

    #[test]
    fn grant_lines_records_grant_and_returns_confirmation() {
        let (gs, _dir) = open_grants();
        let (id, s) = peer_str();
        let lines = grant_lines(&gs, &s, "blob-list").unwrap();
        assert_eq!(lines, vec![format!("Granted blob-list to {id}")]);
        assert!(gs.has_grant(Privilege::BlobList, &id).unwrap());
    }

    #[test]
    fn revoke_lines_removes_grant_and_returns_confirmation() {
        let (gs, _dir) = open_grants();
        let (id, s) = peer_str();
        gs.grant(Privilege::BlobList, id).unwrap();
        let lines = revoke_lines(&gs, &s, "blob-list").unwrap();
        assert_eq!(lines, vec![format!("Revoked blob-list from {id}")]);
        assert!(!gs.has_grant(Privilege::BlobList, &id).unwrap());
    }

    #[test]
    fn filtered_grant_lines_on_empty_store_returns_no_grants_message() {
        let (gs, _dir) = open_grants();
        let lines = filtered_grant_lines(&gs, None, None).unwrap();
        assert_eq!(lines, vec!["No grants.".to_owned()]);
    }

    #[test]
    fn filtered_grant_lines_returns_count_and_one_line_per_grant() {
        let (gs, _dir) = open_grants();
        let (id, _) = peer_str();
        gs.grant(Privilege::BlobList, id).unwrap();
        let lines = filtered_grant_lines(&gs, None, None).unwrap();
        assert_eq!(lines.len(), 2, "header + one entry");
        assert!(lines[0].contains("1 grants:"));
        assert!(lines[1].contains("blob-list"));
        assert!(lines[1].contains(&id.to_string()));
    }

    #[test]
    fn filtered_grant_lines_filters_by_peer() {
        let (gs, _dir) = open_grants();
        let (id1, s1) = peer_str();
        let (id2, _) = peer_str();
        gs.grant(Privilege::BlobList, id1).unwrap();
        gs.grant(Privilege::BlobList, id2).unwrap();
        let lines = filtered_grant_lines(&gs, Some(&s1), None).unwrap();
        assert_eq!(lines.len(), 2, "header + one entry");
        assert!(lines[1].contains(&id1.to_string()));
        assert!(!lines[1].contains(&id2.to_string()));
    }

    #[test]
    fn filtered_grant_lines_no_match_returns_no_grants_message() {
        let (gs, _dir) = open_grants();
        let (_, s) = peer_str();
        let lines = filtered_grant_lines(&gs, Some(&s), None).unwrap();
        assert_eq!(lines, vec!["No grants.".to_owned()]);
    }

    #[test]
    fn grant_lines_rejects_unknown_privilege() {
        let (gs, _dir) = open_grants();
        let (_, s) = peer_str();
        assert!(grant_lines(&gs, &s, "superuser").is_err());
    }

    #[test]
    fn filtered_grant_lines_filters_by_privilege() {
        let (gs, _dir) = open_grants();
        let (id, _) = peer_str();
        gs.grant(Privilege::BlobList, id).unwrap();
        let lines = filtered_grant_lines(&gs, None, Some("blob-list")).unwrap();
        assert_eq!(lines.len(), 2, "header + one entry");
        assert!(lines[1].contains("blob-list"));
        assert!(lines[1].contains(&id.to_string()));
    }

    #[test]
    fn filtered_grant_lines_and_filter_requires_both_conditions_to_match() {
        let (gs, _dir) = open_grants();
        let (id1, s1) = peer_str();
        let (id2, s2) = peer_str();
        gs.grant(Privilege::BlobList, id1).unwrap();
        gs.grant(Privilege::BlobList, id2).unwrap();
        // peer1 + blob-list: matches exactly one entry
        let lines = filtered_grant_lines(&gs, Some(&s1), Some("blob-list")).unwrap();
        assert_eq!(lines.len(), 2, "header + one entry");
        assert!(lines[1].contains(&id1.to_string()));
        assert!(!lines[1].contains(&id2.to_string()));
        // peer2 + blob-list: matches the other entry only
        let lines = filtered_grant_lines(&gs, Some(&s2), Some("blob-list")).unwrap();
        assert_eq!(lines.len(), 2, "header + one entry");
        assert!(lines[1].contains(&id2.to_string()));
        assert!(!lines[1].contains(&id1.to_string()));
    }
}
