mod common;

use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use ringdrop::daemon::protocol::{Event, EventKind, Op};

/// Connect to the daemon, send a raw JSON line, and return the first event.
async fn send_raw(port: u16, json: &str) -> Event {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(format!("{json}\n").as_bytes())
        .await
        .unwrap();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    serde_json::from_str(line.trim()).expect("daemon sent non-JSON")
}

#[tokio::test]
async fn daemon_contract_holds_with_redb() {
    common::daemon_contract(common::TestDaemon::start().await).await;
}

#[tokio::test]
async fn node_id_with_qr_code_emits_peer_id_then_qr_art_lines() {
    let daemon = common::TestDaemon::start().await;

    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(Op::NodeId { qr_code: true }, |event| {
            if let EventKind::Line { text } = event.kind {
                lines.push(text);
            }
        })
        .await
        .unwrap();

    assert!(
        lines.len() > 2,
        "expected the peer-id line plus a blank line and QR art lines, got {lines:?}"
    );
    assert!(!lines[0].is_empty(), "first line must be the peer-id");
    assert_eq!(lines[1], "", "second line must be a blank separator");
    assert!(
        lines[2..].iter().any(|l| !l.is_empty()),
        "remaining lines must contain rendered QR art"
    );
}

#[tokio::test]
async fn ring_add_self_is_rejected_via_daemon() {
    let daemon = common::TestDaemon::start().await;

    let mut node_id = String::new();
    daemon
        .client
        .send(Op::NodeId { qr_code: false }, |event| {
            if let EventKind::Line { text } = event.kind {
                node_id = text;
            }
        })
        .await
        .unwrap();

    daemon
        .client
        .run(Op::RingNew {
            name: "test".into(),
        })
        .await
        .unwrap();

    let err = daemon
        .client
        .run(Op::RingAdd {
            ring: "test".into(),
            peer: node_id,
        })
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("yourself"),
        "expected 'yourself' in error message; got: {err}"
    );
    daemon.shutdown().await;
}

/// Import `file` via the daemon and create `rings` beforehand.
async fn import_with_rings(daemon: &common::TestDaemon, file: &std::path::Path, rings: &[&str]) {
    for ring in rings {
        daemon
            .client
            .run(Op::RingNew {
                name: (*ring).into(),
            })
            .await
            .unwrap();
    }
    daemon
        .client
        .run(Op::Import {
            path: file.to_path_buf(),
            rings: rings.iter().map(|r| (*r).to_owned()).collect(),
            open: false,
        })
        .await
        .unwrap();
}

/// Returns the lines from `BlobList` filtered to `ring`.
async fn blob_list_for_ring(daemon: &common::TestDaemon, ring: &str) -> Vec<String> {
    let mut lines = Vec::new();
    daemon
        .client
        .send(
            Op::BlobList {
                peer: None,
                rings: Some(vec![ring.to_owned()]),
            },
            |event| {
                if let EventKind::Line { text } = event.kind {
                    lines.push(text);
                }
            },
        )
        .await
        .unwrap();
    lines
}

#[tokio::test]
async fn attach_with_no_rings_and_no_open_returns_error() {
    let daemon = common::TestDaemon::start().await;
    let err = daemon
        .client
        .run(Op::BlobAttach {
            target: "deadbeef".into(),
            rings: vec![],
            open: false,
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("nothing to attach"),
        "expected 'nothing to attach' in error; got: {err}"
    );
    daemon.shutdown().await;
}

#[tokio::test]
async fn detach_all_removes_every_ring_association() {
    let daemon = common::TestDaemon::start().await;
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "data.txt", b"content").await;

    import_with_rings(&daemon, &file, &["friends"]).await;

    let mut lines = Vec::new();
    daemon
        .client
        .send(
            Op::BlobDetach {
                target: file.to_string_lossy().into_owned(),
                rings: vec![],
                open: false,
                all: true,
            },
            |event| {
                if let EventKind::Line { text } = event.kind {
                    lines.push(text);
                }
            },
        )
        .await
        .unwrap();
    assert!(
        lines.iter().any(|l| l.contains("all rings")),
        "expected confirmation mentioning 'all rings'; got: {lines:?}"
    );

    let ring_lines = blob_list_for_ring(&daemon, "friends").await;
    assert_eq!(
        ring_lines,
        vec!["No blobs in local store."],
        "blob should no longer appear under 'friends' after detach --all"
    );

    daemon.shutdown().await;
}

#[tokio::test]
async fn detach_ring_removes_only_that_ring() {
    let daemon = common::TestDaemon::start().await;
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "data.txt", b"content").await;

    import_with_rings(&daemon, &file, &["friends", "work"]).await;

    daemon
        .client
        .run(Op::BlobDetach {
            target: file.to_string_lossy().into_owned(),
            rings: vec!["friends".into()],
            open: false,
            all: false,
        })
        .await
        .unwrap();

    let friends_lines = blob_list_for_ring(&daemon, "friends").await;
    assert_eq!(
        friends_lines,
        vec!["No blobs in local store."],
        "blob should be gone from 'friends'"
    );

    let work_lines = blob_list_for_ring(&daemon, "work").await;
    assert!(
        work_lines.iter().any(|l| l.contains("1 blobs")),
        "blob must still appear under 'work'"
    );

    daemon.shutdown().await;
}

#[tokio::test]
async fn detach_open_revokes_public_access_keeping_named_rings() {
    let daemon = common::TestDaemon::start().await;
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "data.txt", b"content").await;

    import_with_rings(&daemon, &file, &["friends"]).await;
    daemon
        .client
        .run(Op::BlobAttach {
            target: file.to_string_lossy().into_owned(),
            rings: vec![],
            open: true,
        })
        .await
        .unwrap();

    daemon
        .client
        .run(Op::BlobDetach {
            target: file.to_string_lossy().into_owned(),
            rings: vec![],
            open: true,
            all: false,
        })
        .await
        .unwrap();

    let open_lines = blob_list_for_ring(&daemon, "open").await;
    assert_eq!(
        open_lines,
        vec!["No blobs in local store."],
        "blob should no longer appear in the open ring"
    );

    let friends_lines = blob_list_for_ring(&daemon, "friends").await;
    assert!(
        friends_lines.iter().any(|l| l.contains("1 blobs")),
        "blob must still appear under 'friends'"
    );

    daemon.shutdown().await;
}

#[tokio::test]
async fn detach_ring_when_not_associated_returns_error() {
    let daemon = common::TestDaemon::start().await;
    let src = TempDir::new().unwrap();
    let file = common::write_file(src.path(), "data.txt", b"content").await;

    import_with_rings(&daemon, &file, &["friends"]).await;
    daemon
        .client
        .run(Op::RingNew {
            name: "work".into(),
        })
        .await
        .unwrap();

    let err = daemon
        .client
        .run(Op::BlobDetach {
            target: file.to_string_lossy().into_owned(),
            rings: vec!["work".into()],
            open: false,
            all: false,
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not attached to"),
        "expected 'not attached to' in error; got: {err}"
    );

    daemon.shutdown().await;
}

#[tokio::test]
async fn parse_failure_with_valid_req_id_echoes_it_back() {
    let daemon = common::TestDaemon::start().await;
    let req_id = "550e8400-e29b-41d4-a716-446655440000";
    let event = send_raw(
        daemon.port,
        &format!(r#"{{"req_id":"{req_id}","op":"nonexistent"}}"#),
    )
    .await;
    assert_eq!(event.req_id.to_string(), req_id);
    assert!(matches!(event.kind, EventKind::Error { .. }));
    daemon.shutdown().await;
}

#[tokio::test]
async fn parse_failure_with_invalid_json_uses_nil_uuid() {
    let daemon = common::TestDaemon::start().await;
    let event = send_raw(daemon.port, "not json at all").await;
    assert_eq!(
        event.req_id.to_string(),
        "00000000-0000-0000-0000-000000000000"
    );
    assert!(matches!(event.kind, EventKind::Error { .. }));
    daemon.shutdown().await;
}

#[tokio::test]
async fn oversized_request_is_rejected_with_error() {
    let daemon = common::TestDaemon::start().await;
    let oversized = "x".repeat(512 * 1024 + 1);
    let event = send_raw(daemon.port, &oversized).await;
    assert_eq!(
        event.req_id.to_string(),
        "00000000-0000-0000-0000-000000000000",
        "oversized request should return nil UUID"
    );
    assert!(
        matches!(event.kind, EventKind::Error { .. }),
        "expected Error event for oversized request"
    );
    daemon.shutdown().await;
}

#[tokio::test]
async fn grants_list_on_empty_store_returns_empty_message() {
    let daemon = common::TestDaemon::start().await;
    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(
            Op::Grants {
                peer: None,
                privilege: None,
            },
            |event| {
                if let EventKind::Line { text } = event.kind {
                    lines.push(text);
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(lines, vec!["No grants."]);
    daemon.shutdown().await;
}

#[tokio::test]
async fn grant_add_then_list_shows_the_grant() {
    let daemon = common::TestDaemon::start().await;
    let mut peer_id = String::new();
    daemon
        .client
        .send(Op::NodeId { qr_code: false }, |event| {
            if let EventKind::Line { text } = event.kind {
                peer_id = text;
            }
        })
        .await
        .unwrap();

    daemon
        .client
        .run(Op::Grant {
            peer: peer_id.clone(),
            privilege: "blob-list".into(),
        })
        .await
        .unwrap();

    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(
            Op::Grants {
                peer: None,
                privilege: None,
            },
            |event| {
                if let EventKind::Line { text } = event.kind {
                    lines.push(text);
                }
            },
        )
        .await
        .unwrap();
    assert!(lines[0].contains("1 grants:"), "got: {:?}", lines);
    assert!(lines[1].contains(&peer_id));
    daemon.shutdown().await;
}

#[tokio::test]
async fn grant_revoke_removes_grant_from_list() {
    let daemon = common::TestDaemon::start().await;
    let mut peer_id = String::new();
    daemon
        .client
        .send(Op::NodeId { qr_code: false }, |event| {
            if let EventKind::Line { text } = event.kind {
                peer_id = text;
            }
        })
        .await
        .unwrap();

    daemon
        .client
        .run(Op::Grant {
            peer: peer_id.clone(),
            privilege: "blob-list".into(),
        })
        .await
        .unwrap();
    daemon
        .client
        .run(Op::Revoke {
            peer: peer_id.clone(),
            privilege: "blob-list".into(),
        })
        .await
        .unwrap();

    let mut lines: Vec<String> = Vec::new();
    daemon
        .client
        .send(
            Op::Grants {
                peer: None,
                privilege: None,
            },
            |event| {
                if let EventKind::Line { text } = event.kind {
                    lines.push(text);
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(lines, vec!["No grants."]);
    daemon.shutdown().await;
}
