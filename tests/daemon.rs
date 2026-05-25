mod common;

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
async fn ring_add_self_is_rejected_via_daemon() {
    let daemon = common::TestDaemon::start().await;

    let mut node_id = String::new();
    daemon
        .client
        .send(Op::NodeId, |event| {
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
            nickname: None,
        })
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("yourself"),
        "expected 'yourself' in error message; got: {err}"
    );
    daemon.shutdown().await;
}

#[tokio::test]
async fn tag_with_no_rings_and_no_open_returns_error() {
    let daemon = common::TestDaemon::start().await;
    let err = daemon
        .client
        .run(Op::Tag {
            target: "deadbeef".into(),
            rings: vec![],
            open: false,
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("nothing to tag"),
        "expected 'nothing to tag' in error; got: {err}"
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
        .send(Op::NodeId, |event| {
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
        .send(Op::NodeId, |event| {
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
