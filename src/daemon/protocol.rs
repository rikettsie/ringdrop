//! IPC protocol types: [`Request`], [`Op`], [`Event`], and [`EventKind`].

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A request sent from a CLI client (or GUI) to the daemon over TCP.
///
/// Each connection carries exactly one `Request` (newline-terminated JSON),
/// followed by a stream of [`Event`]s from the daemon until
/// [`EventKind::Done`] or [`EventKind::Error`].
///
/// `req_id` is echoed back on every response event, allowing a persistent
/// connection to multiplex concurrent requests (e.g. a GUI importing several
/// files simultaneously).
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    /// Unique identifier echoed on every response event for this request.
    pub req_id: Uuid,
    /// The operation to perform.
    #[serde(flatten)]
    pub op: Op,
}

/// The operation to perform, carried inside a [`Request`].
///
/// Serialised as `{"op": "<snake_case_variant>", ...fields}`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// Returns this node's [`EndpointId`] as a hex string.
    ///
    /// [`EndpointId`]: iroh::EndpointId
    NodeId,
    /// Imports a file or directory and tags it with the given rings.
    Import {
        /// Path to the file or directory to import.
        path: PathBuf,
        /// Ring names to tag the blob with (ignored when `open` is `true`).
        rings: Vec<String>,
        /// Tag the blob as publicly accessible, overriding `rings`.
        open: bool,
    },
    /// Lists all blobs in the local store.
    BlobList {
        /// Optional filter by this peer-id.
        peer: Option<String>,
        /// Optional filter by a ring name.
        rings: Option<Vec<String>>,
    },
    /// Removes a blob from the local store. `target` is a filename or hex hash.
    BlobRemove {
        /// File path or BLAKE3 hex hash identifying the blob to remove.
        target: String,
    },
    /// Attaches a blob to the given rings (or the open ring). `target` is a filename or hex hash.
    BlobAttach {
        /// File path or BLAKE3 hex hash identifying the blob to attach.
        target: String,
        /// Ring names to apply (ignored when `open` is `true`).
        rings: Vec<String>,
        /// Attach the blob as publicly accessible, overriding `rings`.
        open: bool,
    },
    /// Removes ring associations from a blob. `target` is a filename or hex hash.
    ///
    /// Exactly one of `rings` (non-empty), `open`, or `all` must be set.
    BlobDetach {
        /// File path or BLAKE3 hex hash identifying the blob to detach.
        target: String,
        /// Ring names to remove (used when `open` and `all` are both `false`).
        rings: Vec<String>,
        /// Remove the open-ring association, leaving named rings intact.
        open: bool,
        /// Remove every ring association, making the blob inaccessible.
        all: bool,
    },
    /// Creates a new ring with the given name.
    RingNew {
        /// Name for the new ring (e.g. `"friends"` or `"work-team"`).
        name: String,
    },
    /// Lists all rings.
    RingList,
    /// Adds `peer` to `ring`.
    ///
    /// If the peer is not yet in the peer store it is automatically registered
    /// there with no nickname. Use [`Op::PeerAdd`] with `--nickname` to set one.
    RingAdd {
        /// Name of the ring to add the peer to.
        ring: String,
        /// Base32 [`EndpointId`] string of the peer to add.
        ///
        /// [`EndpointId`]: iroh::EndpointId
        peer: String,
    },
    /// Removes `peer` from `ring`.
    RingRemove {
        /// Name of the ring to remove the peer from.
        ring: String,
        /// Base32 [`EndpointId`] string of the peer to remove.
        ///
        /// [`EndpointId`]: iroh::EndpointId
        peer: String,
    },
    /// Lists all members of `ring`.
    RingMembers {
        /// Name of the ring whose membership to list.
        ring: String,
    },
    /// Downloads the blob described by `ticket` and exports it to `dest`.
    Receive {
        /// URI-encoded [`ShareTicket`] produced by `rdrop import` or `rdrop blob list`.
        ///
        /// [`ShareTicket`]: crate::core::ShareTicket
        ticket: String,
        /// Filesystem path to write the received file or directory to.
        dest: PathBuf,
        /// Overwrite an existing destination without prompting.
        force_overwrite: bool,
    },
    /// Grants `privilege` to `peer`.
    ///
    /// `peer` is a base32-encoded [`EndpointId`]; `privilege` is the canonical
    /// privilege name (e.g. `"blob-list"`).
    ///
    /// [`EndpointId`]: iroh::EndpointId
    Grant {
        /// Base32 [`EndpointId`] string of the peer to grant access to.
        ///
        /// [`EndpointId`]: iroh::EndpointId
        peer: String,
        /// Privilege to grant (e.g. `"blob-list"`).
        privilege: String,
    },
    /// Revokes `privilege` from `peer`.
    ///
    /// `peer` is a base32-encoded [`EndpointId`]; `privilege` is the canonical
    /// privilege name (e.g. `"blob-list"`).
    ///
    /// [`EndpointId`]: iroh::EndpointId
    Revoke {
        /// Base32 [`EndpointId`] string of the peer to revoke access from.
        ///
        /// [`EndpointId`]: iroh::EndpointId
        peer: String,
        /// Privilege to revoke (e.g. `"blob-list"`).
        privilege: String,
    },
    /// Lists current grants as `privilege peer_id` pairs, one per [`EventKind::Line`].
    ///
    /// Both filters are optional; omitting them returns all grants.
    Grants {
        /// Only show grants for this base32 [`EndpointId`].
        ///
        /// [`EndpointId`]: iroh::EndpointId
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peer: Option<String>,
        /// Only show grants for this privilege name (e.g. `"blob-list"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        privilege: Option<String>,
    },
    /// Fetches the blob catalog from a remote node.
    ///
    /// The remote node must have granted `BlobList` privilege to the local
    /// node's identity. Entries visible via ring membership are streamed back
    /// as [`EventKind::Line`] events, one per blob.
    ///
    /// `peer` is a base32-encoded [`EndpointId`].
    ///
    /// [`EndpointId`]: iroh::EndpointId
    RemoteBlobList {
        /// Base32 [`EndpointId`] string of the remote node to query.
        ///
        /// [`EndpointId`]: iroh::EndpointId
        peer: String,
    },
    /// Adds `peer` to the peer store, optionally with a nickname.
    ///
    /// Idempotent: if the peer is already in the store the nickname is updated.
    /// `peer` is a base32-encoded [`EndpointId`].
    ///
    /// [`EndpointId`]: iroh::EndpointId
    PeerAdd {
        /// Base32 [`EndpointId`] string of the peer to register.
        ///
        /// [`EndpointId`]: iroh::EndpointId
        peer: String,
        /// Optional human-readable label for this peer.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nickname: Option<String>,
    },
    /// Lists all peers in the local peer store with their nicknames.
    PeerList,
    /// Removes a peer from the local peer store and from all rings.
    ///
    /// Errors if the peer is not in the store. `peer` is a base32-encoded
    /// [`EndpointId`].
    ///
    /// [`EndpointId`]: iroh::EndpointId
    PeerRemove {
        /// Base32 [`EndpointId`] string of the peer to remove.
        ///
        /// [`EndpointId`]: iroh::EndpointId
        peer: String,
    },
    /// Gracefully stops the daemon after draining in-flight requests.
    Shutdown,
}

/// An event streamed from the daemon to the client.
///
/// The daemon sends one or more events per request, always ending with
/// [`EventKind::Done`] or [`EventKind::Error`]. `req_id` matches the value
/// sent in the originating [`Request`], enabling multiplexed connections.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Matches the [`Request::req_id`] that triggered this event.
    pub req_id: Uuid,
    /// The event payload.
    #[serde(flatten)]
    pub kind: EventKind,
}

/// The payload of a daemon [`Event`].
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    /// Line of text to be printed to stdout (by a console process) or rendered (by a GUI).
    Line {
        /// The line of text to display.
        text: String,
    },
    /// Download/upload progress indicator for long-running transfers.
    Progress {
        /// Bytes received or sent so far.
        done: u64,
        /// Total expected bytes.
        total: u64,
    },
    /// Per-file progress during a directory blob (`HashSeq`) transfer.
    FileProgress {
        /// 1-based index of the current file in the collection.
        file_index: usize,
        /// Total number of files in the collection.
        file_total: usize,
        /// Relative path of the file within the collection.
        file_name: String,
        /// Bytes received so far for this file.
        done: u64,
        /// Total size of this file in bytes.
        total: u64,
    },
    /// Signal of request completed successfully; no further events will follow for this req_id.
    Done,
    /// Signal of request failed; no further events will follow for this req_id.
    Error {
        /// Human-readable description of the failure.
        message: String,
    },
    /// Structured data record for machine consumers (e.g. a GUI).
    ///
    /// Emitted alongside or instead of `Line` events by ops that return lists.
    /// The CLI ignores this variant; the GUI uses it exclusively.
    Record {
        /// JSON-encoded payload. The shape depends on the originating `Op` —
        /// see each handler's documentation for the field names.
        value: serde_json::Value,
    },
}

impl Event {
    /// Constructs a [`EventKind::Line`] event carrying a text message.
    pub fn line(req_id: Uuid, text: impl Into<String>) -> Self {
        Self {
            req_id,
            kind: EventKind::Line { text: text.into() },
        }
    }

    /// Constructs a blank [`EventKind::Line`] event — renders as an empty line in the console.
    pub fn blank(req_id: Uuid) -> Self {
        Self::line(req_id, "")
    }

    /// Constructs a [`EventKind::Progress`] event with byte counts.
    pub fn progress(req_id: Uuid, done: u64, total: u64) -> Self {
        Self {
            req_id,
            kind: EventKind::Progress { done, total },
        }
    }

    /// Constructs a [`EventKind::Done`] event signalling successful completion.
    pub fn done(req_id: Uuid) -> Self {
        Self {
            req_id,
            kind: EventKind::Done,
        }
    }

    /// Constructs an [`EventKind::Error`] event carrying a human-readable message.
    pub fn error(req_id: Uuid, message: impl Into<String>) -> Self {
        Self {
            req_id,
            kind: EventKind::Error {
                message: message.into(),
            },
        }
    }

    /// Constructs an [`EventKind::Record`] event carrying a structured JSON value.
    pub fn record(req_id: Uuid, value: serde_json::Value) -> Self {
        Self {
            req_id,
            kind: EventKind::Record { value },
        }
    }

    /// Constructs a [`EventKind::FileProgress`] event for a directory transfer.
    pub fn file_progress(
        req_id: Uuid,
        file_index: usize,
        file_total: usize,
        file_name: impl Into<String>,
        done: u64,
        total: u64,
    ) -> Self {
        Self {
            req_id,
            kind: EventKind::FileProgress {
                file_index,
                file_total,
                file_name: file_name.into(),
                done,
                total,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_node_id_serializes_to_snake_case_tag() {
        assert_eq!(
            serde_json::to_string(&Op::NodeId).unwrap(),
            r#"{"op":"node_id"}"#
        );
    }

    #[test]
    fn op_blob_list_serializes_correctly() {
        assert_eq!(
            serde_json::to_string(&Op::BlobList {
                peer: None,
                rings: None,
            })
            .unwrap(),
            r#"{"op":"blob_list","peer":null,"rings":null}"#
        );
    }

    #[test]
    fn op_ring_new_serializes_with_name_field() {
        let json = serde_json::to_string(&Op::RingNew {
            name: "friends".into(),
        })
        .unwrap();
        assert_eq!(json, r#"{"op":"ring_new","name":"friends"}"#);
    }

    // Request: req_id is mandatory

    #[test]
    fn request_serializes_req_id() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let req = Request {
            req_id: id,
            op: Op::BlobList {
                peer: None,
                rings: None,
            },
        };
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"req_id":"550e8400-e29b-41d4-a716-446655440000","op":"blob_list","peer":null,"rings":null}"#
        );
    }

    #[test]
    fn request_without_req_id_fails_to_deserialize() {
        let result: Result<Request, _> = serde_json::from_str(r#"{"op":"node_id"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn request_with_empty_req_id_fails_to_deserialize() {
        let result: Result<Request, _> = serde_json::from_str(r#"{"req_id":"","op":"node_id"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn request_with_malformed_req_id_fails_to_deserialize() {
        let result: Result<Request, _> =
            serde_json::from_str(r#"{"req_id":"not-a-uuid","op":"node_id"}"#);
        assert!(result.is_err());
    }

    // Event: req_id is mandatory

    #[test]
    fn event_done_serializes_req_id_and_type() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            serde_json::to_string(&Event::done(id)).unwrap(),
            r#"{"req_id":"550e8400-e29b-41d4-a716-446655440000","type":"done"}"#
        );
    }

    #[test]
    fn event_line_serializes_req_id_type_and_text() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            serde_json::to_string(&Event::line(id, "hello world")).unwrap(),
            r#"{"req_id":"550e8400-e29b-41d4-a716-446655440000","type":"line","text":"hello world"}"#
        );
    }

    #[test]
    fn event_progress_serializes_correctly() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            serde_json::to_string(&Event::progress(id, 50, 100)).unwrap(),
            r#"{"req_id":"550e8400-e29b-41d4-a716-446655440000","type":"progress","done":50,"total":100}"#
        );
    }

    #[test]
    fn event_file_progress_serializes_correctly() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            serde_json::to_string(&Event::file_progress(id, 1, 3, "readme.txt", 50, 100)).unwrap(),
            r#"{"req_id":"550e8400-e29b-41d4-a716-446655440000","type":"file_progress","file_index":1,"file_total":3,"file_name":"readme.txt","done":50,"total":100}"#
        );
    }

    #[test]
    fn event_error_serializes_correctly() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            serde_json::to_string(&Event::error(id, "something went wrong")).unwrap(),
            r#"{"req_id":"550e8400-e29b-41d4-a716-446655440000","type":"error","message":"something went wrong"}"#
        );
    }

    #[test]
    fn event_without_req_id_fails_to_deserialize() {
        let result: Result<Event, _> = serde_json::from_str(r#"{"type":"done"}"#);
        assert!(result.is_err());
    }

    // Round-trips

    #[test]
    fn request_round_trips_through_json() {
        let original = Request {
            req_id: Uuid::new_v4(),
            op: Op::RingNew {
                name: "work".into(),
            },
        };
        let parsed: Request =
            serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn event_round_trips_through_json() {
        let original = Event::progress(Uuid::new_v4(), 42, 100);
        let parsed: Event =
            serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn op_grant_serializes_correctly() {
        let json = serde_json::to_string(&Op::Grant {
            peer: "abc123".into(),
            privilege: "blob-list".into(),
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"op":"grant","peer":"abc123","privilege":"blob-list"}"#
        );
    }

    #[test]
    fn op_revoke_serializes_correctly() {
        let json = serde_json::to_string(&Op::Revoke {
            peer: "abc123".into(),
            privilege: "blob-list".into(),
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"op":"revoke","peer":"abc123","privilege":"blob-list"}"#
        );
    }

    #[test]
    fn op_grants_without_filters_serializes_to_op_only() {
        assert_eq!(
            serde_json::to_string(&Op::Grants {
                peer: None,
                privilege: None,
            })
            .unwrap(),
            r#"{"op":"grants"}"#
        );
    }

    #[test]
    fn op_grants_with_filters_serializes_optional_fields() {
        let json = serde_json::to_string(&Op::Grants {
            peer: Some("abc123".into()),
            privilege: Some("blob-list".into()),
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"op":"grants","peer":"abc123","privilege":"blob-list"}"#
        );
    }

    #[test]
    fn op_remote_blob_list_serializes_correctly() {
        let json = serde_json::to_string(&Op::RemoteBlobList {
            peer: "abc123".into(),
        })
        .unwrap();
        assert_eq!(json, r#"{"op":"remote_blob_list","peer":"abc123"}"#);
    }

    #[test]
    fn op_grants_without_filters_deserializes_correctly() {
        let op: Op = serde_json::from_str(r#"{"op":"grants"}"#).unwrap();
        assert_eq!(
            op,
            Op::Grants {
                peer: None,
                privilege: None
            }
        );
    }

    #[test]
    fn op_grants_with_peer_filter_deserializes_correctly() {
        let op: Op = serde_json::from_str(r#"{"op":"grants","peer":"abc123"}"#).unwrap();
        assert_eq!(
            op,
            Op::Grants {
                peer: Some("abc123".into()),
                privilege: None
            }
        );
    }

    #[test]
    fn op_grants_with_both_filters_round_trips() {
        let original = Op::Grants {
            peer: Some("abc123".into()),
            privilege: Some("blob-list".into()),
        };
        let parsed: Op = serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn op_ring_add_serializes_without_nickname_field() {
        let json = serde_json::to_string(&Op::RingAdd {
            ring: "friends".into(),
            peer: "abc123".into(),
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"op":"ring_add","ring":"friends","peer":"abc123"}"#
        );
    }

    #[test]
    fn op_detach_with_all_serializes_correctly() {
        let json = serde_json::to_string(&Op::BlobDetach {
            target: "abc.txt".into(),
            rings: vec![],
            open: false,
            all: true,
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"op":"blob_detach","target":"abc.txt","rings":[],"open":false,"all":true}"#
        );
    }

    #[test]
    fn op_detach_round_trips_through_json() {
        let original = Op::BlobDetach {
            target: "abc.txt".into(),
            rings: vec!["friends".into()],
            open: false,
            all: false,
        };
        let parsed: Op = serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn op_peer_add_without_nickname_omits_nickname_field() {
        let json = serde_json::to_string(&Op::PeerAdd {
            peer: "abc123".into(),
            nickname: None,
        })
        .unwrap();
        assert_eq!(json, r#"{"op":"peer_add","peer":"abc123"}"#);
    }

    #[test]
    fn op_peer_add_with_nickname_includes_nickname_field() {
        let json = serde_json::to_string(&Op::PeerAdd {
            peer: "abc123".into(),
            nickname: Some("alice".into()),
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"op":"peer_add","peer":"abc123","nickname":"alice"}"#
        );
    }

    #[test]
    fn op_peer_list_serializes_correctly() {
        assert_eq!(
            serde_json::to_string(&Op::PeerList).unwrap(),
            r#"{"op":"peer_list"}"#
        );
    }

    #[test]
    fn op_peer_remove_serializes_correctly() {
        let json = serde_json::to_string(&Op::PeerRemove {
            peer: "abc123".into(),
        })
        .unwrap();
        assert_eq!(json, r#"{"op":"peer_remove","peer":"abc123"}"#);
    }

    #[test]
    fn op_peer_add_round_trips_with_nickname() {
        let original = Op::PeerAdd {
            peer: "abc123".into(),
            nickname: Some("alice".into()),
        };
        let parsed: Op = serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn op_peer_add_without_nickname_deserializes_correctly() {
        let op: Op = serde_json::from_str(r#"{"op":"peer_add","peer":"abc123"}"#).unwrap();
        assert_eq!(
            op,
            Op::PeerAdd {
                peer: "abc123".into(),
                nickname: None,
            }
        );
    }

    #[test]
    fn event_record_serializes_correctly() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let value = serde_json::json!({"hash": "abc123", "name": "file.txt"});
        let event = Event::record(id, value);
        let json = serde_json::to_string(&event).unwrap();
        // Must carry the "type":"record" tag and the value fields.
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "record");
        assert_eq!(parsed["value"]["hash"], "abc123");
        assert_eq!(parsed["value"]["name"], "file.txt");
        assert_eq!(parsed["req_id"], "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn event_record_round_trips_through_json() {
        let original = Event::record(
            Uuid::new_v4(),
            serde_json::json!({"peer_id": "deadbeef", "rings": ["friends", "work"]}),
        );
        let parsed: Event =
            serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn event_blank_produces_empty_text() {
        let id = Uuid::new_v4();
        let event = Event::blank(id);
        assert_eq!(
            event.kind,
            EventKind::Line {
                text: String::new()
            }
        );
    }

    #[test]
    fn event_record_round_trips_with_non_object_json_value() {
        let id = Uuid::new_v4();
        let original = Event::record(id, serde_json::json!([1, 2, 3]));
        let parsed: Event =
            serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(parsed, original);
    }
}
