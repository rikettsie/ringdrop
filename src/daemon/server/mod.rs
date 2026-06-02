//! TCP-based IPC server that wraps a [`Node`] and dispatches [`Op`]s.
//!
//! Each accepted TCP connection carries exactly one [`Request`] (newline-
//! terminated JSON). The server deserialises it, dispatches to the appropriate
//! handler, and streams [`Event`]s back until the operation completes.
//!
//! [`Node`]: crate::core::Node
//! [`Op`]: crate::daemon::protocol::Op
//! [`Request`]: crate::daemon::protocol::Request
//! [`Event`]: crate::daemon::protocol::Event

mod handlers;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_lite::StreamExt;
use iroh_rings::Registry;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};
use tracing::{error, info};
use uuid::Uuid;

use crate::core::Node;
use crate::daemon::protocol::{Event, Op, Request};

/// The background daemon server.
///
/// Listens on a local TCP socket and serves [`Op`] requests from
/// [`DaemonClient`]s. Each accepted connection is handled in a separate
/// Tokio task; an [`Op::Shutdown`] request drains in-flight tasks (up to 30 s)
/// then shuts the node down cleanly.
///
/// [`Op`]: crate::daemon::protocol::Op
/// [`DaemonClient`]: crate::daemon::client::DaemonClient
/// [`Op::Shutdown`]: crate::daemon::protocol::Op::Shutdown
pub struct DaemonServer<R> {
    node: Arc<Node<R>>,
    listener: TcpListener,
    shutdown: Arc<Notify>,
}

impl<R: Registry + Clone + Send + Sync + 'static> DaemonServer<R> {
    /// Binds the daemon to `127.0.0.1:port` (use `0` to let the OS pick a port).
    ///
    /// # Errors
    ///
    /// Returns an error if the port is already in use.
    pub async fn bind(node: Node<R>, port: u16) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|e| anyhow::anyhow!("cannot bind to port {port}: {e}"))?;
        info!(port, "Rdrop daemon listening");
        Ok(Self {
            node: Arc::new(node),
            listener,
            shutdown: Arc::new(Notify::new()),
        })
    }

    /// Returns the port the server is actually listening on.
    ///
    /// Useful when bound on port `0` (OS-assigned ephemeral port).
    pub fn local_port(&self) -> u16 {
        self.listener
            .local_addr()
            .expect("listener is bound")
            .port()
    }

    /// Runs the server event loop until an [`Op::Shutdown`] request is received.
    ///
    /// Accepts connections, dispatches requests, and on shutdown drains
    /// in-flight tasks (up to 30 s) before calling [`Node::shutdown`].
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP accept loop fails or the node shutdown fails.
    ///
    /// [`Op::Shutdown`]: crate::daemon::protocol::Op::Shutdown
    /// [`Node::shutdown`]: crate::core::Node::shutdown
    pub async fn run(self) -> Result<()> {
        let mut tasks: JoinSet<()> = JoinSet::new();
        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    let (stream, addr) = result?;
                    info!(%addr, "connection accepted");
                    let node = Arc::clone(&self.node);
                    let shutdown = Arc::clone(&self.shutdown);
                    tasks.spawn(async move {
                        if let Err(e) = handle_connection(stream, node, shutdown).await {
                            error!("connection error: {e:#}");
                        }
                    });
                }
                _ = self.shutdown.notified() => {
                    info!("shutdown requested, draining in-flight requests");
                    break;
                }
            }
        }

        // Give in-flight requests up to 30s to finish cleanly, then abort.
        let drain = async { while tasks.join_next().await.is_some() {} };
        if tokio::time::timeout(Duration::from_secs(30), drain)
            .await
            .is_err()
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }

        Arc::try_unwrap(self.node)
            .unwrap_or_else(|_| panic!("all connection tasks completed"))
            .shutdown()
            .await
    }
}

use super::MAX_LINE_BYTES;

async fn handle_connection<R: Registry + Clone + Send + Sync + 'static>(
    stream: TcpStream,
    node: Arc<Node<R>>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut framed = FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_LINE_BYTES));

    let line = match framed.next().await {
        None => return Ok(()),
        Some(Err(LinesCodecError::MaxLineLengthExceeded)) => {
            emit(
                &mut writer,
                &Event::error(Uuid::nil(), "request exceeds maximum line length"),
            )
            .await;
            return Ok(());
        }
        Some(Err(e)) => return Err(e.into()),
        Some(Ok(l)) => l,
    };

    let req: Request = match serde_json::from_str(&line) {
        Ok(r) => r,
        Err(e) => {
            // Best-effort: recover req_id from the raw JSON so the client can
            // correlate the error. Falls back to Uuid::nil() (all zeros) when
            // the payload is not even valid JSON or carries no req_id field.
            let req_id = serde_json::from_str::<serde_json::Value>(&line)
                .ok()
                .and_then(|v| v.get("req_id")?.as_str()?.parse::<Uuid>().ok())
                .unwrap_or_else(Uuid::nil);
            emit(&mut writer, &Event::error(req_id, e.to_string())).await;
            return Ok(());
        }
    };

    let req_id = req.req_id;
    let (tx, mut rx) = mpsc::channel::<Event>(32);

    tokio::spawn(dispatch(req.op, req_id, node, tx, shutdown));

    while let Some(event) = rx.recv().await {
        if !emit(&mut writer, &event).await {
            break;
        }
    }

    Ok(())
}

/// Writes one event to the TCP stream. Returns `false` if the connection should
/// be closed — either because the event could not be serialized (logged as an
/// error) or because the write itself failed (client disconnected, not logged).
async fn emit(writer: &mut (impl AsyncWriteExt + Unpin), event: &Event) -> bool {
    let json = match serde_json::to_string(event) {
        Ok(j) => j,
        Err(e) => {
            error!("failed to serialize event, closing connection: {e}");
            return false;
        }
    };
    writer
        .write_all(format!("{json}\n").as_bytes())
        .await
        .is_ok()
}

async fn dispatch<R: Registry + Clone + Send + Sync + 'static>(
    op: Op,
    req_id: Uuid,
    node: Arc<Node<R>>,
    tx: mpsc::Sender<Event>,
    shutdown: Arc<Notify>,
) {
    if let Op::Shutdown = op {
        let _ = tx.send(Event::done(req_id)).await;
        shutdown.notify_one();
        return;
    }

    match handle_op(op, req_id, &node, &tx).await {
        Ok(()) => {}
        Err(e) => {
            let _ = tx.send(Event::error(req_id, e.to_string())).await;
        }
    }
}

async fn handle_op<R: Registry + Clone + Send + Sync + 'static>(
    op: Op,
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
) -> Result<()> {
    match op {
        Op::NodeId => {
            let peer_id = node.endpoint.id();
            let _ = tx.send(Event::line(req_id, peer_id.to_string())).await;
            let _ = tx
                .send(Event::record(
                    req_id,
                    serde_json::json!({ "peer_id": peer_id.to_string() }),
                ))
                .await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::Import { path, rings, open } => {
            handlers::blob::handle_import(req_id, node, tx, path, rings, open).await?;
        }
        Op::BlobList { peer, rings } => {
            handlers::blob::handle_blob_list(req_id, node, tx, peer, rings).await?;
        }
        Op::BlobRemove { target } => {
            handlers::blob::handle_blob_remove(req_id, node, tx, target).await?;
        }
        Op::Tag {
            target,
            rings,
            open,
        } => {
            handlers::tag::handle_tag(req_id, node, tx, target, rings, open).await?;
        }
        Op::Untag {
            target,
            rings,
            open,
            all,
        } => {
            handlers::tag::handle_untag(req_id, node, tx, target, rings, open, all).await?;
        }
        Op::RingNew { name } => {
            let lines = handlers::ring::ring_new_lines(&node.registry, &name)?;
            send_lines(tx, req_id, &lines).await;
            let _ = tx
                .send(Event::record(req_id, serde_json::json!({ "name": name })))
                .await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::RingList => {
            let lines = handlers::ring::ring_list_lines(&node.registry)?;
            send_lines(tx, req_id, &lines).await;
            for ring in node.registry.list_rings()? {
                let _ = tx
                    .send(Event::record(
                        req_id,
                        serde_json::json!({
                            "name": ring.as_str(),
                            "open": ring.is_open(),
                        }),
                    ))
                    .await;
            }
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::RingAdd { ring, peer } => {
            let lines = handlers::ring::ring_add_lines(
                &node.registry,
                &node.peers,
                node.endpoint.id(),
                &ring,
                &peer,
            )?;
            send_lines(tx, req_id, &lines).await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::RingRemove { ring, peer } => {
            let lines = handlers::ring::ring_remove_lines(&node.registry, &ring, &peer)?;
            send_lines(tx, req_id, &lines).await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::RingMembers { ring } => {
            let lines = handlers::ring::ring_members_lines(&node.registry, &node.peers, &ring)?;
            send_lines(tx, req_id, &lines).await;
            if ring != iroh_rings::OPEN_RING_NAME {
                for (peer_id, _label) in node.registry.list_ring_peers(&ring)? {
                    let nickname = node.peers.get(&peer_id).ok().flatten().flatten();
                    let _ = tx
                        .send(Event::record(
                            req_id,
                            serde_json::json!({
                                "peer_id": peer_id.to_string(),
                                "nickname": nickname,
                            }),
                        ))
                        .await;
                }
            }
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::PeerAdd { peer, nickname } => {
            let lines = handlers::peer::peer_add_lines(&node.peers, &peer, nickname.as_deref())?;
            send_lines(tx, req_id, &lines).await;
            let _ = tx
                .send(Event::record(
                    req_id,
                    serde_json::json!({
                        "peer_id": peer,
                        "nickname": nickname,
                    }),
                ))
                .await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::PeerList => {
            let lines = handlers::peer::peer_list_lines(&node.peers)?;
            send_lines(tx, req_id, &lines).await;
            for (peer_id, nick) in node.peers.list()? {
                let _ = tx
                    .send(Event::record(
                        req_id,
                        serde_json::json!({
                            "peer_id": peer_id.to_string(),
                            "nickname": nick,
                        }),
                    ))
                    .await;
            }
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::PeerRemove { peer } => {
            let lines = handlers::peer::peer_remove_lines(
                &node.peers,
                &node.grants,
                &node.registry,
                &peer,
            )?;
            send_lines(tx, req_id, &lines).await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::Receive {
            ticket,
            dest,
            force_overwrite,
        } => {
            handlers::receive::handle_receive(req_id, node, tx, ticket, dest, force_overwrite)
                .await?;
        }
        Op::Grant { peer, privilege } => {
            handlers::grant::handle_grant(req_id, node, tx, peer, privilege).await?;
        }
        Op::Revoke { peer, privilege } => {
            handlers::grant::handle_revoke(req_id, node, tx, peer, privilege).await?;
        }
        Op::Grants { peer, privilege } => {
            handlers::grant::handle_grants(req_id, node, tx, peer, privilege).await?;
        }
        Op::RemoteBlobList { peer } => {
            handlers::remote::handle_remote_blob_list(req_id, node, tx, peer).await?;
        }
        Op::Shutdown => unreachable!("handled before handle_op"),
    }
    Ok(())
}

async fn send_lines(tx: &mpsc::Sender<Event>, req_id: Uuid, lines: &[String]) {
    for line in lines {
        let _ = tx.send(Event::line(req_id, line.clone())).await;
    }
}
