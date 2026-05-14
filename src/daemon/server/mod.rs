mod handlers;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;
use tracing::{error, info};
use uuid::Uuid;

use crate::core::Node;
use crate::daemon::protocol::{Event, Op, Request};
use iroh_rings::RedbRegistry;

pub struct DaemonServer {
    node: Arc<Node<RedbRegistry>>,
    listener: TcpListener,
    shutdown: Arc<Notify>,
}

impl DaemonServer {
    pub async fn bind(node: Node<RedbRegistry>, port: u16) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|e| anyhow::anyhow!("cannot bind to port {port}: {e}"))?;
        info!(port, "daemon listening");
        Ok(Self {
            node: Arc::new(node),
            listener,
            shutdown: Arc::new(Notify::new()),
        })
    }

    pub fn local_port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }

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

async fn handle_connection(
    stream: TcpStream,
    node: Arc<Node<RedbRegistry>>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }

    let req: Request = match serde_json::from_str(line.trim()) {
        Ok(r) => r,
        Err(e) => {
            emit(&mut writer, &Event::error(Uuid::new_v4(), e.to_string())).await;
            return Ok(());
        }
    };

    let req_id = req.req_id;
    let (tx, mut rx) = mpsc::channel::<Event>(32);

    tokio::spawn(dispatch(req.op, req_id, node, tx, shutdown));

    while let Some(event) = rx.recv().await {
        emit(&mut writer, &event).await;
    }

    Ok(())
}

async fn emit(writer: &mut (impl AsyncWriteExt + Unpin), event: &Event) {
    if let Ok(json) = serde_json::to_string(event) {
        let _ = writer.write_all(format!("{json}\n").as_bytes()).await;
    }
}

async fn dispatch(
    op: Op,
    req_id: Uuid,
    node: Arc<Node<RedbRegistry>>,
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

async fn handle_op(
    op: Op,
    req_id: Uuid,
    node: &Node<RedbRegistry>,
    tx: &mpsc::Sender<Event>,
) -> Result<()> {
    match op {
        Op::NodeId => {
            let _ = tx
                .send(Event::line(req_id, node.endpoint.id().to_string()))
                .await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::Import { path, rings, open } => {
            handlers::blob::handle_import(req_id, node, tx, path, rings, open).await?;
        }
        Op::BlobList => {
            handlers::blob::handle_blob_list(req_id, node, tx).await?;
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
        Op::Tags { target } => {
            handlers::tag::handle_tags(req_id, node, tx, target).await?;
        }
        Op::RingNew { name } => {
            let lines = handlers::ring::ring_new_lines(&node.registry, &name)?;
            send_lines(tx, req_id, &lines).await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::RingList => {
            let lines = handlers::ring::ring_list_lines(&node.registry)?;
            send_lines(tx, req_id, &lines).await;
            let _ = tx.send(Event::done(req_id)).await;
        }
        Op::RingAdd {
            ring,
            peer,
            nickname,
        } => {
            let lines = handlers::ring::ring_add_lines(
                &node.registry,
                node.endpoint.id(),
                &ring,
                &peer,
                nickname.as_deref(),
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
            let lines = handlers::ring::ring_members_lines(&node.registry, &ring)?;
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
        Op::Shutdown => unreachable!("handled before handle_op"),
    }
    Ok(())
}

async fn send_lines(tx: &mpsc::Sender<Event>, req_id: Uuid, lines: &[String]) {
    for line in lines {
        let _ = tx.send(Event::line(req_id, line.clone())).await;
    }
}
