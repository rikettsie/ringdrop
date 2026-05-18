use std::net::SocketAddr;

use anyhow::{Context, Result};
use futures_lite::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, LinesCodec};
use uuid::Uuid;

use super::protocol::{Event, EventKind, Op, Request};
use super::MAX_LINE_BYTES;

/// A lightweight TCP client for talking to a running [`DaemonServer`].
///
/// Each call to [`DaemonClient::send`] opens a new TCP connection, sends one
/// [`Op`] as a JSON line, and reads back [`Event`]s until the stream ends
/// with [`EventKind::Done`] or [`EventKind::Error`].
///
/// [`DaemonServer`]: crate::daemon::server::DaemonServer
/// [`Op`]: crate::daemon::protocol::Op
/// [`Event`]: crate::daemon::protocol::Event
/// [`EventKind::Done`]: crate::daemon::protocol::EventKind::Done
/// [`EventKind::Error`]: crate::daemon::protocol::EventKind::Error
pub struct DaemonClient {
    addr: SocketAddr,
}

impl DaemonClient {
    /// Create a client that connects to the daemon on `127.0.0.1:port`.
    pub fn new(port: u16) -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    /// Returns `true` if a TCP connection to the daemon address succeeds.
    pub async fn is_running(&self) -> bool {
        TcpStream::connect(self.addr).await.is_ok()
    }

    /// Send an operation to the daemon and call `on_event` for each event
    /// received until [`EventKind::Done`] or [`EventKind::Error`] for this
    /// request's `req_id`.
    ///
    /// A UUID v4 `req_id` is generated automatically and injected into the
    /// request before sending; the same id is echoed back on every response
    /// event, allowing multiplexed connections.
    pub async fn send(&self, op: Op, mut on_event: impl FnMut(Event)) -> Result<()> {
        let req = Request {
            req_id: Uuid::new_v4(),
            op,
        };

        let stream = TcpStream::connect(self.addr).await.with_context(|| {
            format!(
                "cannot connect to rdrop daemon at {} — start it with: rdrop daemon start",
                self.addr
            )
        })?;

        let (reader, mut writer) = stream.into_split();
        let json_req = serde_json::to_string(&req)?;
        writer.write_all(format!("{json_req}\n").as_bytes()).await?;

        let mut framed = FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_LINE_BYTES));
        loop {
            let line = framed
                .next()
                .await
                .ok_or_else(|| {
                    anyhow::anyhow!("rdrop daemon closed connection before sending Done or Error")
                })?
                .context("framing error reading from daemon")?;
            let event: Event = serde_json::from_str(&line)
                .with_context(|| format!("malformed event from daemon: {line}"))?;
            let is_eos = matches!(event.kind, EventKind::Done | EventKind::Error { .. });
            on_event(event);
            if is_eos {
                break;
            }
        }
        Ok(())
    }

    /// Convenience wrapper: prints [`EventKind::Line`] events to stdout and
    /// converts [`EventKind::Error`] into an `anyhow` error.
    pub async fn run(&self, op: Op) -> Result<()> {
        let mut err: Option<String> = None;
        self.send(op, |event| match event.kind {
            EventKind::Line { text } => println!("{text}"),
            EventKind::Error { message } => err = Some(message),
            EventKind::Done | EventKind::Progress { .. } => {}
        })
        .await?;
        if let Some(msg) = err {
            anyhow::bail!(msg);
        }
        Ok(())
    }
}
