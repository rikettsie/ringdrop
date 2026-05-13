use std::net::SocketAddr;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use uuid::Uuid;

use super::protocol::{Event, EventKind, Op, Request};

pub struct DaemonClient {
    addr: SocketAddr,
}

impl DaemonClient {
    pub fn new(port: u16) -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    pub async fn is_running(&self) -> bool {
        TcpStream::connect(self.addr).await.is_ok()
    }

    /// Send an operation to the daemon and call `on_event` for each event
    /// received until [`EventKind::Done`] or [`EventKind::Error`] for this
    /// request's `req_id`.
    ///
    /// A UUID v4 `req_id` is generated automatically and injected into the
    /// request before sending; the same id is echoed back on every response
    /// event, allowing multiplexed connections in the future.
    pub async fn send(&self, op: Op, mut on_event: impl FnMut(Event)) -> Result<()> {
        let req = Request {
            req_id: Some(Uuid::new_v4()),
            op,
        };

        let stream = TcpStream::connect(self.addr).await.with_context(|| {
            format!(
                "cannot connect to daemon at {} — start it with: rdrop daemon start",
                self.addr
            )
        })?;

        let (reader, mut writer) = stream.into_split();
        let json = serde_json::to_string(&req)?;
        writer.write_all(format!("{json}\n").as_bytes()).await?;

        let mut reader = BufReader::new(reader);
        let mut buf = String::new();
        loop {
            buf.clear();
            if reader.read_line(&mut buf).await? == 0 {
                anyhow::bail!("daemon closed connection before sending Done or Error");
            }
            let event: Event = serde_json::from_str(buf.trim())
                .with_context(|| format!("malformed event from daemon: {}", buf.trim()))?;
            let is_terminal = matches!(event.kind, EventKind::Done | EventKind::Error { .. });
            on_event(event);
            if is_terminal {
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
