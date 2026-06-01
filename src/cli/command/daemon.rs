use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::config::Config;
use crate::core::Node;
use crate::daemon::client::DaemonClient;
use crate::daemon::protocol::{EventKind, Op};
use crate::daemon::server::DaemonServer;
use iroh_rings::RedbRegistry;

pub(crate) async fn run_start(data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;

    if client.is_running().await {
        println!("Rdrop daemon is already running.");
        return Ok(());
    }

    let exe = std::env::current_exe().context("could not resolve current executable path")?;
    let data_dir_str = data_dir
        .to_str()
        .context("data_dir path is not valid UTF-8")?;

    let mut cmd = std::process::Command::new(&exe);
    cmd.args(["--data-dir", data_dir_str, "daemon", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // When pressing Ctrl-C in the terminal, the kernel sends SIGINT
    // to every process in the foreground process group.
    // Since the daemon is in its own separate group, it doesn't receive
    // that signal (only the parent `daemon start` does).
    // The daemon keeps running after the parent exits.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    // Windows has no SIGINT, so the following flags achieve the same isolation
    // via the Windows API.
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
        cmd.creation_flags(0x00000008 | 0x00000200);
    }

    cmd.spawn().context("failed to spawn daemon process")?;

    // Poll until the daemon is reachable (up to 3s).
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if client.is_running().await {
            let node_id = query_node_info(&client)
                .await
                .unwrap_or_else(|_| "?".into());
            let relay_label = relay_label(data_dir);
            println!("Rdrop daemon started. Node ID: {node_id}");
            println!("Relay: {relay_label}");
            return Ok(());
        }
    }

    anyhow::bail!("Rdrop daemon did not become reachable within 3s — check logs")
}

pub(crate) async fn run_stop(data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;

    if !client.is_running().await {
        println!("Rdrop daemon is not running.");
        return Ok(());
    }

    client.run(Op::Shutdown).await?;

    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !client.is_running().await {
            println!("Rdrop daemon stopped.");
            return Ok(());
        }
    }

    anyhow::bail!("Rdrop daemon did not stop within 3s")
}

pub(crate) async fn run_status(data_dir: &Path) -> Result<()> {
    let client = super::daemon_client(data_dir)?;

    if !client.is_running().await {
        println!("Rdrop daemon is not running. Start it with: rdrop daemon start");
        return Ok(());
    }

    let relay_label = relay_label(data_dir);
    match query_node_info(&client).await {
        Ok(id) => {
            println!("Rdrop daemon running. Node ID: {id}");
            println!("Relay: {relay_label}");
        }
        Err(e) => println!("Rdrop daemon running but failed to get node info: {e}"),
    }
    Ok(())
}

pub(crate) async fn run_serve(data_dir: &Path) -> Result<()> {
    let cfg = Config::load_or_create(data_dir).context("loading config")?;
    let port = cfg.daemon_port;
    let registry =
        RedbRegistry::open(data_dir.join("registry.redb")).context("opening registry")?;
    let node = Node::start(data_dir, cfg, registry).await?;

    DaemonServer::bind(node, port).await?.run().await
}

/// Returns a human-readable relay label for display in `daemon start` and `daemon status`.
///
/// Reads the config from disk; if the config cannot be loaded (e.g. file missing), falls
/// back to `"default (n0)"` so that status output is never blocked by a config error.
fn relay_label(data_dir: &Path) -> String {
    match Config::load_or_create(data_dir) {
        Ok(cfg) => match cfg.relay_url {
            Some(url) => format!("{url} (custom)"),
            None => "default (n0)".to_owned(),
        },
        Err(_) => "default (n0)".to_owned(),
    }
}

async fn query_node_info(client: &DaemonClient) -> Result<String> {
    let mut node_id = String::new();
    let mut err: Option<String> = None;
    client
        .send(Op::NodeId, |event| match event.kind {
            EventKind::Line { text } => node_id = text,
            EventKind::Error { message } => err = Some(message),
            _ => {}
        })
        .await?;
    if let Some(msg) = err {
        anyhow::bail!(msg);
    }
    Ok(node_id)
}
