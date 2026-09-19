use std::path::{Path, PathBuf};

use anyhow::Result;
use iroh_rings::Registry;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::{Node, ProgressEvent, ShareTicket};
use crate::daemon::protocol::Event;

use super::send;

fn expand_tilde(path: &Path, home: Option<&Path>) -> PathBuf {
    match (path.strip_prefix("~"), home) {
        (Ok(rest), Some(home)) => home.join(rest),
        _ => path.to_path_buf(),
    }
}

// If --dir is None, default_receive_dir from config.json is chosen
// If default_receive_dir is None then current directory is chosen
fn resolve_dest(
    dest: Option<PathBuf>,
    default_receive_dir: Option<PathBuf>,
    home: Option<&Path>,
) -> PathBuf {
    dest.or_else(|| default_receive_dir.map(|d| expand_tilde(&d, home)))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn check_dest(
    dest: &Path,
    name: Option<&str>,
    hash_hex: &str,
    force_overwrite: bool,
) -> Result<PathBuf> {
    let expected = if dest.is_dir() {
        dest.join(name.unwrap_or(hash_hex))
    } else {
        dest.to_path_buf()
    };
    if expected.exists() && !force_overwrite {
        anyhow::bail!(
            "destination '{}' already exists; \
             use --dest to choose a different location or --force-overwrite to replace it",
            expected.display()
        );
    }
    Ok(expected)
}

pub(crate) async fn handle_receive<R: Registry + Clone + Send + Sync + 'static>(
    req_id: Uuid,
    node: &Node<R>,
    tx: &mpsc::Sender<Event>,
    ticket_str: String,
    dest: Option<PathBuf>,
    default_receive_dir: Option<PathBuf>,
    force_overwrite: bool,
) -> Result<()> {
    let ticket = ShareTicket::from_uri(&ticket_str)?;
    let hash_hex = ticket.hash().to_string();

    // for resolving ~ in default_receive_dir
    let home_dir = dirs_next::home_dir();
    let dest = resolve_dest(dest, default_receive_dir, home_dir.as_deref());
    let dest_path = check_dest(&dest, ticket.name.as_deref(), &hash_hex, force_overwrite)?;

    send(
        tx,
        Event::line(
            req_id,
            format!(
                "Fetching {} from {}{}",
                ticket.hash(),
                ticket.peer_id(),
                ticket
                    .name
                    .as_deref()
                    .map(|n| format!(" ({n})"))
                    .unwrap_or_default()
            ),
        ),
    )
    .await;
    send(
        tx,
        Event::line(req_id, format!("Destination: {}", dest_path.display())),
    )
    .await;
    send(
        tx,
        Event::line(
            req_id,
            "(If interrupted, re-run this command to resume from where it stopped.)",
        ),
    )
    .await;

    // Progress events are emitted by a separate task so they don't block the
    // download future — `on_progress` is `Fn` (not async), so it can't await
    // the channel send directly.
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();
    let on_progress = move |ev: ProgressEvent| {
        let _ = progress_tx.send(ev);
    };

    let event_tx = tx.clone();
    let progress_task = tokio::spawn(async move {
        let mut current_file: Option<(usize, usize, String)> = None;
        while let Some(ev) = progress_rx.recv().await {
            match ev {
                ProgressEvent::FileStart { index, total, name } => {
                    current_file = Some((index, total, name));
                }
                ProgressEvent::Bytes { done, total } => {
                    let ipc_ev = if let Some((fi, ft, ref fname)) = current_file {
                        Event::file_progress(req_id, fi, ft, fname.clone(), done, total)
                    } else {
                        Event::progress(req_id, done, total)
                    };
                    let _ = event_tx.send(ipc_ev).await;
                }
            }
        }
    });

    let result = node
        .download_with_progress(&ticket, &dest_path, on_progress)
        .await;

    // on_progress has been dropped (download finished), so progress_tx is gone
    // and progress_rx will return None — awaiting the task is instant.
    let _ = progress_task.await;

    match result {
        Ok(()) => {
            send(tx, Event::line(req_id, "Transfer complete.")).await;
            send(tx, Event::done(req_id)).await;
            Ok(())
        }
        Err(e) => {
            let mut msg = format!("Transfer failed: {e:#}");
            if e.to_string().contains("access denied") {
                let public_id = node.endpoint.id();
                msg.push_str(&format!(
                    "\n\nYour peer-id: {public_id}\n\
                     Ask the file owner to run:\n  rdrop ring add <ring-name> {public_id}"
                ));
            }
            anyhow::bail!(msg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn dest_does_not_exist_is_accepted() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("output.txt");
        assert!(check_dest(&dest, Some("output.txt"), "deadbeef", false).is_ok());
    }

    #[test]
    fn existing_dest_without_force_overwrite_is_rejected() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("output.txt");
        std::fs::write(&dest, b"old").unwrap();
        let err = check_dest(&dest, Some("output.txt"), "deadbeef", false).unwrap_err();
        assert!(err.to_string().contains("already exists"));
        assert!(err.to_string().contains("--force-overwrite"));
    }

    #[test]
    fn existing_dest_with_force_overwrite_is_accepted() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("output.txt");
        std::fs::write(&dest, b"old").unwrap();
        assert!(check_dest(&dest, Some("output.txt"), "deadbeef", true).is_ok());
    }

    #[test]
    fn dest_is_dir_and_named_file_does_not_exist_is_accepted() {
        let dir = TempDir::new().unwrap();
        let result = check_dest(dir.path(), Some("fox.txt"), "deadbeef", false).unwrap();
        assert_eq!(result, dir.path().join("fox.txt"));
    }

    #[test]
    fn dest_is_dir_and_named_file_exists_without_force_is_rejected() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("fox.txt"), b"old").unwrap();
        let err = check_dest(dir.path(), Some("fox.txt"), "deadbeef", false).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn dest_is_dir_and_named_file_exists_with_force_is_accepted() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("fox.txt"), b"old").unwrap();
        assert!(check_dest(dir.path(), Some("fox.txt"), "deadbeef", true).is_ok());
    }

    #[test]
    fn dest_is_dir_and_no_ticket_name_falls_back_to_hash() {
        let dir = TempDir::new().unwrap();
        let hash_hex = "abc123";
        std::fs::write(dir.path().join(hash_hex), b"old").unwrap();
        let err = check_dest(dir.path(), None, hash_hex, false).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn receive_dir_explicit_dest_takes_precedence_over_config_default() {
        let got = resolve_dest(Some("/from/flag".into()), Some("/from/config".into()), None);
        assert_eq!(got, PathBuf::from("/from/flag"));
    }

    #[test]
    fn receive_dir_config_default_is_used_when_dest_is_absent() {
        let got = resolve_dest(None, Some("/from/config".into()), None);
        assert_eq!(got, PathBuf::from("/from/config"));
    }

    #[test]
    fn receive_dir_explicit_dest_is_used_when_config_default_is_absent() {
        let got = resolve_dest(Some("/from/flag".into()), None, None);
        assert_eq!(got, PathBuf::from("/from/flag"));
    }

    #[test]
    fn receive_dir_falls_back_to_current_dir_when_neither_is_set() {
        assert_eq!(resolve_dest(None, None, None), PathBuf::from("."));
    }

    #[test]
    fn expand_tilde_replaces_leading_tilde_with_home() {
        let home = PathBuf::from("/home/fake");
        let got = expand_tilde(Path::new("~/Downloads"), Some(&home));
        assert_eq!(got, PathBuf::from("/home/fake/Downloads"));
    }

    #[test]
    fn expand_tilde_bare_tilde_becomes_home() {
        let home = PathBuf::from("/home/fake");
        let got = expand_tilde(Path::new("~"), Some(&home));
        assert_eq!(got, PathBuf::from("/home/fake"));
    }

    #[test]
    fn expand_tilde_leaves_named_user_tilde_untouched() {
        let home = PathBuf::from("/home/fake");
        let got = expand_tilde(Path::new("~alice/Downloads"), Some(&home));
        assert_eq!(got, PathBuf::from("~alice/Downloads"));
    }

    #[test]
    fn expand_tilde_ignores_tilde_that_is_not_the_first_component() {
        let home = PathBuf::from("/home/fake");
        let got = expand_tilde(Path::new("/tmp/~/Downloads"), Some(&home));
        assert_eq!(got, PathBuf::from("/tmp/~/Downloads"));
    }

    #[test]
    fn expand_tilde_returns_path_unchanged_without_home() {
        let got = expand_tilde(Path::new("~/Downloads"), None);
        assert_eq!(got, PathBuf::from("~/Downloads"));
    }

    #[test]
    fn receive_dir_config_default_is_tilde_expanded() {
        let home = PathBuf::from("/home/fake");
        let got = resolve_dest(None, Some("~/Downloads".into()), Some(&home));
        assert_eq!(got, PathBuf::from("/home/fake/Downloads"));
    }

    #[test]
    fn receive_dir_explicit_dest_is_not_tilde_expanded() {
        let home = PathBuf::from("/home/fake");
        let got = resolve_dest(Some("~/Downloads".into()), None, Some(&home));
        assert_eq!(got, PathBuf::from("~/Downloads"));
    }
}
