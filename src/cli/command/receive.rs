use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};

use crate::config::Config;
use crate::core::Node;
use crate::core::ShareTicket;
use crate::registry::RedbRegistry;

pub fn check_dest(
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

pub async fn run(
    ticket_str: &str,
    dest: PathBuf,
    force_overwrite: bool,
    data_dir: &Path,
) -> Result<()> {
    let ticket = ShareTicket::from_uri(ticket_str)?;

    let hash_hex = ticket.hash().to_string();
    if let Err(e) = check_dest(&dest, ticket.name.as_deref(), &hash_hex, force_overwrite) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }

    let cfg = Config::load_or_create(data_dir).context("loading config")?;
    let public_id = cfg.public_id();
    let registry =
        RedbRegistry::open(data_dir.join("registry.redb")).context("opening registry")?;
    let node = Node::start(data_dir, cfg, registry).await?;

    println!(
        "Fetching {} from {}{}",
        ticket.hash(),
        ticket.peer_id(),
        ticket
            .name
            .as_deref()
            .map(|n| format!(" ({n})"))
            .unwrap_or_default()
    );
    println!("Destination: {}", dest.display());
    println!("(If interrupted, re-run this command to resume from where it stopped.)");

    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] \
                 {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );
    let on_progress = {
        let pb = pb.clone();
        move |bytes: u64, total: u64| {
            pb.set_length(total);
            pb.set_position(bytes);
        }
    };

    match node
        .download_with_progress(&ticket, &dest, on_progress)
        .await
    {
        Ok(()) => {
            pb.finish_and_clear();
            println!("Transfer complete.");
        }
        Err(e) => {
            pb.finish_and_clear();
            eprintln!("Transfer failed: {e:#}");
            if e.to_string().contains("access denied") {
                eprintln!("\nYour peer-id: {public_id}");
                eprintln!("Ask the file owner to run:");
                eprintln!("  rdrop ring add <ring-name> {public_id}");
            }
            std::process::exit(1);
        }
    }

    node.shutdown().await
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
}
