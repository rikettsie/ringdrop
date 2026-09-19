use std::path::{Path, PathBuf};

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};

use crate::core::ShareTicket;
use crate::daemon::protocol::{EventKind, Op};

/// clap value parser for `--dest`: rejects an empty path, which would
/// otherwise be silently treated as "no destination".
pub(super) fn parse_dest(raw: &str) -> Result<PathBuf, String> {
    if raw.is_empty() {
        Err("must not be empty".to_owned())
    } else {
        Ok(PathBuf::from(raw))
    }
}

pub(crate) async fn run(
    ticket_str: &str,
    dest: Option<PathBuf>,
    force_overwrite: bool,
    data_dir: &Path,
) -> Result<()> {
    let _ = ShareTicket::from_uri(ticket_str)?;

    let client = super::daemon_client(data_dir)?;

    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.green/yellow}] \
                 {bytes}/{total_bytes} ({bytes_per_sec}, {eta}){msg}",
            )
            .unwrap()
            .progress_chars("█▷ "),
    );

    let mut error_msg: Option<String> = None;
    // (file_index, file_total, file_name) of the file currently downloading.
    let mut current_file: Option<(usize, usize, String)> = None;

    client
        .send(
            Op::Receive {
                ticket: ticket_str.to_owned(),
                dest,
                force_overwrite,
            },
            |event| match event.kind {
                EventKind::Line { text } => {
                    pb.println(text);
                }
                EventKind::Progress { done, total } => {
                    pb.set_length(total);
                    pb.set_position(done);
                }
                EventKind::FileProgress {
                    file_index,
                    file_total,
                    file_name,
                    done,
                    total,
                } => {
                    let is_new_file = current_file
                        .as_ref()
                        .is_none_or(|(fi, _, _)| *fi != file_index);
                    if is_new_file {
                        // Print a static completion line for the file that just finished.
                        if let Some((pfi, pft, pfname)) = current_file.take() {
                            pb.println(format!("  \u{2713} [{pfi}/{pft}] {pfname}"));
                        }
                        current_file = Some((file_index, file_total, file_name.clone()));
                        pb.set_message(format!(" [{file_index}/{file_total}] {file_name}"));
                    }
                    pb.set_length(total);
                    pb.set_position(done);
                }
                EventKind::Done => {
                    // Print completion line for the last file of a directory transfer.
                    if let Some((fi, ft, fname)) = current_file.take() {
                        pb.println(format!("  \u{2713} [{fi}/{ft}] {fname}"));
                    }
                    pb.finish_and_clear();
                }
                EventKind::Error { message } => {
                    current_file = None;
                    pb.finish_and_clear();
                    error_msg = Some(message);
                }
                EventKind::Record { .. } => {}
            },
        )
        .await?;

    if let Some(msg) = error_msg {
        eprintln!("error: {msg}");
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dest_rejects_empty_path() {
        assert!(parse_dest("").is_err());
    }

    #[test]
    fn parse_dest_accepts_non_empty_path() {
        assert_eq!(parse_dest("./downloads"), Ok(PathBuf::from("./downloads")));
    }
}
