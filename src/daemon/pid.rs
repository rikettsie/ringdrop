use std::path::{Path, PathBuf};

use anyhow::Result;

pub fn path(data_dir: &Path) -> PathBuf {
    data_dir.join("daemon.pid")
}

pub fn write(data_dir: &Path) -> Result<()> {
    std::fs::write(path(data_dir), std::process::id().to_string())?;
    Ok(())
}

pub fn read(data_dir: &Path) -> Option<u32> {
    std::fs::read_to_string(path(data_dir))
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

pub fn remove(data_dir: &Path) {
    let _ = std::fs::remove_file(path(data_dir));
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn write_and_read_round_trips() {
        let dir = tmpdir();
        write(dir.path()).unwrap();
        let pid = read(dir.path()).unwrap();
        assert_eq!(pid, std::process::id());
    }

    #[test]
    fn read_returns_none_when_file_absent() {
        let dir = tmpdir();
        assert!(read(dir.path()).is_none());
    }

    #[test]
    fn remove_deletes_file() {
        let dir = tmpdir();
        write(dir.path()).unwrap();
        assert!(path(dir.path()).exists());
        remove(dir.path());
        assert!(!path(dir.path()).exists());
    }

    #[test]
    fn remove_is_idempotent_when_file_absent() {
        let dir = tmpdir();
        remove(dir.path()); // must not panic
    }
}
