use std::path::Path;

use anyhow::{Context, Result};
use iroh::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub secret_key: SecretKey,
}

impl Config {
    pub fn public_id(&self) -> EndpointId {
        self.secret_key.public()
    }

    pub fn load_or_create(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join("config.json");
        if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
        } else {
            let cfg = Config {
                secret_key: SecretKey::generate(),
            };
            let raw = serde_json::to_string_pretty(&cfg)?;
            std::fs::write(&path, raw).with_context(|| format!("writing {}", path.display()))?;
            Ok(cfg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn creates_config_file_on_first_call() {
        let dir = tmpdir();
        assert!(!dir.path().join("config.json").exists());
        Config::load_or_create(dir.path()).unwrap();
        assert!(dir.path().join("config.json").exists());
    }

    #[test]
    fn returns_same_key_on_repeated_calls() {
        let dir = tmpdir();
        let first = Config::load_or_create(dir.path()).unwrap();
        let second = Config::load_or_create(dir.path()).unwrap();
        assert_eq!(first.secret_key.to_bytes(), second.secret_key.to_bytes());
    }

    #[test]
    fn returns_error_on_invalid_config_file() {
        let dir = tmpdir();
        std::fs::write(dir.path().join("config.json"), b"not valid json").unwrap();
        let err = Config::load_or_create(dir.path()).unwrap_err();
        assert!(err.to_string().contains("parsing"));
    }
}
