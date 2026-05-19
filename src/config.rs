//! Node configuration: identity key and daemon port.

use std::path::Path;

use anyhow::{Context, Result};
use iroh::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};

/// Persistent node configuration, stored as JSON in the data directory.
///
/// Loaded (or created) by [`Config::load_or_create`]. The `secret_key` is
/// the node's long-term identity: it determines the [`EndpointId`] that peers
/// see and add to their rings.
///
/// [`EndpointId`]: iroh::EndpointId
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// Long-term Ed25519 secret key; determines the [`EndpointId`] peers add to their rings.
    ///
    /// [`EndpointId`]: iroh::EndpointId
    pub secret_key: SecretKey,
    /// TCP port the daemon listens on for local IPC connections (default: 60001).
    #[serde(default = "Config::default_daemon_port")]
    pub daemon_port: u16,
}

impl Config {
    fn default_daemon_port() -> u16 {
        60001
    }

    /// Returns the [`EndpointId`] (Ed25519 public key) derived from the secret key.
    ///
    /// [`EndpointId`]: iroh::EndpointId
    pub fn public_id(&self) -> EndpointId {
        self.secret_key.public()
    }

    /// Load configuration from `data_dir/config.json`, creating it with a fresh
    /// secret key if the file does not yet exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn load_or_create(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join("config.json");
        if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
        } else {
            let cfg = Config {
                secret_key: SecretKey::generate(),
                daemon_port: Self::default_daemon_port(),
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
    fn daemon_port_defaults_when_field_absent_in_existing_config() {
        let dir = tmpdir();
        let key = iroh::SecretKey::generate();
        let legacy = serde_json::json!({ "secret_key": key });
        std::fs::write(dir.path().join("config.json"), legacy.to_string()).unwrap();
        let cfg = Config::load_or_create(dir.path()).unwrap();
        assert_eq!(cfg.daemon_port, 60001);
    }

    #[test]
    fn returns_error_on_invalid_config_file() {
        let dir = tmpdir();
        std::fs::write(dir.path().join("config.json"), b"not valid json").unwrap();
        let err = Config::load_or_create(dir.path()).unwrap_err();
        assert!(err.to_string().contains("parsing"));
    }
}
