use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Reserved name for the built-in open ring (the public ring).
/// Properties tagged with this are accessible to any peer without membership checks.
pub const OPEN_RING_NAME: &str = "open";

/// Identifies a ring by its user-defined name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ring {
    pub name: String,

    #[serde(with = "serde_millis")]
    pub timestamp: Instant,
}

impl Ring {
    pub fn new<S: AsRef<str>>(name: S) -> Result<Self> {
        let name = name.as_ref();
        if name.is_empty() {
            return Err(anyhow!("ring name must not be empty"));
        }
        if name.contains(|c: char| c.is_whitespace() || c == '\0') {
            return Err(anyhow!(
                "ring name must not contain whitespace or NUL bytes"
            ));
        }
        Ok(Self {
            name: name.to_string(),
            timestamp: Instant::now(),
        })
    }

    pub fn new_open() -> Self {
        Self {
            name: OPEN_RING_NAME.to_string(),
            timestamp: Instant::now(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.name
    }

    pub fn is_open(&self) -> bool {
        self.name == OPEN_RING_NAME
    }
}

impl std::fmt::Display for Ring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
