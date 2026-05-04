use serde::{Deserialize, Serialize};

/// Reserved name for the built-in open ring.
/// Blobs tagged with this are accessible to any peer without membership checks.
pub const OPEN_RING_NAME: &str = "open";

/// Identifies a ring by its user-defined name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RingId(pub String);

impl RingId {
    pub fn open() -> Self {
        RingId(OPEN_RING_NAME.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_open(&self) -> bool {
        self.0 == OPEN_RING_NAME
    }
}

impl std::fmt::Display for RingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
