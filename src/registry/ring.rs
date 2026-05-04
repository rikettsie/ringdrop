use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The well-known "open ring" — blobs tagged with this are accessible to
/// any peer without membership checks.
///
/// The UUID is the nil UUID (all zeros), chosen to be:
///  - deterministic and stable across restarts
///  - clearly distinguishable from random UUIDs in logs and CLI output
///  - impossible to accidentally collide with a user-created ring
pub const OPEN_RING_ID: RingId = RingId(Uuid::from_bytes([0u8; 16]));

/// Human-readable name shown in CLI output for the open ring.
pub const OPEN_RING_NAME: &str = "open-ring";

/// Opaque identifier for a ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RingId(pub Uuid);

impl RingId {
    pub fn new() -> Self {
        RingId(Uuid::new_v4())
    }
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
    pub fn from_bytes(b: [u8; 16]) -> Self {
        RingId(Uuid::from_bytes(b))
    }
    /// Returns true if this is the built-in open ring.
    pub fn is_open(&self) -> bool {
        *self == OPEN_RING_ID
    }
}

impl std::fmt::Display for RingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_open() {
            write!(f, "{} (open-ring)", self.0)
        } else {
            write!(f, "{}", self.0)
        }
    }
}
