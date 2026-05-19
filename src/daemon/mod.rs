//! Background daemon: TCP-based IPC server and client.
//!
//! The daemon runs a [`Node`] in the background and exposes it over a local
//! TCP socket. CLI commands talk to it via [`DaemonClient`], which sends a
//! single [`protocol::Request`] (newline-terminated JSON) and reads back a
//! stream of [`protocol::Event`]s.
//!
//! [`Node`]: crate::core::Node
//! [`DaemonClient`]: client::DaemonClient

pub mod client;
pub mod protocol;
pub mod server;

// 512 KiB covers any realistic JSON request (long --data-dir paths, many
// --ring flags, long ticket URIs) while still bounding per-connection memory.
pub(crate) const MAX_LINE_BYTES: usize = 512 * 1024;
