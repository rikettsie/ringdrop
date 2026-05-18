//! # ringdrop
//!
//! P2P file transfer with ring-based access control, built on [iroh] (QUIC +
//! hole-punching) and [iroh-blobs] (BLAKE3 chunked storage with bao encoding).
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │  CLI (rdrop)  <->  DaemonClient  <->  DaemonServer  │
//! └──────────────────────────┬──────────────────────────┘
//!                            │
//!                         Node<R>
//!                     ┌──────┴───────┐
//!                Endpoint         FsStore
//!              (QUIC/iroh)      (iroh-blobs)
//!                     └──────┬───────┘
//!                         RingGate
//!             (iroh-rings, ALPN /iroh-rings/1)
//! ```
//!
//! A [`core::Node`] wraps an iroh QUIC endpoint, an iroh-blobs persistent blob
//! store, and a `RingGate` that enforces ring-based access control: a blob is
//! only served to a peer whose [`EndpointId`] is a member of at least one ring
//! the blob has been tagged with.
//!
//! The [`daemon`] module runs a `Node` as a background TCP server so that the
//! CLI can talk to it over a local IPC connection.
//!
//! [`EndpointId`]: iroh::EndpointId

pub mod cli;
pub mod config;
pub mod core;
pub mod daemon;
pub mod util;
