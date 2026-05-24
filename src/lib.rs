#![deny(elided_lifetimes_in_paths)]
#![deny(unreachable_pub)]
#![warn(missing_docs)]

//! # ringdrop
//!
//! P2P file transfer with ring-based access control, built on [iroh] (QUIC +
//! hole-punching) and [iroh-blobs] (BLAKE3 chunked storage with bao encoding).
//!
//! ## Architecture
//!
//! ```text
//!         ┌─────────────────────┐
//!         │     CLI (rdrop)     │
//!         │          ↕          │
//!         │    DaemonClient     │
//!         └──────────┼──────────┘
//! ┌──────────────────↕────────────────────┐
//! │            DaemonServer               │
//! │           (owns the Node)             │
//! │  ┌───────────────┴─────────────────┐  │            
//! │  │            Node<R>              │  │
//! │  │  ┌────────────┴──────────────┐  │  │
//! │  │  │                           │  │  │
//! │  │  │  FsStore    Registry (R)  │  │  │
//! │  │  │    └───────┬──────┘       │  │  │
//! │  │  │            ↕              │  │  │
//! │  │  │         RingGate          │  │  │
//! │  │  │   (ALPN /iroh-rings/2)    │  │  │
//! │  │  │            ↕              │  │  │
//! │  │  │        Endpoint           │  │  │
//! │  │  │       (QUIC/iroh)         │  │  │
//! │  │  └───────────┼┼┼─────────────┘  │  │
//! │  └──────────────│││────────────────┘  │
//! └─────────────────│││───────────────────┘
//!                   │││
//!                 internet
//! ```
//!
//! A [`core::Node`] wraps an iroh QUIC endpoint, an iroh-blobs persistent blob
//! store, a `Registry` that tracks ring membership and permission-typed
//! resource associations, and a `RingGate` that enforces access control: a
//! blob is only served to a peer that holds [`Permission::Read`] on it — either
//! through ring membership or the built-in open ring.
//!
//! [`Permission::Read`]: iroh_rings::Permission
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
