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
//!             ┌─────────────────────┐
//!             │     CLI (rdrop)     │
//!             │          ↕          │
//!             │    DaemonClient     │
//!             └──────────┼──────────┘
//! ┌──────────────────────↕───────────────────────┐
//! │                 DaemonServer                 │
//! │               (owns the Node)                │
//! │  ┌───────────────────┴────────────────────┐  │
//! │  │                  Node                  │  │
//! │  │  ┌──────────────────────────────────┐  │  │
//! │  │  │ FsStore  Registry  Grants  Peers │  │  │
//! │  │  │    └───────────┬─────────────┘   │  │  │
//! │  │  │  ┌─────────────┴─────────────┐   │  │  │
//! │  │  │  │        RingGate           │   │  │  │
//! │  │  │  │    (/iroh-rings/2)        │   │  │  │
//! │  │  │  ├───────────────────────────┤   │  │  │
//! │  │  │  │     CatalogHandler        │   │  │  │
//! │  │  │  │  (/ringdrop/catalog/0)    │   │  │  │
//! │  │  │  └─────────────┬─────────────┘   │  │  │
//! │  │  │            Endpoint              │  │  │
//! │  │  │           (QUIC/iroh)            │  │  │
//! │  │  └───────────────┼┼┼────────────────┘  │  │
//! │  └──────────────────┼┼┼───────────────────┘  │
//! └─────────────────────┼┼┼──────────────────────┘
//!                    internet
//! ```
//!
//! A [`core::Node`] wraps an iroh QUIC endpoint, an iroh-blobs persistent blob
//! store, a `Registry` that tracks ring membership and permission-typed
//! resource associations, and two protocol handlers registered on the endpoint:
//! a `RingGate` (`/iroh-rings/2`) that enforces access control — a blob is only
//! served to a peer that holds [`Permission::Read`] on it — and a
//! `CatalogHandler` (`/ringdrop/catalog/0`) that lets authorised peers query
//! the local blob list.
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
pub(crate) mod local_store;
pub mod util;
