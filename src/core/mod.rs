//! Core P2P node: QUIC transport, blob storage, and ring-gated transfers.
//!
//! The central type is [`Node`], which owns an iroh [`Endpoint`], an
//! iroh-blobs [`FsStore`], and a `RingGate`. Call [`Node::start`] to bring
//! a node online, then use [`Node::import_file`] / [`Node::import_directory`]
//! to add blobs and [`Node::download`] to fetch them from remote peers via a
//! [`ShareTicket`].
//!
//! [`Endpoint`]: iroh::Endpoint
//! [`FsStore`]: iroh_blobs::store::fs::FsStore

mod node;
mod protocol;
mod ticket;

pub use node::Node;
pub use ticket::ShareTicket;
