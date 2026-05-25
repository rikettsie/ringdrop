use anyhow::{Context, Result};
use data_encoding::BASE32_NOPAD;
use iroh::{EndpointAddr, EndpointId};
use iroh_blobs::{BlobFormat, Hash};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TicketWire {
    addr: EndpointAddr,
    hash: Hash,
    format: BlobFormat,
    name: Option<String>,
}

/// An out-of-band transfer ticket that encodes everything a receiver needs.
///
/// Tickets are serialised to a `rdrop://` URI (base32-encoded JSON) for easy
/// copy-paste. Pass one to [`Node::download`] to fetch the blob.
///
/// [`Node::download`]: crate::core::Node::download
#[derive(Debug, Clone)]
pub struct ShareTicket {
    addr: EndpointAddr,
    hash: Hash,
    format: BlobFormat,
    /// Human-readable display name, typically the original filename; `None` when unknown.
    pub name: Option<String>,
}

impl ShareTicket {
    /// Create a ticket for a single raw blob (`BlobFormat::Raw`).
    pub fn new(addr: EndpointAddr, hash: Hash, name: Option<String>) -> Self {
        ShareTicket {
            addr,
            hash,
            format: BlobFormat::Raw,
            name,
        }
    }

    /// Create a ticket for a directory / collection (`BlobFormat::HashSeq`).
    pub fn new_collection(addr: EndpointAddr, hash: Hash, name: Option<String>) -> Self {
        ShareTicket {
            addr,
            hash,
            format: BlobFormat::HashSeq,
            name,
        }
    }

    /// Create a ticket with an explicit `format`.
    pub fn from_format(
        addr: EndpointAddr,
        hash: Hash,
        format: BlobFormat,
        name: Option<String>,
    ) -> Self {
        ShareTicket {
            addr,
            hash,
            format,
            name,
        }
    }

    /// Returns the BLAKE3 root hash of the blob or collection.
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// Returns the blob format (`Raw` for files, `HashSeq` for directories).
    pub fn format(&self) -> BlobFormat {
        self.format
    }

    /// Returns the network address of the node that issued this ticket.
    pub fn node_addr(&self) -> &EndpointAddr {
        &self.addr
    }

    /// Returns the [`EndpointId`] (Ed25519 public key) of the issuing node.
    ///
    /// [`EndpointId`]: iroh::EndpointId
    pub fn peer_id(&self) -> EndpointId {
        self.addr.id
    }

    /// Encode the ticket as a `rdrop://` URI.
    ///
    /// The URI is base32-encoded JSON and can be decoded with [`ShareTicket::from_uri`].
    ///
    /// # Errors
    ///
    /// Returns an error if the ticket cannot be serialised to JSON.
    pub fn to_uri(&self) -> Result<String> {
        let wire = TicketWire {
            addr: self.addr.clone(),
            hash: self.hash,
            format: self.format,
            name: self.name.clone(),
        };
        let json = serde_json::to_string(&wire).context("serializing ticket")?;
        let encoded = BASE32_NOPAD.encode(json.as_bytes()).to_lowercase();
        Ok(format!("rdrop://{encoded}"))
    }

    /// Decode a `rdrop://` URI produced by [`ShareTicket::to_uri`].
    ///
    /// # Errors
    ///
    /// Returns an error if the URI is missing the `rdrop://` prefix, the
    /// base32 payload cannot be decoded, or the inner JSON is malformed.
    pub fn from_uri(s: &str) -> Result<Self> {
        let encoded = s
            .strip_prefix("rdrop://")
            .context("invalid ticket: expected 'rdrop://' prefix")?;
        let bytes = BASE32_NOPAD
            .decode(encoded.to_uppercase().as_bytes())
            .context("invalid ticket: base32 decode failed")?;
        let wire: TicketWire =
            serde_json::from_slice(&bytes).context("invalid ticket: json parse failed")?;
        Ok(ShareTicket {
            addr: wire.addr,
            hash: wire.hash,
            format: wire.format,
            name: wire.name,
        })
    }
}
