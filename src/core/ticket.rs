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
    /// Creates a ticket for a single raw blob (`BlobFormat::Raw`).
    pub fn new(addr: EndpointAddr, hash: Hash, name: Option<String>) -> Self {
        ShareTicket {
            addr,
            hash,
            format: BlobFormat::Raw,
            name,
        }
    }

    /// Creates a ticket for a directory / collection (`BlobFormat::HashSeq`).
    pub fn new_collection(addr: EndpointAddr, hash: Hash, name: Option<String>) -> Self {
        ShareTicket {
            addr,
            hash,
            format: BlobFormat::HashSeq,
            name,
        }
    }

    /// Creates a ticket with an explicit `format`.
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

    /// Encodes the ticket as a `rdrop://` URI.
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

    /// Decodes a `rdrop://` URI produced by [`ShareTicket::to_uri`].
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

#[cfg(test)]
mod tests {
    use data_encoding::BASE32_NOPAD;
    use iroh::{EndpointAddr, SecretKey};
    use iroh_blobs::{BlobFormat, Hash};

    use super::*;

    fn make_addr() -> EndpointAddr {
        EndpointAddr::new(SecretKey::generate().public())
    }

    fn test_hash() -> Hash {
        Hash::from_bytes([0xab; 32])
    }

    #[test]
    fn raw_ticket_round_trips_through_uri() {
        let addr = make_addr();
        let hash = test_hash();
        let ticket = ShareTicket::new(addr.clone(), hash, Some("file.txt".into()));

        let uri = ticket.to_uri().unwrap();
        assert!(uri.starts_with("rdrop://"));

        let decoded = ShareTicket::from_uri(&uri).unwrap();
        assert_eq!(decoded.hash(), hash);
        assert_eq!(decoded.format(), BlobFormat::Raw);
        assert_eq!(decoded.name.as_deref(), Some("file.txt"));
        assert_eq!(decoded.peer_id(), addr.id);
    }

    #[test]
    fn collection_ticket_round_trips_through_uri() {
        let addr = make_addr();
        let ticket = ShareTicket::new_collection(addr, test_hash(), None);

        let uri = ticket.to_uri().unwrap();
        let decoded = ShareTicket::from_uri(&uri).unwrap();
        assert_eq!(decoded.format(), BlobFormat::HashSeq);
        assert_eq!(decoded.name, None);
    }

    #[test]
    fn nameless_ticket_round_trips() {
        let ticket = ShareTicket::new(make_addr(), test_hash(), None);
        let decoded = ShareTicket::from_uri(&ticket.to_uri().unwrap()).unwrap();
        assert_eq!(decoded.name, None);
    }

    #[test]
    fn from_format_preserves_explicit_format() {
        let ticket = ShareTicket::from_format(make_addr(), test_hash(), BlobFormat::HashSeq, None);
        assert_eq!(ticket.format(), BlobFormat::HashSeq);
    }

    #[test]
    fn accessors_return_construction_values() {
        let addr = make_addr();
        let hash = test_hash();
        let ticket = ShareTicket::new(addr.clone(), hash, Some("test.bin".into()));

        assert_eq!(ticket.hash(), hash);
        assert_eq!(ticket.format(), BlobFormat::Raw);
        assert_eq!(ticket.peer_id(), addr.id);
        assert_eq!(ticket.node_addr().id, addr.id);
    }

    #[test]
    fn from_uri_rejects_missing_rdrop_prefix() {
        let err = ShareTicket::from_uri("https://example.com/abc").unwrap_err();
        assert!(err.to_string().contains("rdrop://"));
    }

    #[test]
    fn from_uri_rejects_bad_base32_payload() {
        let err = ShareTicket::from_uri("rdrop://!!!not-base32!!!").unwrap_err();
        assert!(err.to_string().contains("base32"));
    }

    #[test]
    fn from_uri_rejects_valid_base32_but_invalid_json() {
        let payload = BASE32_NOPAD.encode(b"not json at all").to_lowercase();
        let uri = format!("rdrop://{payload}");
        let err = ShareTicket::from_uri(&uri).unwrap_err();
        assert!(err.to_string().contains("json"));
    }
}
