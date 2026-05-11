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

#[derive(Debug, Clone)]
pub struct ShareTicket {
    addr: EndpointAddr,
    hash: Hash,
    format: BlobFormat,
    pub name: Option<String>,
}

impl ShareTicket {
    pub fn new(addr: EndpointAddr, hash: Hash, name: Option<String>) -> Self {
        ShareTicket {
            addr,
            hash,
            format: BlobFormat::Raw,
            name,
        }
    }

    pub fn new_collection(addr: EndpointAddr, hash: Hash, name: Option<String>) -> Self {
        ShareTicket {
            addr,
            hash,
            format: BlobFormat::HashSeq,
            name,
        }
    }

    pub fn hash(&self) -> Hash {
        self.hash
    }
    pub fn format(&self) -> BlobFormat {
        self.format
    }
    pub fn node_addr(&self) -> &EndpointAddr {
        &self.addr
    }
    pub fn peer_id(&self) -> EndpointId {
        self.addr.id
    }

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
