# ringdrop

`rdrop` is a P2P file transfer tool with *ring-based* access control.
Built on top of [iroh](https://github.com/n0-computer/iroh) and [iroh-blobs](https://github.com/n0-computer/iroh-blobs) verified streaming.

To share a file, tag it with one or more rings and get back an `rdrop://` ticket to hand to peers.
Only peers who are members of those rings can download it.
Transfers resume automatically if interrupted — no verified data is re-transferred after a crash or disconnect.

## Features

- **Ring-based access control** — share with specific peers or groups via private rings, or open to everyone
- **Crash-safe resumption** — BLAKE3 bitfield tracks verified chunks; interrupted downloads pick up where they left off
- **Verified streaming** — every 16 KiB chunk is verified against the BLAKE3 hash tree before being written to disk
- **Directory support** — import and share entire directories as a single ticket

## Install

```sh
cargo install --path .
```

## Usage

### Print your PeerId

Share your PeerId with others so they can add you to their rings:

```sh
rdrop id
```

### Manage rings

```sh
rdrop ring new                            # create a private ring
rdrop ring list                           # list all rings
rdrop ring add <ring-id> <peer-id>        # add a peer to a ring
rdrop ring remove <ring-id> <peer-id>     # remove a peer from a ring
rdrop ring members <ring-id>              # list peers of a ring
```

### Share a file

```sh
rdrop share file.txt
rdrop share file.txt --name "my report"
```

This imports the file, prints an `rdrop://` ticket, and keeps the node running to serve downloads. Press `Ctrl-C` to stop serving.

### Grant access

Access control is managed separately from sharing. Tag a file with a ring to grant access to its members, or mark it open so anyone with the ticket can download it.

```sh
rdrop tag file.txt --ring <uuid>   # restrict to a ring
rdrop tag file.txt --open          # anyone with the ticket
rdrop tag <hash>    --ring <uuid>  # same, by hash
rdrop tag <hash>    --open
```

### Receive a file

```sh
rdrop receive rdrop://ABCDEF... [--dest ./downloads]
```

Re-run the same command to resume an interrupted transfer.

## How it works

`ringdrop` is built on top of:

| Crate | Role |
|---|---|
| [iroh](https://github.com/n0-computer/iroh) | QUIC transport, NAT traversal, relay fallback |
| [iroh-blobs](https://github.com/n0-computer/iroh-blobs) | BLAKE3 chunking, `FsStore`, verified streaming |
| [bao-tree](https://github.com/n0-computer/bao-tree) | bao encoding/decoding, `ChunkRanges`, bitfield conversion |
| [redb](https://github.com/cberner/redb) | Embedded persistent store for the ring registry |
| [tokio](https://tokio.rs) | Async runtime |

Access control is enforced at the connection level via a custom ALPN protocol (`iroh/ring/1`). When a peer requests a blob, the sender checks whether that peer's `PeerId` belongs to any ring the blob is tagged with. If not, the transfer is denied before any data is sent.

## License

MIT — see [LICENSE](LICENSE).
