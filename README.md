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

To build the binary in release mode and install it in your OS default path (e.g. `~/.cargo/bin/rdrop` on Linux and macOS), just run:

```sh
cargo install --path .
```

After that, `rdrop` is callable from anywhere in the shell without any further configuration.

## Usage

### Print your PeerId

Share your PeerId with others so they can add you to their rings:

```sh
rdrop id
```

### Manage rings

```sh
rdrop ring new <name>                                        # create a private ring
rdrop ring list                                              # list all rings
rdrop ring add <ring-name> <peer-id>                         # add a peer to a ring
rdrop ring add <ring-name> <peer-id> --nickname "Alice"      # with a display label
rdrop ring remove <ring-name> <peer-id>                      # remove a peer
rdrop ring members <ring-name>                               # list peers of a ring
```

### Import and manage files (blobs)

`rdrop blob` groups all blob lifecycle operations. `rdrop import` is a shortcut for `rdrop blob import`.

**Import** a file or directory into the local blob store and get a ticket:

```sh
rdrop import file.txt                    # shortcut — warns if untagged
rdrop import file.txt --open             # publicly accessible
rdrop import file.txt --tag friends      # restrict to a ring

rdrop blob import file.txt --open        # same, via blob subcommand
```

If no `--tag` or `--open` is given and the file has no existing tags, a warning is printed — the blob won't be transferred until it is tagged.
If the file was already imported, the existing rings are summarised instead.

**List** all local blobs with their ring tags and share ticket:

```sh
rdrop blob list
```

**Remove** a blob from the local store and all its ring tags:

```sh
rdrop blob remove file.txt
rdrop blob remove <hash>
```

Disk space is reclaimed on the next `rdrop serve` run (GC cycle).

### Grant or change access

Tag a file with a ring at any time — before or after importing.

```sh
rdrop tag file.txt --ring friends   # restrict to a ring
rdrop tag file.txt --open           # anyone with the ticket
rdrop tag <hash>   --ring friends   # same, by BLAKE3 hash
rdrop tag <hash>   --open
```

### Serve

Start the node and serve all authorised blobs until `Ctrl-C`:

```sh
rdrop serve
```

Keep this running while peers download. The same node serves every blob that has been tagged — there is no per-file serving step.

### Receive a file

```sh
rdrop receive rdrop://ABCDEF... [--dest ./downloads]
```

Re-run the same command to resume an interrupted transfer.

## Activate more logging

By default only warnings are printed. Set `RUST_LOG` to get more detail:

```sh
RUST_LOG=ringdrop=info rdrop serve      # info-level logs for all ringdrop code
RUST_LOG=debug rdrop serve              # debug logs including iroh internals
```

This applies to every command, not just `serve`.

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
