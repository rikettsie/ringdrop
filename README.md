# ringdrop

[![Tests](https://github.com/rikettsie/ringdrop/actions/workflows/tests.yml/badge.svg)](https://github.com/rikettsie/ringdrop/actions/workflows/tests.yml)
[![crates.io](https://img.shields.io/crates/v/ringdrop.svg)](https://crates.io/crates/ringdrop)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE-MIT)

`rdrop` is a streamed P2P file transfer tool with *ring-based* access control, built on [iroh-blobs](https://github.com/n0-computer/iroh-blobs) and [iroh-rings](https://github.com/rikettsie/iroh-rings).

To share a file, associate it with one or more rings and get back an `rdrop://` ticket to hand to peers.
Only peers who are members of those rings can download it.
Transfers resume automatically if interrupted — no verified data is re-transferred after a crash or disconnect.

Access control is enforced at the connection level via an ALPN protocol (`/iroh-rings/0`). When a peer requests a blob, the sender checks whether that peer's `peer-id` belongs to any ring the blob is associated with. If not, the transfer is denied before any data is sent.

## Features

- **Ring-based access control** — share with specific peers or groups via private rings, or open to everyone
- **Crash-safe resumption** — BLAKE3 bitfield tracks verified chunks; interrupted downloads pick up where they left off
- **Verified streaming** — every 16 KiB chunk is verified against the BLAKE3 hash tree before being written to disk
- **Directory support** — import and share entire directories as a single ticket

## Install

| Platform | Quick command |
|---|---|
| Linux | `cargo install ringdrop` |
| macOS | `brew tap rikettsie/tap && brew install rdrop` |
| Windows (PowerShell) | `scoop bucket add rikettsie https://github.com/rikettsie/scoop-bucket; scoop install rdrop` |

For prerequisites, alternative methods, and troubleshooting see [install/INSTALL.md](install/INSTALL.md).

## Usage

### Print your `peer-id`

Share your `peer-id` (i.e. your node public id) with others so they can add you to their rings:

```sh
rdrop id
```

### Manage rings

```sh
rdrop ring new <ring-name>                                   # create a private ring
rdrop ring list                                              # list all rings
rdrop ring add <ring-name> <peer-id>                         # add a peer to a ring
rdrop ring add <ring-name> <peer-id> --nickname <nickname>   # with a display label
rdrop ring remove <ring-name> <peer-id>                      # remove a peer
rdrop ring members <ring-name>                               # list peers of a ring
```

### Import and manage files (blobs)

**Import** a file or directory into the local blob store and produce a ticket:

```sh
rdrop import <file-name>                    # shortcut, warns if not associated with any ring
rdrop import <file-name> --open             # publicly accessible
rdrop import <file-name> --tag <ring-name>  # restrict to a ring

rdrop blob import <file-name> --open        # same, via blob subcommand
```

If no `--ring` or `--open` is given, then the file is not associated with any ring and a warning is printed. The blob cannot be transferred until it is associated with a ring.
If the file was already imported, the existing rings are summarised instead.

`rdrop blob` groups all blob lifecycle operations. `rdrop import` is a shortcut for `rdrop blob import`.

**List** all local blobs with their ring tags and share ticket:

```sh
rdrop blob list
```

**Remove** a blob from the local store and all its ring associations:

```sh
rdrop blob remove <file-name>
rdrop blob remove <hash>
```

### Grant or change access

Associate a file with one or more rings:

```sh
rdrop tag <file-name> --ring <ring-name>   # restrict to a ring
rdrop tag <file-name> --open               # anyone with the ticket
rdrop tag <hash>   --ring <ring-name>   # same, by BLAKE3 hash
rdrop tag <hash>   --open
```

### Start the daemon

`rdrop` serves blobs through a background daemon. Start it once; it keeps running until you stop it:

```sh
rdrop daemon start    # start in the background
rdrop daemon status   # show status and node ID
rdrop daemon stop     # stop the daemon
```

The daemon serves every blob that has been associated with a ring — there is no per-file serving step to do.

### Receive a file

```sh
rdrop receive rdrop://ABCDEF... [--dest ./downloads]
```

Re-run the same command to resume an interrupted transfer.

## Activate more logging

By default only warnings are printed. Set `RUST_LOG` to get more detail:

```sh
RUST_LOG=ringdrop=info rdrop daemon start   # info-level logs for all ringdrop code
RUST_LOG=debug rdrop daemon start           # debug logs including iroh internals
```

This applies to every command, not just the daemon.

## Contributing

After cloning, activate the pre-commit hooks (it runs `cargo fmt --check` and `cargo clippy` before every commit, and tag verifications before every push):

```sh
git config core.hooksPath .githooks
```

## Dependencies

`ringdrop` is built on:

| Crate | Role |
|---|---|
| [iroh](https://github.com/n0-computer/iroh) | QUIC transport, NAT traversal, relay fallback |
| [iroh-blobs](https://github.com/n0-computer/iroh-blobs) | BLAKE3 chunking, `FsStore`, verified streaming |
| [bao-tree](https://github.com/n0-computer/bao-tree) | bao encoding/decoding, `ChunkRanges`, bitfield conversion |
| [redb](https://github.com/cberner/redb) | Embedded persistent store for the ring registry |
| [tokio](https://tokio.rs) | Async runtime |

## License

MIT — see [LICENSE](LICENSE).
