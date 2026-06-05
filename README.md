<h1 align="center">Ringdrop</h1>

[![Tests](https://github.com/rikettsie/ringdrop/actions/workflows/tests.yml/badge.svg)](https://github.com/rikettsie/ringdrop/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/rikettsie/ringdrop/graph/badge.svg)](https://codecov.io/gh/rikettsie/ringdrop)
[![crates.io](https://img.shields.io/crates/v/ringdrop.svg)](https://crates.io/crates/ringdrop)
[![docs.rs](https://docs.rs/ringdrop/badge.svg)](https://docs.rs/ringdrop)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE-MIT)

<img width="38%" align="right" src="https://raw.githubusercontent.com/rikettsie/ringdrop/main/docs/mascot.png">

> **Ringdrop** is a streamed P2P file transfer tool,
> with *ring-based* access control, built on [iroh-blobs](https://github.com/n0-computer/iroh-blobs)
> and [iroh-rings](https://github.com/rikettsie/iroh-rings).

To share a file, associate it with one or more rings and get back an `rdrop://` ticket to hand to peers.
Only peers who are members of those rings can download it.
Transfers resume automatically if interrupted — no verified data is re-transferred after a crash or disconnect.

Access control is enforced at the connection level. Ring–resource associations carry typed permissions (`Read`, `Write`, `Delete`). When a peer requests to download a blob, the sender checks that the peer holds `Read` permission on it — either through ring membership or the built-in open ring — and denies the transfer before any data is sent if not.

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
| All platforms (pre-built bin) | `cargo binstall ringdrop` |

For prerequisites, alternative methods, and troubleshooting see [docs/install.md](docs/install.md).

## Commands

Full reference: [docs/cli.md](docs/cli.md)

| Command | Description |
|---|---|
| `rdrop id` | Print your peer-id so others can add you to their rings |
| `rdrop daemon` | Start, stop, and inspect the background daemon |
| `rdrop ring` | Manage rings (create, list, add/remove peers, view members) |
| `rdrop peer` | Manage the local peer address book with optional nicknames |
| `rdrop import` | Import a file or directory and get a shareable ticket |
| `rdrop blob` | Full blob lifecycle: import, list, remove |
| `rdrop tag` | Associate an already-imported blob with a ring (or mark it open) |
| `rdrop untag` | Remove ring associations from a blob (revoke access) |
| `rdrop receive` | Download from a ticket (automatically resumes if interrupted) |
| `rdrop grant` | Grant specific rights to remote peers on your local node |
| `rdrop remote` | Perform a command in a remote node |

## Custom relay

By default ringdrop uses the public relay infrastructure provided by [Number 0](https://n0.computer) (`relay.iroh.network`). You can point the node to a relay you control by adding a `relay_url` field to `config.json` (located in your data directory, `~/.local/share/ringdrop/` by default):

```json
{
  "relay_url": "https://relay.yourdomain.com"
}
```

When the field is absent the default n0 relay is used. An invalid URL or an unreachable relay causes the daemon to fail at startup with a clear error rather than silently falling back.

> **Self-hosting a relay**: see the [iroh-relay README](https://github.com/n0-computer/iroh/tree/main/iroh-relay) for instructions on running your own relay instance. Peer discovery (pkarr/DNS) continues to use n0's `iroh.link` infrastructure regardless of which relay you choose.

## Contributing

If you have ideas/contributions or anything is not working the way you expect (in which case, please include an output with `RUST_LOG=debug`) and feel free to open an issue or PR.

After cloning, activate the pre-commit hooks (it runs `cargo fmt --check` and `cargo clippy` before every commit, and tag verifications before every push):

```sh
git config core.hooksPath .githooks
```

## Wire protocols

| ALPN | Purpose |
|---|---|
| `/iroh-rings/2` | Blob transfer — gate enforces ring membership and `Read` permission before any data is transferred |
| `/ringdrop/catalog/0` | Catalog queries — lets a peer list the blobs accessible to them on a remote node (requires `blob-list` grant) |

## Notable dependencies

`ringdrop` is built on:

| Crate | Role |
|---|---|
| [iroh](https://github.com/n0-computer/iroh) | QUIC transport, NAT traversal, relay fallback |
| [iroh-rings](https://github.com/rikettsie/iroh-rings) | Ring-based access control, grant management |
| [iroh-blobs](https://github.com/n0-computer/iroh-blobs) | BLAKE3 chunking, `FsStore`, verified streaming |
| [bao-tree](https://github.com/n0-computer/bao-tree) | bao encoding/decoding, `ChunkRanges`, bitfield conversion |
| [redb](https://github.com/cberner/redb) | Embedded persistent store for the ring registry |
| [tokio](https://tokio.rs) | Async runtime |

## License

MIT — see [LICENSE](LICENSE).
