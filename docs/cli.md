# CLI Reference

Full reference for the `rdrop` command-line interface.

> **Tip:** run `rdrop help <subcommand>` for flag details on any command.

---

## `rdrop id`

Print this node's peer-id (its public key in base32). Share it with others so they can add you to their peer address book and to their rings.

```sh
rdrop id
```

---

## `rdrop daemon`

`rdrop` serves blob transfer through a background daemon and listen for CLI commands. Start it once; it keeps running until you stop it.

| Command | Description |
|---|---|
| `rdrop daemon start` | Start the daemon in the background |
| `rdrop daemon stop` | Stop a running daemon |
| `rdrop daemon status` | Show daemon status and node ID |

### Logging

By default only warnings are printed. Set `RUST_LOG` to get more detail:

```sh
RUST_LOG=ringdrop=info rdrop daemon start   # info-level logs for all ringdrop code
RUST_LOG=debug rdrop daemon start           # debug logs including iroh internals
```

This applies to every command, not just the daemon.

---

## `rdrop ring`

Manage rings. A ring is a named group of peers; blobs attached to a ring are downloadable by its members.

| Command | Description |
|---|---|
| `rdrop ring new <name>` | Create a new ring |
| `rdrop ring list` | List all rings with member counts |
| `rdrop ring add <ring> <peer-id>` | Add a peer to a ring (auto-registers in address book) |
| `rdrop ring remove <ring> <peer-id>` | Remove a peer from a ring |
| `rdrop ring members <ring>` | List members of a ring |

Examples:

```sh
rdrop ring new friends
rdrop ring list
rdrop ring add friends <peer-id>
rdrop ring remove friends <peer-id>
rdrop ring members friends
```

**Notes:**
- `ring add` auto-registers the peer in the local peer address book if not already present. You can use `rdrop peer add <peer-id> --nickname <name>` afterward to assign a nickname.
- The built-in `open` ring has no membership list — any peer can access blobs associated with this special ring.

---

## `rdrop peer`

Manage the local peer address book. Peers registered here can be given human-readable nicknames that appear consistently throughout the other command output.

| Command | Description |
|---|---|
| `rdrop peer add <peer-id>` | Register a peer; preserves any existing nickname |
| `rdrop peer add <peer-id> --nickname <name>` | Register or rename a peer |
| `rdrop peer list` | List all known peers |
| `rdrop peer remove <peer-id>` | Remove peer from address book, all rings, and all grants |

Examples:

```sh
rdrop peer add <peer-id>
rdrop peer add <peer-id> --nickname alice
rdrop peer add <peer-id> --nickname bob   # rename: run again with a different nickname
rdrop peer list
rdrop peer remove <peer-id>
```

**Notes:**
- `peer add` is idempotent: re-running with the same peer and nickname results in no operation.
- `peer add --nickname <name>` updates any existing nickname.
- `peer remove` also removes the peer from every ring and revokes all their catalog grants.
- `peer remove` errors if the peer is not in the address book (consistent with `ring remove` and `grant remove`).

---

## `rdrop import`

Shortcut for `rdrop blob import`. Import a file or directory into the local blob store and print a downloadable ticket.

Examples:

```sh
rdrop import file.txt                            # not attached to any ring — warns until attached
rdrop import file.txt --open                     # publicly accessible (anyone with the ticket)
rdrop import file.txt --ring friends             # restrict to the "friends" ring
rdrop import file.txt --ring friends --ring work # multiple rings
```

If no `--ring` or `--open` is given, the blob is stored but cannot be downloaded yet (it needs to be attached to a ring or the `"open"` one). If the file was already imported, the existing ring associations are summarised instead.

---

## `rdrop blob`

Offer blob lifecycle management.

| Command | Description |
|---|---|
| `rdrop blob import <filename>` | Import a file or directory |
| `rdrop blob list` | List all local blobs with kind, size and ring associations and tickets |
| `rdrop blob remove <filename\|hash>` | Remove a blob and all its ring associations |
| `rdrop blob attach <filename\|hash> <ring>...` | Attach a blob to one or more rings |
| `rdrop blob detach <filename\|hash> <ring>...` | Detach a blob from one or more rings |

Examples:

```sh
rdrop blob import file.txt --ring friends
rdrop blob list
rdrop blob list --ring friends             # filter by ring
rdrop blob list --peer <peer-id>           # filter by peer access
rdrop blob remove file.txt
rdrop blob attach file.txt friends
rdrop blob detach file.txt friends
```

---

## `rdrop receive`

Download a blob from an `rdrop://` ticket. Automatically resumes if interrupted — no verified data is re-transferred.

Examples:

```sh
rdrop receive rdrop://ABCDEF...
rdrop receive rdrop://ABCDEF... --dest ./downloads
rdrop receive rdrop://ABCDEF... --dest ./downloads/file.txt
```

`--dest` can be a directory (file is placed inside it) or an explicit file path.

---

## `rdrop info`

Decode a ticket and display its fields without contacting the daemon or downloading anything.

```sh
rdrop info rdrop://ABCDEF...
```

Output:

```
hash     <blake3-hash>
peer     <base32-peer-id>
relays   https://iroh.custom.relay.example.com
format   Raw
name     summer-notes.txt
```

Fields:

| Field | Description |
|---|---|
| `hash` | BLAKE3 content-addressed hash of the blob or collection root |
| `peer` | Ed25519 public key of the sender (used to identify and authenticate the QUIC connection) |
| `relays` | iroh relay servers the sender is reachable through; enables connection through NAT. `(none)` if absent |
| `format` | `Raw` for a single file, `HashSeq` for a directory/collection |
| `name` | Original filename or directory name used as the download destination; `(none)` if absent |

---

## `rdrop grant`

Grant specific rights to remote peers on your local node.

| Command | Description |
|---|---|
| `rdrop grant add <peer-id> <privilege>` | Grant a privilege to a peer |
| `rdrop grant remove <peer-id> <privilege>` | Revoke a privilege |
| `rdrop grant list` | List all grants |
| `rdrop grant list --peer <peer-id>` | Filter by peer |
| `rdrop grant list --privilege <privilege>` | Filter by privilege |

The only currently defined privilege is `blob-list`. A peer with this privilege can list the blobs they have access to on your node (they only see what their ring membership already allows them to download).

Examples:

```sh
rdrop grant add <peer-id> blob-list
rdrop grant remove <peer-id> blob-list
rdrop grant list
rdrop grant list --peer <peer-id>
rdrop grant list --privilege blob-list
```

---

## `rdrop remote`

Perform a command in a remote node.

| Command | Description |
|---|---|
| `rdrop remote blob-list <peer-id>` | List blobs accessible to you on a remote node |

The remote must have granted you the `blob-list` privilege (see `rdrop grant add`).

Example:

```sh
rdrop remote blob-list <peer-id>
```

---

## Global flags

| Flag | Description |
|---|---|
| `--data-dir <path>` | Override the data directory (default: `~/.ringdrop`) |
| `--version` | Print version |
| `--help` | Print help |

The data directory can also be set via the `RINGDROP_DATA_DIR` environment variable.
