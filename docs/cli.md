# CLI Reference

Full reference for the `rdrop` command-line interface.

> **Tip:** run `rdrop help <subcommand>` for flag details on any command.

---

## `rdrop id`

Print this node's peer-id (its public key in base32). Share it with others so they can add you to their rings.

```sh
rdrop id
```

---

## `rdrop daemon`

`rdrop` serves blobs through a background daemon. Start it once; it keeps running until you stop it.

| Command | Description |
|---|---|
| `rdrop daemon start` | Start the daemon in the background |
| `rdrop daemon stop` | Stop a running daemon |
| `rdrop daemon status` | Show daemon status and node ID |

```sh
rdrop daemon start
rdrop daemon status
rdrop daemon stop
```

The daemon serves every blob that has been associated with a ring — there is no per-file serving step.

### Logging

By default only warnings are printed. Set `RUST_LOG` to get more detail:

```sh
RUST_LOG=ringdrop=info rdrop daemon start   # info-level logs for all ringdrop code
RUST_LOG=debug rdrop daemon start           # debug logs including iroh internals
```

This applies to every command, not just the daemon.

---

## `rdrop ring`

Manage rings. A ring is a named group of peers; blobs tagged with a ring are downloadable by its members.

| Command | Description |
|---|---|
| `rdrop ring new <name>` | Create a new ring |
| `rdrop ring list` | List all rings with member counts |
| `rdrop ring add <ring> <peer-id>` | Add a peer to a ring (auto-registers in address book) |
| `rdrop ring remove <ring> <peer-id>` | Remove a peer from a ring |
| `rdrop ring members <ring>` | List members of a ring |

```sh
rdrop ring new friends
rdrop ring list
rdrop ring add friends <peer-id>
rdrop ring remove friends <peer-id>
rdrop ring members friends
```

**Notes:**
- `ring add` auto-registers the peer in the local address book if not already present. Use `rdrop peer add <peer-id> --nickname <name>` afterward to assign a nickname.
- The built-in `open` ring has no membership list — any peer can access blobs tagged with it.

---

## `rdrop peer`

Manage the local peer address book. Peers registered here can be given human-readable nicknames that appear in `ring members` and other output.

| Command | Description |
|---|---|
| `rdrop peer add <peer-id>` | Register a peer; preserves any existing nickname |
| `rdrop peer add <peer-id> --nickname <name>` | Register or rename a peer |
| `rdrop peer list` | List all known peers |
| `rdrop peer remove <peer-id>` | Remove peer from address book, all rings, and all grants |

```sh
rdrop peer add <peer-id>
rdrop peer add <peer-id> --nickname alice
rdrop peer add <peer-id> --nickname bob   # rename: run again with a different nickname
rdrop peer list
rdrop peer remove <peer-id>
```

**Notes:**
- `peer add` is idempotent: re-running with the same peer and nickname is a no-op.
- `peer add --nickname <name>` updates any existing nickname.
- `peer remove` also removes the peer from every ring and revokes all their catalog grants.
- `peer remove` errors if the peer is not in the address book (consistent with `ring remove` and `grant remove`).

---

## `rdrop import`

Shortcut for `rdrop blob import`. Import a file or directory into the local blob store and print a downloadable ticket.

```sh
rdrop import <file>                        # untagged — warns until tagged
rdrop import <file> --open                 # publicly accessible (anyone with the ticket)
rdrop import <file> --ring friends         # restrict to the "friends" ring
rdrop import <file> --ring friends --ring work  # multiple rings
```

If no `--ring` or `--open` is given the blob is stored but cannot be downloaded until tagged. If the file was already imported, the existing ring associations are summarised instead.

---

## `rdrop blob`

Full blob lifecycle management.

| Command | Description |
|---|---|
| `rdrop blob import <file>` | Import and tag a file or directory |
| `rdrop blob list` | List all local blobs with ring tags and tickets |
| `rdrop blob remove <file\|hash>` | Remove a blob and all its ring associations |

```sh
rdrop blob import file.txt --ring friends
rdrop blob list
rdrop blob list --ring friends             # filter by ring
rdrop blob list --peer <peer-id>           # filter by peer access
rdrop blob remove file.txt
rdrop blob remove <blake3-hex-hash>
```

---

## `rdrop tag`

Associate an already-imported blob with a ring (or mark it open). Use this to change access after import.

```sh
rdrop tag <file>  --ring friends
rdrop tag <file>  --open
rdrop tag <hash>  --ring friends
rdrop tag <hash>  --open
```

---

## `rdrop untag`

Remove ring associations from an already-imported blob, revoking access for the affected rings.

| Flag | Description |
|---|---|
| `--ring <name>` | Remove a specific ring (repeatable) |
| `--open` | Revoke public access (removes the open-ring association) |
| `--all` | Remove every ring association; blob becomes inaccessible to all peers |

Exactly one of `--ring`, `--open`, or `--all` must be given; they are mutually exclusive.

```sh
rdrop untag <file>  --ring friends          # revoke friends-ring access
rdrop untag <file>  --ring friends --ring work  # remove two rings at once
rdrop untag <file>  --open                  # make no longer publicly accessible
rdrop untag <hash>  --all                   # revoke all access
```

**Notes:**
- Untagging with `--ring` or `--open` preserves all other ring associations.
- If the blob is not currently tagged with the specified ring, the command errors.
- `--all` always succeeds, even if the blob has no ring associations.

---

## `rdrop receive`

Download a blob from an `rdrop://` ticket. Automatically resumes if interrupted — no verified data is re-transferred.

```sh
rdrop receive rdrop://ABCDEF...
rdrop receive rdrop://ABCDEF... --dest ./downloads
rdrop receive rdrop://ABCDEF... --dest ./downloads/file.txt
```

`--dest` can be a directory (file is placed inside it) or an explicit file path.

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
