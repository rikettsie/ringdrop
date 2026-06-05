# Changelog

All notable changes to this project will be documented in this file.

## [0.13.1] - 2026-06-05

### Miscellaneous Tasks

- Dispatch to ringdrop-packaging directly from publish job

## [0.13.0] - 2026-06-05

### Features

- (**CLI**) Add info command to extract ticket details
- Add ascii art on ringdrop text
- Add ringdrop mascot PNG

### Miscellaneous Tasks

- Notify ringdrop-packaging on release publish

## [0.12.0] - 2026-06-02

### Documentation

- Document custom relay configuration in README
- Use third-person singular in all doc comment summary sentences

### Features

- (**config**) Add relay_url field for custom relay configuration
- (**node**) Wire custom relay into endpoint builder
- (**daemon**) Report relay in daemon start and status output
- (**ipc**) Add a parsable structured Record on the Daemon wire protocol

## [0.11.1] - 2026-05-29

### Documentation

- Fix cargo binstall target (repo name vs binary name)

## [0.11.0] - 2026-05-29

### Documentation

- Add cargo binstall as installation method

## [0.10.0] - 2026-05-27

### Bug Fixes

- (**peer-remove**) Revoke all grants when removing a peer

### Documentation

- Add docs/cli.md and docs/install.md, slim README to command table
- Add tag and untag to README command table

### Features

- (**peers**) Add PeerStore — local peer address book (peers.redb)
- (**protocol**) Add peer ops, drop nickname from RingAdd
- (**handlers**) Add peer handlers, resolve ring nicknames from PeerStore
- (**server**) Dispatch peer ops and updated ring handlers
- (**cli**) Add rdrop peer subcommand, drop --nickname from ring add
- (**local-store**) Add LocalStore, migrate old redb files on startup
- (**untag**) Add rdrop untag command to revoke ring access
- (**migration**) Backfill peers from ring memberships into local.redb

### Miscellaneous Tasks

- Add docs.rs and codecov badge

### Refactoring

- Remove inconsistent protocol re-exports
- Extract format_peer_entry, drop duplicated peer display logic
- (**stores**) Add from_db constructor to GrantStore and PeerStore
- (**node**) Use LocalStore to open the shared local.redb
- (**peer**) Drop peer nick, peer add handles all nickname management
- (**tags**) Drop tags, blob list already shows ring associations

## [0.9.0] - 2026-05-25

### Bug Fixes

- (**CLI**) Expose blob list --peer and --ring options
- (**node**) Show the full reason of a connection error

### Documentation

- Add new commands in README.md

### Features

- (**CLI**) Add blob list filtering by peer and by ring
- (**grants**) Add GrantStore backed by redb
- (**catalog**) Restructure protocol module and add CatalogHandler
- (**node**) Integrate GrantStore and CatalogHandler
- (**core**) Add grant and remote commands + daemon protocol ops

### Refactoring

- (**protocol**) Support iroh-rings v2 permission semantics
- Improve code readability

## [0.8.0] - 2026-05-20

### Documentation

- (**architecture**) Make a more precise schema

## [0.7.1] - 2026-05-18

### Documentation

- (**lib**) Add a better global schema in lib.rs

## [0.7.0] - 2026-05-18

### Documentation

- Update documentation
- Update README with new protocol version

### Refactoring

- Support new /iroh-rings/1 protocol version
- (**daemon**) Remove pid file lifecycle support - IPC rules over
- Support new /iroh-rings/1 protocol version

## [0.6.0] - 2026-05-15

### Bug Fixes

- (**daemon**) Harden server edge cases
- (**daemon**) Bound IPC line length to 512 KiB
- (**daemon**) Move --data-dir before subcommand in spawned daemon args

### Documentation

- Update install section with platform-specific instructions
- Update README with daemon lifecycle

### Features

- (**install**) Add install scripts and INSTALL.md for Linux, macOS, and Windows
- (**config**) Add daemon_port field with default 60001
- (**core**) Add Node::import_path helper, clarify make_ticket comment
- (**daemon**) Add IPC protocol types and module skeleton
- (**daemon**) Add PID file helpers
- (**daemon**) Add TCP client
- (**daemon**) Add TCP server with concurrent connection handling
- (**cli**) Add daemon subcommand
- (**daemon**) Add background daemon with TCP-based IPC

### Refactoring

- (**cli**) Replace standalone-node commands with daemon-client proxies
- (**daemon**) Enforce req_id as mandatory field on both Request and Event
- (**daemon**) Split server.rs into server/ with per-domain handler modules
- (**cli**) Clarify daemon start command

## [0.5.1] - 2026-05-12

### Documentation

- Update README.md referencing /iroh-rings/0 ALPN

## [0.5.0] - 2026-05-12

### Refactoring

- (**ticket**) Move ticket module inside core/
- (**protocol**) Split protocol into sender and receiver
- (**Registry**) Make Registry a trait to suport generic backend implementations
- Extract access protocol into the generalized iroh-rings crate

## [0.4.3] - 2026-05-10

### Bug Fixes

- Keep only the relay URL in the share ticket

### Miscellaneous Tasks

- (**release**) Extract release notes from CHANGELOG.md

## [0.4.2] - 2026-05-10

### Features

- (**CLI**) Change --tag with --ring option in blob import

### Miscellaneous Tasks

- Add git-cliff support for CHANGELOG.md generation

## [0.4.1] - 2026-05-10

### Miscellaneous Tasks

- Add more target builds in CI (Linux, macOS, Windows)
- Add rdrop formula hooks for brew install
- Add rdrop manifest hooks for scoop bucket install
- Bump version 0.4.1

## [0.4.0] - 2026-05-09

### Bug Fixes

- Properly tag and stream every directory member via iroh-blobs Collection
- Adapt progress bar to the case of multiple directory files

### Features

- (**CLI**) Add --force-overwrite option to blob receive

## [0.3.0] - 2026-05-08

### Bug Fixes

- (**ci**) Don't publish unless lint and test jobs both pass
- Use actual file byte count, with no protocol overhead
- Ensure FStore is in sync
- (**CLI**) Add file name alongside its hash in the blob list

### Documentation

- Add CI tests badge on README
- Add shields.io crates.io version
- Add MIT license badge in README

### Features

- (**CLI**) Add progress bar during transfer

### Miscellaneous Tasks

- Rename release workflow to 'Publish'

### Refactoring

- (**PeerId**) Move peer-id out of the Registry and enhance boot for heavy commands

## [0.2.1] - 2026-05-05

### Documentation

- Update README.md with better code block examples

### Miscellaneous Tasks

- Add OIDC Github connection for crate release

## [0.2.0] - 2026-05-05

### Bug Fixes

- Unregister the standard blobs ALPN as unintentional blob transfer could leak

### Documentation

- Update README with new command split usage
- Update README usage with log overrides

### Features

- Split Share command into Import and Serve with options
- (**CLI**) Add Clob subcommand support to manage imported files

## [0.1.0] - 2026-05-05

### Bug Fixes

- Enforce ring existence on file tag
- Make download resolve on the first item from the stream
- (**CLI**) Enforce tag command options with a clear error message
- Set absolute path on iroh-blobs export
- Add check the self peer id cannot be added to a ring

### Features

- P2P file transfer with ring-gated access over iroh/QUIC
- Add persisted config file
- Add --oneshot option to share subcommand to shutdown on transfer complete
- (**registry**) Add optional nickname when adding a peer to a ring

### Miscellaneous Tasks

- Add test lint and publish workflow


