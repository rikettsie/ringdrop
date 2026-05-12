# Changelog

All notable changes to this project will be documented in this file.

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


