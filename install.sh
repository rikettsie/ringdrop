#!/usr/bin/env bash
set -euo pipefail

REPO="https://github.com/rikettsie/ringdrop"
BINARY="rdrop"

main() {
    if ! command -v cargo &>/dev/null; then
        echo "error: cargo not found. Install Rust via https://rustup.rs and re-run this script." >&2
        exit 1
    fi

    local tmp
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT

    echo "Cloning $REPO..."
    git clone --depth 1 "$REPO" "$tmp"

    echo "Compiling and installing $BINARY..."
    cargo install --path "$tmp" --locked

    echo ""
    echo "$BINARY installed successfully."

    if [[ ":$PATH:" != *":$HOME/.cargo/bin:"* ]]; then
        echo "Make sure ~/.cargo/bin is in your PATH. Add this to your shell profile:"
        echo '  export PATH="$HOME/.cargo/bin:$PATH"'
    fi
}

main "$@"
