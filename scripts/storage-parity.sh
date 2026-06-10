#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "storage-parity: cargo test -p ganglion-storage --features keratin -- --test-threads=1"
cargo test -p ganglion-storage --features keratin -- --test-threads=1

echo "storage-parity: cargo test -p ganglion-openraft persisted_node_startup --quiet"
cargo test -p ganglion-openraft persisted_node_startup --quiet
