#!/usr/bin/env bash
set -euo pipefail

echo "scenario: startup-entrypoint-smoke"
echo "expected invariants:"
echo " - every persisted startup constructor is exercised"
echo " - constructors expose deterministic startup-replay profile resolution"
echo " - env-backed and explicit override constructors remain consistent"

echo "INFO: no Jepsen harness is wired, running local Rust smoke checks"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
cargo test -p ganglion-openraft persisted_node_startup --quiet
echo "INFO: startup-entrypoint-smoke completed"
