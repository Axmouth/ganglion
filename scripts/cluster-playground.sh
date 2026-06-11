#!/usr/bin/env bash
# Interactive (or scripted) ganglion raft cluster playground.
#
# Examples:
#   scripts/cluster-playground.sh                       # 3 nodes, interactive stdin
#   scripts/cluster-playground.sh --nodes 5
#   scripts/cluster-playground.sh --script "status; write 1; kill 2; status; quit"
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
exec cargo run -p ganglion-openraft --features openraft --example cluster_demo --quiet -- "$@"
