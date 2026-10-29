#!/usr/bin/env bash
set -euo pipefail

echo "scenario: crash-recovery"
echo "expected invariants:"
echo " - restart node applies term bump correctly"
echo " - stale term is rejected"
echo " - state transitions remain deterministic"

echo "INFO: no Jepsen harness is wired, running local Rust smoke checks"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
cargo test -p ganglion-openraft persisted_node_rejects_stale_term_after_restart --quiet
cargo test -p ganglion-openraft persisted_node_rejects_corrupt_file_log --quiet
cargo test -p ganglion-openraft persisted_node_rejects_non_sequential_file_log_indexes --quiet
echo "INFO: crash-recovery smoke checks completed"
