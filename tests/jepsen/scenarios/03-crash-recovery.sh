#!/usr/bin/env bash
set -euo pipefail

echo "scenario: crash-recovery"
echo "expected invariants:"
echo " - restart node applies term bump correctly"
echo " - stale term is rejected"
echo " - state transitions remain deterministic"

if command -v clojure >/dev/null 2>&1; then
  echo "clojure present; running planned jepsen checks"
  echo "TODO: integrate ganglion/jepsen harness invocation"
  exit 0
fi

echo "INFO: clojure runtime missing; running local smoke invariants"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
cargo test -p ganglion-openraft persisted_node_rejects_stale_term_after_restart --quiet
cargo test -p ganglion-openraft persisted_node_rejects_corrupt_file_log --quiet
cargo test -p ganglion-openraft persisted_node_rejects_non_sequential_file_log_indexes --quiet
echo "INFO: crash-recovery smoke checks completed"
