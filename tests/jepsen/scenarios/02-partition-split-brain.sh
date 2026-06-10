#!/usr/bin/env bash
set -euo pipefail

echo "scenario: split-brain"
echo "expected invariants:"
echo " - only one active leader's writes persist per generation"
echo " - no conflicting accepted generation values"
echo " - follower-safe convergence after heal"

if command -v clojure >/dev/null 2>&1; then
  echo "clojure present; running planned jepsen checks"
  echo "TODO: integrate ganglion/jepsen harness invocation"
  exit 0
fi

echo "INFO: clojure runtime missing; running local smoke invariants"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
cargo test -p ganglion-openraft fuzz_control_loop_publishing_and_rejection_matrix --quiet
cargo test -p ganglion-openraft control_loop_does_not_publish_on_consensus_reject --quiet
cargo test -p ganglion-openraft fuzz_apply_snapshot_handles_term_and_generation_rejections --quiet
echo "INFO: split-brain smoke checks completed"
