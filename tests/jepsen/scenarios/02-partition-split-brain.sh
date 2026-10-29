#!/usr/bin/env bash
set -euo pipefail

echo "scenario: split-brain"
echo "expected invariants:"
echo " - only one active leader's writes persist per generation"
echo " - no conflicting accepted generation values"
echo " - follower-safe convergence after heal"

echo "INFO: no Jepsen harness is wired, running local Rust smoke checks"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
cargo test -p ganglion-openraft fuzz_control_loop_publishing_and_rejection_matrix --quiet
cargo test -p ganglion-openraft control_loop_does_not_publish_on_consensus_reject --quiet
cargo test -p ganglion-openraft fuzz_apply_snapshot_handles_term_and_generation_rejections --quiet
echo "INFO: split-brain smoke checks completed"
