#!/usr/bin/env bash
set -euo pipefail

echo "scenario: baseline-failover"
echo "expected invariants:"
echo " - leader handoff observed"
echo " - monotonic generation"
echo " - no duplicate publishes on rejected proposals"

echo "INFO: no Jepsen harness is wired, running local Rust smoke checks"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
cargo test -p ganglion-openraft persisted_metadata_node_roundtrips_state_and_replays_logs --quiet
cargo test -p ganglion-openraft persisted_node_control_loop_publishes_to_watchers --quiet
cargo test -p ganglion-openraft control_loop_publishes_planned_snapshot_to_watchers --quiet
echo "INFO: baseline-failover smoke checks completed"
