#!/usr/bin/env bash
set -euo pipefail

echo "scenario: raft-runtime-failover"
echo "expected invariants:"
echo " - 3-node openraft cluster elects a leader and replicates committed snapshots"
echo " - stale-generation writes are rejected after consensus, not before"
echo " - non-leader writes surface NotLeader"
echo " - leader loss: survivors re-elect and continue committing"
echo " - partitioned follower rejoins and converges on committed state"
echo " - durable node restart recovers committed state from the WAL"

if command -v clojure >/dev/null 2>&1; then
  echo "clojure present; running planned jepsen checks"
  echo "TODO: integrate ganglion/jepsen raft-runtime harness invocation"
fi

echo "INFO: running focused openraft runtime checks (jepsen harness not yet wired)"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

cargo test -p ganglion-openraft --features openraft three_node_cluster_elects_replicates_and_rejects_stale --quiet
cargo test -p ganglion-openraft --features openraft leader_loss_triggers_reelection_and_writes_continue --quiet
cargo test -p ganglion-openraft --features openraft partitioned_follower_rejoins_and_catches_up --quiet
cargo test -p ganglion-openraft --features openraft durable_node_recovers_committed_state_after_restart --quiet
cargo test -p ganglion-openraft --features openraft file_store_passes_openraft_contract_suite --quiet
cargo test -p ganglion-openraft --features openraft file_store_survives_reopen_with_vote_log_and_purge --quiet

echo "INFO: raft-runtime-failover completed"
