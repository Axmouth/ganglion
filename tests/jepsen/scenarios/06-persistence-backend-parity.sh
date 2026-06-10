#!/usr/bin/env bash
set -euo pipefail

echo "scenario: persistence-backend-parity"
echo "expected invariants:"
echo " - file and Keratin persistence backends match bounded-tail behavior"
echo " - malformed tails recover only when replay budget allows"
echo " - persisted startup constructors remain deterministic under mixed-tail startup logs"
echo " - stale term proposals remain rejected after restart"
echo " - restart failover ordering enforces higher-term leadership sequencing"
echo " - log reset behavior is correct on term bump"

if command -v clojure >/dev/null 2>&1; then
  echo "clojure present; running planned jepsen checks"
  echo "TODO: integrate ganglion/jepsen persistence harness invocation"
fi

echo "INFO: running focused rust persistence parity checks (jepsen harness not yet wired)"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

cargo test -p ganglion-storage --features keratin -- --test-threads=1 fuzz_file_metadata_log_tail_boundary_recovery --quiet
cargo test -p ganglion-storage --features keratin -- --test-threads=1 fuzz_keratin_metadata_log_tail_boundary_recovery --quiet
cargo test -p ganglion-storage --features keratin -- --test-threads=1 keratin_metadata_log_recoverable_non_sequential_tail --quiet
cargo test -p ganglion-storage --features keratin -- --test-threads=1 keratin_metadata_log_rejects_non_sequential_index --quiet

cargo test -p ganglion-openraft persisted_node_startup_profile_selection_with_mixed_tail_and_explicit_override --quiet
cargo test -p ganglion-openraft persisted_node_recovered_startup_replays_control_loop_on_next_apply --quiet
cargo test -p ganglion-openraft persisted_node_rejects_stale_term_after_restart --quiet
cargo test -p ganglion-openraft persisted_node_failover_ordering_after_restart --quiet
cargo test -p ganglion-openraft persisted_node_resets_log_on_term_bump --quiet
cargo test -p ganglion-openraft persisted_node_startup_entrypoint_smoke_checks --quiet
echo "INFO: persistence-backend-parity completed"
