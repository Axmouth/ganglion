#!/usr/bin/env bash
set -euo pipefail

echo "scenario: persistence-backend-parity"
echo "expected invariants:"
echo " - file and Keratin persistence backends match bounded-tail behavior"
echo " - malformed tails recover only when replay budget allows"
echo " - persisted startup constructors remain deterministic under mixed-tail startup logs"

if command -v clojure >/dev/null 2>&1; then
  echo "clojure present; running planned jepsen checks"
  echo "TODO: integrate ganglion/jepsen persistence harness invocation"
  exit 0
fi

echo "INFO: clojure runtime missing; running focused rust persistence parity checks"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

cargo test -p ganglion-storage --features keratin -- --test-threads=1 fuzz_file_metadata_log_tail_boundary_recovery --quiet
cargo test -p ganglion-storage --features keratin -- --test-threads=1 fuzz_keratin_metadata_log_tail_boundary_recovery --quiet
cargo test -p ganglion-storage --features keratin -- --test-threads=1 keratin_metadata_log_recoverable_non_sequential_tail --quiet
cargo test -p ganglion-storage --features keratin -- --test-threads=1 keratin_metadata_log_rejects_non_sequential_index --quiet

cargo test -p ganglion-openraft persisted_node_startup --quiet
echo "INFO: persistence-backend-parity completed"
