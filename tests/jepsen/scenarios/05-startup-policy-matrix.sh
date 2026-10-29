#!/usr/bin/env bash
set -euo pipefail

echo "scenario: startup-policy-matrix"
echo "expected invariants:"
echo " - startup policy constructor precedence is explicit > env > default"
echo " - strict/default/tail profiles show distinct outcomes under malformed tail conditions"
echo " - env strict path succeeds only on clean recovery logs"

echo "INFO: no Jepsen harness is wired, running focused Rust startup-policy matrix checks"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
cargo test -p ganglion-openraft persisted_node_startup_profile_matrix_for_strict_default_and_env_permutations --quiet
cargo test -p ganglion-openraft persisted_node_startup_profile_selection_with_mixed_tail_and_explicit_override --quiet
echo "INFO: startup-policy-matrix completed"
