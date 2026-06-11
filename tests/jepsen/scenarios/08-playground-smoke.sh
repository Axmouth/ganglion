#!/usr/bin/env bash
set -euo pipefail

echo "scenario: playground-smoke"
echo "expected invariants:"
echo " - scripted playground runs a full lifecycle: write, kill, restart, add, remove"
echo " - the final write commits after all membership churn"
echo " - the playground exits cleanly"

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."

OUTPUT="$(timeout 180 bash scripts/cluster-playground.sh --script \
  "status; write 1; kill 3; write 2; restart 3; add 4; remove 4; write 3; status; quit" 2>&1)"

echo "$OUTPUT" | tail -20

fail() { echo "FAIL: $1" >&2; exit 1; }

echo "$OUTPUT" | grep -q "write committed via node .*generation=1" \
  || fail "first write did not commit"
echo "$OUTPUT" | grep -q "node 3 killed" || fail "kill did not run"
echo "$OUTPUT" | grep -q "write committed via node .*generation=2" \
  || fail "write during node-3 downtime did not commit"
echo "$OUTPUT" | grep -q "node 3 restarted" || fail "restart did not run"
echo "$OUTPUT" | grep -q "node 4 added and promoted to voter" || fail "add did not run"
echo "$OUTPUT" | grep -q "node 4 removed from the cluster" || fail "remove did not run"
echo "$OUTPUT" | grep -q "write committed via node .*generation=3" \
  || fail "final write did not commit"
echo "$OUTPUT" | grep -q "playground stopped" || fail "playground did not exit cleanly"

echo "INFO: playground-smoke completed"
