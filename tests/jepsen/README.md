# Jepsen Scenarios for Ganglion

This directory is a starter scaffold for the future Jepsen harness.
Keep it aligned with `JEPSEN_PLAN.md` and the final openraft transport implementation.

## Intended layout

- `README.md` (this file): scenario intent and mapping to expected checks.
- `scenarios/`: declarative scenario definitions (rebalancing, failover, partitions).
- `artifacts/`: captured run logs and CI summaries.

## Next concrete step

- Add a small orchestration runner (likely Clojure) that can:
  - boot a 3-node metadata cluster,
  - inject partition/fail/restart,
  - assert leader-election and snapshot monotonicity invariants,
  - emit artifacts to `artifacts/`.
