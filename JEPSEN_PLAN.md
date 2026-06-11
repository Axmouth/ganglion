# Jepsen-Style Fault Injection Plan

This file captures the Jepsen validation plan for the `ganglion-openraft` control plane.
It is a roadmap artifact, not a time-based log.

## Purpose

- Validate leader election and state progress guarantees for metadata snapshots.
- Confirm failure behavior under partition, process crash, and stale-write attempts.
- Keep failures visible as end-to-end scenarios before real openraft transport is fully adopted.

## Assumptions

- Current implementation is `InMemoryMetadataNode` with deterministic in-memory behavior.
- True openraft transport/storage integration is coming later.
- Planner policy is pure and deterministic (`DeterministicPartitionPlacement`).

## Scenario Scaffolding

### 1) Baseline control cluster
- 3 simulated metadata nodes (`n1`, `n2`, `n3`) with fixed terms and leaders.
- Shared test driver publishes planner output via `plan_and_publish`.
- Assertions:
  - elected leader can propose and commit snapshots,
  - followers reject proposals,
  - generation is monotonic in persisted snapshot state.

### 2) Leader isolation and rejoin
- Cut leader connectivity to peers (or force fail-stop) while preserving its term.
- Drive follower nodes to continue proposing with older terms and stale proposals.
- Assertions:
  - stale proposers are rejected for term/generation reasons,
  - no follower state regresses,
  - recovered leader handles term transition correctly after reconnect.

### 3) Split-brain stress
- Force two nodes to believe they are leaders in different partitions.
- Inject snapshot proposals from both partitions with increasing/decreasing generations.
- Assertions:
  - only one partition’s leader is accepted by active peers,
  - conflicting writes do not both commit,
  - follower-visible snapshots preserve last-committed generation order.

### 4) Crash/restart and log reset
- Restart node with term bump and clear in-memory state (simulating hard restart/recovery).
- Assertions:
  - stale term behavior and term bumps reset local command sequence behavior,
  - post-restart proposer role checks are enforced.

### 5) Coordination observer safety checks
- Subscribe multiple watchers to snapshot updates while failures are injected.
- Assertions:
  - watchers only advance on successful consensus apply,
  - no phantom publication on rejected proposals,
  - all watchers converge to same committed snapshot.

### 6) Persistence backend parity checks
- Exercise storage-tail recovery invariants for file and Keratin metadata backends.
- Assertions:
  - malformed tails recover only within configured bounded-tail budgets,
  - non-sequential boundaries are rejected when strict and truncated only when allowed,
  - startup persistence checks remain deterministic after mixed/recoverable tails.

## Test artifact plan

- `tests/jepsen/openraft-plan-smoke.md`: scenario-by-scenario command list for a future harness.
- `tests/jepsen/` directory with:
  - scenario definitions,
  - reusable cluster orchestration scripts,
  - result collection checklist.
- Integration into CI behind a separate workflow/job that runs when openraft adapter is replaced.
- Runtime result collection:
  - each scenario writes `<scenario>.json` with status + expected-invariant metadata,
  - each `run.sh all` invocation writes `run-summary.json` aggregating all scenario artifacts.

### Fallback execution (current)

- Scenario scripts in `tests/jepsen/scenarios/` now run focused `ganglion-openraft` Rust checks when Jepsen/Clojure is unavailable.
- This keeps invariant coverage active in non-Clojure environments while keeping the future Jepsen hook in place.
- The fallback set now includes `06-persistence-backend-parity.sh` to validate storage parity and startup tails in one persistence scenario.
- That scenario now also exercises explicit restart/failover ordering, including stale-term rejection after a higher-term failover write.
- The fallback set now includes `07-raft-runtime-failover.sh` covering the real openraft runtime path:
  election/replication/stale rejection, leader-loss re-election, partitioned-follower rejoin,
  durable WAL restart recovery, and the file-store contract suite.
- The fallback set now includes `08-playground-smoke.sh`: scripted run of the cluster playground
  (`scripts/cluster-playground.sh`) through a full lifecycle — write, kill, write-under-loss,
  restart, add+promote, remove, final write — asserting each step's output and clean exit.

### Scenario artifact hardening (current)

- `tests/jepsen/run.sh all` now records `scenario_count` and `failed_scenarios` in `run-summary.json`.
- `run_scenario` summaries are always written individually and consumed by `validate-summary.json`.
- Aggregate rerun confidence now requires the scenario summary artifacts to be present before validation is marked successful.

## Exit checks

- No test should treat “new follower start” as a no-op publish path.
- Planner output after every accepted snapshot should be deterministically reproducible.
- Consensus-layer rejection paths must be exercised with generated inputs, not only fixed fixtures.

### Validation stability note

- Hang behavior observed during earlier broad validation runs is tied to an unrelated long-running background invocation path.
- Direct scenario execution and targeted module tests remain stable under this condition.
- When a one-shot aggregate command appears to stall, run a focused scenario first (`tests/jepsen/run.sh scenario <name>`) and confirm no orphaned validation processes are still active before retrying full coverage.
