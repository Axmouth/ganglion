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

## Test artifact plan

- `tests/jepsen/openraft-plan-smoke.md`: scenario-by-scenario command list for a future harness.
- `tests/jepsen/` directory with:
  - scenario definitions,
  - reusable cluster orchestration scripts,
  - result collection checklist.
- Integration into CI behind a separate workflow/job that runs when openraft adapter is replaced.

## Exit checks

- No test should treat “new follower start” as a no-op publish path.
- Planner output after every accepted snapshot should be deterministically reproducible.
- Consensus-layer rejection paths must be exercised with generated inputs, not only fixed fixtures.
