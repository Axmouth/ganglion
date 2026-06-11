# Ganglion Plan

## Goal

Deliver a neutral, reusable embedded-consensus + metadata-planning library that supports
leader election workflows and similar coordination needs, while keeping `fibril` as a consumer
rather than a design driver.

## Consumer contract (what fibril actually needs)

`fibril` consumes coordination through a sync trait built on snapshot reads and a
`watch::Receiver<CoordinationSnapshot>` (`fibril/crates/broker/src/coordination.rs`); it
explicitly refuses to host consensus itself (`REPLICATION_PLANNING.md`). Ganglion therefore must
provide, in priority order:

1. A committed-snapshot watch stream published from consensus apply — this is the integration
   surface, not an add-on.
2. An embedded controller-capable node: leader-only planning + proposal, replicated commit,
   followers observing the same stream.
3. Fencing/epoch data alongside assignments (fibril's split-brain defense consumes epochs).
   Schema addition needs a design pass before committing to it.

## Active plan (non-versioned)

### 1) Openraft-backed metadata plane

- DONE: storage adapters (`GanglionLogStore`, `GanglionStateMachine`) pass
  `openraft::testing::Suite`; in-process network router and `RaftMetadataNode` form real
  multi-node clusters with `MetadataConsensus`-equivalent semantics (NotLeader/StaleGeneration).
- NEXT: committed-snapshot watch publication from the state machine apply path
  (tokio `watch` channel; bridges async raft to fibril's sync consumption).
- THEN: durable raft storage — back the raft log/vote/state with `MetadataLog`
  (file + Keratin) so the raft path reaches durability parity with the legacy path.
- The legacy `OpenraftLikeStore`/`MetadataNode` path stays as the simple sync backend until the
  raft path reaches durability parity; after that it remains as a test double / single-node mode
  unless complexity says otherwise.
- OPEN DECISION (user input wanted): whether `MetadataConsensus` grows an async variant, or
  `RaftMetadataNode` keeps its own async API with watch-stream reads being the only sync surface.

### 2) Pluggable planner strategies

- Keep `ganglion-core` planner APIs pure and deterministic.
- Strategy registry exists (`deterministic`, `least-loaded`); keep selection explicit and
  discoverable for callers.

### 3) Storage and durability path

- Keep storage abstraction stable (`MetadataLog`) and continue compatibility with file-backed and
  Keratin-backed adapters.
- Reuse the same abstraction under the openraft path (see Active plan 1).
- Keep startup/recovery profile behavior explicit in constructor and diagnostics APIs.

### 4) API and validation work

- Maintain one mutable API reference file (`API.md`) for current public contracts.
- Keep one-shot validation (`scripts/validate.sh`) as the operational check entrypoint.
- Maintain reproducible proptest/Jepsen fallback paths and artifact capture.
- Add raft-runtime scenarios (election, failover, partition via router deregister) to the Jepsen
  fallback inventory once watch publication lands.

## Short-term roadmap

1. Committed-snapshot watch publication from `GanglionStateMachine` apply/install paths,
   surfaced on `RaftMetadataNode` (and mirrored on the legacy nodes for contract parity).
2. Durable raft storage: `MetadataLog`-backed log store + vote persistence + restart recovery
   tests through the raft path.
3. Raft-runtime failure scenarios: leader loss + re-election, partition (router deregister),
   restart-with-durable-log; wire into Jepsen fallback scripts.

## Medium-term roadmap

1. Membership change/learner flows on `RaftMetadataNode` (`add_learner`, `change_membership`).
2. Epoch/fencing surface for assignments (schema design first; user decision).
3. Durability telemetry around append/clear/truncate and startup recovery outcomes.

## Long-term roadmap

- Wire transport (gRPC or similar) implementing `RaftNetwork` beyond in-process.
- Operator-oriented strategy/telemetry configuration without binding to a single domain model.
- Upgrade-friendly schema and migration hooks for persisted metadata snapshots and replay records.
