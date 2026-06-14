# Ganglion Plan

## Goal

Deliver a neutral, reusable embedded-consensus and metadata-planning library for systems that need
a small shared control plane. Ganglion should help consumers decide who owns what, who is alive,
and which small control documents are committed, without taking over their domain model.

Fibril is the first serious consumer, but not the boundary of the design. Generic coordination code
should move into Ganglion over time. Queue-specific, broker-specific, or storage-engine-specific
logic should stay in Fibril and adapt to Ganglion's neutral model.

## Current Consumer Contract

`fibril` consumes coordination through a sync trait built on snapshot reads and a
`watch::Receiver<CoordinationSnapshot>` (`fibril/crates/broker/src/coordination.rs`); it
explicitly refuses to host consensus itself (`REPLICATION_PLANNING.md`). Ganglion therefore must
provide, in priority order:

1. A committed-snapshot watch stream published from consensus apply. This is the integration
   surface, not an add-on.
2. An embedded controller-capable node: leader-only planning + proposal, replicated commit,
   followers observing the same stream.
3. Fencing/epoch data alongside assignments (fibril's split-brain defense consumes epochs).
4. Resource catalogue and opaque replicated attributes for consumer-owned control documents.
5. Topology and health surfaces suitable for CLI/admin views.

Detailed historical designs live in `DESIGN.md` and `fibril/REPLICATION_PLANNING.md`. The worklog
tracks what actually landed. `SURFACE_INVENTORY.md` is the current reverse roadmap.

## Active plan (non-versioned)

### 1) Openraft-backed metadata plane

- DONE: storage adapters (`GanglionLogStore`, `GanglionStateMachine`) pass
  `openraft::testing::Suite`. In-process network router and `RaftMetadataNode` form real
  multi-node clusters with `MetadataConsensus`-equivalent semantics (NotLeader/StaleGeneration).
- DONE: committed-snapshot watch publication from the state machine apply path.
- DONE: durable WAL-backed raft storage and persistent state-machine snapshots.
- DONE: TCP transport for real multi-process clusters.
- NEXT: keep the runtime API small and document the bootstrap/runbook path clearly.
- NEXT: add remaining failure-mode scenarios for asymmetric partitions and leader-on-minority
  partitions.
- DECIDED (user, 2026-06-11): `RaftMetadataNode` stays async-only for writes; sync consumers read
  via `watch_committed()`/`committed_snapshot()`. `MetadataConsensus` remains the sync trait for
  the legacy in-memory/persisted nodes. No blocking adapter.

### 2) Generic coordination model

- DONE: generic `ResourceIdentity`, `PartitionAssignment`, durability policy, catalogue, and
  opaque attributes.
- DONE: deterministic and least-loaded placement strategies.
- DONE: owner-change epoch stamping and guarded proposals.
- NEXT: move downstream provider logic into Ganglion when it is truly domain-free. Likely
  candidates are heartbeat/liveness helpers, guarded attribute publication, catalogue sync, and
  generic controller-loop helpers.
- KEEP OUT: queue names, topic/group validation, message/event tails, client routing, and
  data-plane promotion semantics.

### 3) Storage and durability path

- Keep storage abstraction stable (`MetadataLog`) and continue compatibility with file-backed and
  Keratin-backed adapters.
- The openraft runtime currently uses durable file-backed raft storage. Keratin-backed metadata
  storage remains useful as a lower-level storage adapter, not the default cluster metadata path.
- Keep startup/recovery profile behavior explicit in constructor and diagnostics APIs.
- Strict raft WAL replay remains the default safety stance. A node with corrupt local coordination
  state should rejoin from peers instead of silently truncating committed raft history.

### 4) API and validation work

- Maintain one mutable API reference file (`API.md`) for current public contracts.
- Keep one-shot validation (`scripts/validate.sh`) as the operational check entrypoint.
- Maintain reproducible proptest/Jepsen fallback paths and artifact capture.
- Keep `FAILURE_MODES.md` synced with what is covered, what is merely reasoned about, and what
  still needs tests.

### 5) Documentation and packaging

- DONE: root README with project positioning and repository map.
- DONE: surface inventory for implemented behavior and known gaps.
- DONE: rough examples document for the current library shape.
- NEXT: operator quickstart for a local multi-node cluster.
- NEXT: library-consumer guide showing snapshot watches, guarded attributes, and resource
  assignment planning.
- NEXT: API cleanup pass before treating any crate surface as stable.

## Short-term roadmap

1. Finish the first documentation pass: README, examples, surface inventory, current plan, and
   runbook-oriented failure notes.
2. Create a generic-extraction inventory for code currently living in Fibril but suitable for
   Ganglion.
3. Add missing failure scenarios for asymmetric partitions and leader-on-minority partitions.
4. Add a consumer-facing example for guarded attributes and resource catalogue writes.

## Medium-term roadmap

1. Move domain-free provider logic from Fibril into Ganglion when the boundary is clear.
2. Replace raft-log heartbeats with leader-local soft liveness if cluster size makes heartbeat
   commits too noisy.
3. Add finer-grained assignment commands for large resource sets, instead of full-snapshot writes.
4. Improve package-level examples and API docs for each crate.
5. Keep expanding scenario coverage around partitions, restarts, membership mistakes, and disk
   failures.

## Long-term roadmap

- Stable crate API and versioned snapshot/migration story.
- Optional higher-level service wrapper for deployments that want a standalone coordination process.
- Operator-oriented strategy and telemetry configuration without binding to a single consumer model.
- Additional storage backends where they buy meaningful operational value.
