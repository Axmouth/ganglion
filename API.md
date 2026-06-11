# Ganglion API

This file tracks what each part of the current scaffolding is meant to do.

## Core (`ganglion-core`)

- `ResourceIdentity`
  - Generic identity for any sharded resource (`namespace`, `name`, `partition`, optional `group`).
  - Used by planners and coordinators without queue-specific assumptions.

- `NodeInfo`
  - Control-plane metadata for a node.
  - Holds node id, data endpoint, admin endpoint, and labels.

- `PartitionAssignment`
  - Describes owner, followers, epoch, and durability policy for one resource.
  - Includes helper methods for role checks and replica-set size.

- `CoordinationSnapshot`
  - Snapshot of all known nodes and assignments with a monotonic `generation`.
  - Used as the atomic metadata state contract for consensus-backed storage.

- `PlacementInput`
  - Planner input with live node set, current desired resources, existing assignments, follower target, and generation.

- `PartitionPlacementPolicy`
  - Trait for pluggable placement strategy.
  - Pure function: `(PlacementInput) -> PlacementPlan`.

- `PlacementStrategy`
  - Pluggable strategy key used by consumers to select behavior.
  - Built-ins:
    - `deterministic`: owner/rebalancing strategy that preserves existing owner when possible.
    - `least-loaded`: spreads owner assignments across live nodes by current ownership load.
  - Provides helpers:
    - `as_str()` for human-facing identifiers,
    - `parse(&str)` for config-like selection,
    - `all()` for discovery,
    - `as_strategy()` for resolving a live strategy implementation.

- `DeterministicPartitionPlacement`
  - First policy implementation.
  - Conservative behavior:
    - reuse existing owner if alive,
    - preserve follower order where possible,
    - fill missing follower slots predictably.

- `LeastLoadedPartitionPlacement`
  - Second built-in policy implementation.
  - Preserves existing assignment when possible, otherwise selects least-owned live node
    to reduce ownership hotspots.

- `plan_local_assignment_transitions`
  - Produces local per-node role transitions from previous and next snapshots.
  - Intended for components that need to demote/pause/promote work safely.

- `ReplicationDurabilityPolicy/Requirement`
  - Generic durability contracts for metadata write acceptance.
  - Does not couple to transport.

## Openraft adapter crate (`ganglion-openraft`)

- `MetadataConsensus`
  - Minimal control-plane trait for consensus adapters.
  - Supports local role checks, current leader read, and snapshot apply/read.

- `InMemoryMetadataNode`
  - Local, in-memory placeholder implementation.
  - Enforces raft-like write invariants in-memory:
    - local leader must propose on writes,
    - stale term and stale generation updates are rejected,
    - term changes can invalidate prior in-memory history.
  - Stores a local node identity and current term for debugging/inspection.
  - Exposes small debugging helpers used by tests:
    - `current_term()`
    - `log_len()`
    - `last_index()`
    - `last_term()`.
  - Intended as bootstrap harness until real openraft adapter logic is wired in.

- `OpenraftAdapterError`
  - Small error surface shared by metadata adapter operations.

- `plan_and_apply`
  - Runs a planner and applies the resulting snapshot in one call.
  - Used in early control-loop bootstrapping.

- `plan_and_publish`
  - Runs a planner and consensus apply, then publishes the committed snapshot through a callback.
  - Useful for control-plane loops where observers consume assignment updates from the same node.

- `PersistedMetadataNode`
  - File-backed consensus adapter using durable logs from `ganglion-storage`.
  - Restores current term and latest committed snapshot from disk during construction.
  - Supports replayable term/retry behavior for restart flows.
- `PersistedMetadataNode::new` now uses bounded-tail replay by default.
- `PersistedMetadataNode::new_strict` preserves strict validation on startup.
- `PersistedMetadataNode::new_with_tail_replay_limit` sets explicit tolerated tail limit for startup.
- `PersistedMetadataNode::new_with_replay_profile` selects startup behavior from a replay profile:
  - `Strict`
  - `Default` (`TruncateTail` with one line tolerance)
  - `TruncateTail { max_tail_lines }`
- `PersistedMetadataNode::new_from_env` reads `GANGLION_PERSISTED_REPLAY_PROFILE` when available.
- `PersistedMetadataNode::new_with_replay_profile_str` accepts an optional raw profile string override and returns the resolved
  startup profile resolution (explicit override, environment, or default) alongside the node:
  - helper return type: `PersistedMetadataReplayProfileResolution`.
  - resolution source: `Explicit`, `Environment`, or `Default`.
- `PersistedMetadataReplayProfileSource`
  - Tracks where the profile decision came from (`Explicit` / `Environment` / `Default`).
- `PersistedMetadataReplayProfileResolution`
  - Structured output for startup-profile decisions with both `profile` and `source`.
- `PersistedMetadataNode::startup_replay_profile` and `startup_replay_policy` expose the resolved startup choice.
- `PersistedMetadataReplayProfile` can also be parsed from strings such as:
  - `strict`
  - `default` / `resilient`
  - `tail:<n>` / `truncate_tail:<n>` / `<n>`.
- `OpenraftAdapterError::Config` reports invalid profile parsing/lookup failures.

- `OpenraftAdapterError::Storage`
  - New variant for storage failures from durability backends.

### Openraft runtime (feature `openraft`)

- `GanglionRaftConfig`
  - `openraft::RaftTypeConfig` for the metadata raft group
    (`NodeId = u64`, `Node = BasicNode`, `SnapshotData = Cursor<Vec<u8>>`).
- `MetadataRaftCommand` / `MetadataRaftResponse` / `MetadataRejection`
  - App payloads: `ApplySnapshot(CoordinationSnapshot)` and
    `ApplySnapshotGuarded { expected_generation, snapshot }` (CAS — commits only if the committed
    generation still matches; checked inside the replicated apply, race-free by construction).
  - Response: `accepted: bool`, `rejection: Option<MetadataRejection>`
    (`StaleGeneration` | `GenerationMismatch { expected, actual }`), committed `snapshot`.
- `GanglionLogStore`
  - In-memory `RaftLogReader` + `RaftLogStorage`; passes `openraft::testing::Suite`.
- `FileRaftLogStore`
  - Durable WAL-backed `RaftLogStorage` (JSON lines: vote/entry/truncate/purge records); strict
    replay on open, fsynced appends, purge-time compaction. Also passes the contract suite.
  - `FileRaftLogStore::open(path)`.
- `GanglionStateMachine`
  - `RaftStateMachine` + `RaftSnapshotBuilder`; holds the committed `CoordinationSnapshot`,
    JSON snapshot build/install, and publishes committed state on a `tokio::sync::watch` channel
    (`watch_committed()`).
  - `GanglionStateMachine::persistent(path)`: snapshots are additionally written to disk
    atomically (tmp + fsync + rename + parent-dir fsync) and restored on open — required for
    state to survive log purges across full restarts, and what bounds recovery time.
- `InProcessRouter<LS>` / `InProcessConnection<LS>`
  - `RaftNetworkFactory` / `RaftNetwork` routing RPCs between same-process `Raft` handles;
    generic over the log store (default `GanglionLogStore`); `deregister` simulates unreachable peers.
- `GanglionRaftOf<LS>` / `GanglionRaft`
  - Raft handle aliases (generic / default in-memory).
- `RaftMetadataNode<LS>`
  - Runtime node: `start` (in-memory) / `start_with_store` / `start_with_storage` (explicit state
    machine) / `start_durable(id, config, router, dir)` (WAL + persisted snapshot under `dir`;
    bounded restart recovery),
    `initialize`, `write_snapshot` (maps `ForwardToLeader` → `NotLeader`, rejected stale commit →
    `StaleGeneration`), `committed_snapshot`, `watch_committed`, leader/applied-index wait helpers,
    `shutdown`. Durable restart: reopen the WAL and `start_with_store` again — do not re-`initialize`.
  - Membership: `add_learner(id, node, blocking)` (replication without vote; blocking waits for
    catch-up) and `change_membership(voters, retain)` (replaces the voter set; `retain` keeps
    demoted voters as learners). Both leader-only (`NotLeader` otherwise).
  - Guarded writes: `write_snapshot_guarded(expected_generation, snapshot)` (CAS;
    `GenerationMismatch` error on lost race) and `plan_and_propose_guarded(plan, max_retries)`
    (read committed → pure `plan` → generation bump + epoch stamping → guarded propose → retry on
    mismatch). The race-safe controller-loop primitive.

### Epoch/fencing helpers (`ganglion-core`)

- `next_assignment_epoch(committed, desired_owner) -> (u64, EpochTransition)`
  - Owner change bumps; follower churn holds; new assignments start at 1.
- `fence_assignment_epoch(committed) -> u64` — operator fence without ownership change.
- `stamp_assignment_epochs(committed, &mut desired) -> Vec<(ResourceIdentity, EpochTransition)>`
  - Applies the rule across a planned snapshot; caller owns tombstone retention if resources can
    be removed and re-added.
- `default_raft_config()`
  - Validated `openraft::Config` tuned for the metadata workload: snapshots every
    `SNAPSHOT_LOGS_SINCE_LAST` (256) entries, `MAX_IN_SNAPSHOT_LOG_TO_KEEP` (64) retained after
    purge — this bounds WAL size and startup replay.
- `StorageTelemetry` / `StorageTelemetrySnapshot`
  - Plain atomic durability counters (appends, batches, fsyncs, compactions, replay size on open,
    snapshot persists/loads). Exposed via `FileRaftLogStore::telemetry()`,
    `GanglionStateMachine::telemetry()`, and aggregated on `RaftMetadataNode::telemetry()`.
    No metrics-crate dependency; consumers map into their own systems.
- `RaftTopology`
  - Serializable per-node view of the raft group (`local_id`, `leader`, `voters`, `learners`,
    raft-id→address map, applied/snapshot indexes, committed generation). Produced sync via
    `RaftMetadataNode::topology()`. This is the JSON contract for topology CLIs/admin diagrams.

## Storage crate (`ganglion-storage`)

- `MetadataLog`
  - Persistence abstraction for append entries and replay semantics.
- `InMemoryMetadataLog`
  - In-memory implementation used by tests.
- `FileMetadataLog`
  - Append-only file-backed log that writes newline-delimited JSON entries.
  - Replay validates index continuity by default and rejects malformed/invalid entries.
  - Supports configurable replay policy through `FileMetadataReplayPolicy`:
    - `Strict` for hard-fail validation
    - `TruncateTail { max_tail_lines }` to recover from bounded trailing corruption.
- `KeratinMetadataLog` (feature `keratin`)
  - Durable metadata backend powered by `keratin-log`.
  - Supports the same append/read/clear/truncate contract as the file log with replay-policy behavior.
  - Intended for append-only segment style durability where restart/replay is handled by the underlying keratin segment system.
- `MetadataLogEntry`
  - Term/index/snapshot records stored by durable implementations.
- `FileMetadataReplayPolicy`
  - Storage replay policy for file logs, selected at constructor time.
- `MetadataLog` constructor pathways for persisted nodes
  - `PersistedMetadataNode::new_with_log(...)` and `PersistedMetadataNode::new_with_log_and_profile(...)`
  - Support injecting any `MetadataLog` implementation for startup/constructor-path tests and backend parity runs.
- `PersistedMetadataNode::new_with_replay_policy`
  - File-backed node constructor that allows bounded-tail recoverability choices at startup.

## Planned next part

- Durable raft storage: `MetadataLog`-backed log/vote persistence for the openraft runtime path.
- Membership change/learner flows on `RaftMetadataNode`; wire transport beyond in-process.

## Coordination crate (`ganglion-coordination`)

- `CoordinationProvider`
  - Read/watch abstraction for snapshot consumers.
  - Exposes:
    - `snapshot()`
    - `owns_resource(...)`
    - `follows_resource(...)`
    - `watch()`
- `InMemoryCoordination`
  - Mutable testable provider with broadcast updates.
  - Keeps an in-memory snapshot and emits updates through `tokio::sync::watch`.
- `StaticCoordination`
  - Immutable fixture provider for tests and bootstrap deployments.
- Helpers
  - `owned_resources(...)`
  - `followed_resources(...)`
  - `owned_by_snapshot(...)`
