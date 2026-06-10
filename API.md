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

- `DeterministicPartitionPlacement`
  - First policy implementation.
  - Conservative behavior:
    - reuse existing owner if alive,
    - preserve follower order where possible,
    - fill missing follower slots predictably.

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
- `MetadataLogEntry`
  - Term/index/snapshot records stored by durable implementations.
- `FileMetadataReplayPolicy`
  - Storage replay policy for file logs, selected at constructor time.
- `PersistedMetadataNode::new_with_replay_policy`
  - File-backed node constructor that allows bounded-tail recoverability choices at startup.

## Planned next part

- `ganglion-openraft` full Raft engine integration to replace current in-memory placeholder.
- Optional transport/watch layer for snapshot notifications and controller handoff loops.

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
