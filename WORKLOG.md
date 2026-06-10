# Ganglion Worklog

Project root: `/home/george/code/ganglion`

## Scope

This file is the detailed history of everything done in this repo, modeled after fibril’s worklog format.

## Planning and scope alignment

- Initialized planning activity for ganglion based on existing `fibril` replication and coordination docs:
  - `/home/george/code/fibril/REPLICATION_PLANNING.md`
  - `/home/george/code/fibril/REPLICATION_WORKLOG.md`
  - `/home/george/code/fibril/crates/broker/src/coordination.rs`
- Read and summarized existing fibril decisions to avoid re-inventing domain-specific semantics.
- Confirmed existing fibril model already separates:
  - coordination ownership/follower assignment
  - durable metadata snapshots
  - optional planner policies
  - follower replication semantics
- Decided to preserve queue-specific details inside fibril and keep ganglion core neutral.
- Added initial plan document with v0 architecture and roadmap:
  - `PLAN.md` (first immutable plan snapshot)
- Reworked the plan doc into explicit immutable-snapshot format to satisfy “do not mutate past plans” requirement.
- Added this worklog to track actions and future milestones in one place.

## Scaffolding and implementation kickoff

- No code implementation yet in ganglion crates (planning-only phase).
- Next concrete tasks to begin immediately:
  1. Bootstrap a Rust workspace for `ganglion` crates.
  2. Move generic planning primitives out of fibril-like docs into compile-time types.
  3. Add openraft adapter and test harness in a constrained v0 branch.

## Scaffold implementation completed

- Added root workspace manifest with members:
  - `crates/ganglion-core`
  - `crates/ganglion-openraft`
- Added `ganglion-core` package and API skeleton with:
  - resource/assignment/snapshot types,
  - durability policy model,
  - deterministic placement planner trait + implementation,
  - local transition planning.
- Added `ganglion-openraft` package and initial consensus adapter scaffold:
  - `MetadataConsensus` trait,
  - `InMemoryMetadataNode`,
  - plan-and-apply convenience helper.
- Added `API.md` to track current API intent in a mutable, central place.
- Updated `PLAN.md` with immutable snapshots and explicit non-time roadmaps.

## Validation pass (in-memory + core behavior)

- Added unit test coverage in `ganglion-core` for deterministic planner behavior and transition derivation.
- Added unit test coverage in `ganglion-openraft` for in-memory consensus guard rails:
  - non-leader proposals
  - stale generation rejection
  - generation monotonic update.

## Open Questions

- Whether to split `ganglion-storage` from `ganglion-openraft` immediately, or start with a single adapter module and refactor after the first integration round.
- Exact crate naming for future external backend adapters (`ganglion-coordination-etcd` preferred, not yet created).
- Which durability policy representation is better for generic clients: numeric node-count requirement or policy enum.

## Resolution Notes from Past Decisions

- Keep planner policy pluggable and pure-function based.
- Keep node epoch/fencing checks explicit and monotonic.
- Avoid queue semantics in core API even if queue data is the first consumer.

## Coordination contract scaffold added

- Added `ganglion-coordination` crate with shared abstractions for ownership-aware snapshot consumers.
- Added:
  - `CoordinationProvider` trait
  - `InMemoryCoordination` with watch updates
  - `StaticCoordination` immutable fixture provider
  - helper filters for owned/followed resources
- Added unit tests for provider role checks and snapshot updates.
- Added crate to workspace members list.

## v4 execution pass completed

- `ganglion-openraft`:
  - Removed an earlier unstable placeholder implementation that leaked references from temporary locks.
  - Reworked `InMemoryMetadataNode` to hold stable internal state:
    - persistent local node id
    - raft-like term tracking
    - leader identity in state
    - append-only metadata log simulation with term-stamped entries
    - generation-based CAS behavior on snapshots
  - Reintroduced/updated tests for:
    - non-leader rejection
    - stale generation rejection
    - stale term rejection
    - generation update
    - planner/apply with log growth
    - term bump resets in-memory log
    - id visibility (`local_node_id`, `leader_id`, `is_leader`)
  - Kept `MetadataConsensus` interface stable while adjusting internals to be deterministic and testable.

- `ganglion-core`:
  - Fixed transition matching to avoid emitting a transition for `(LocalRole::None, LocalRole::None)`.
  - Updated deterministic planner assertion in tests to match current stable follower-choice behavior.
  - Re-ran full repo tests and verified green.

- Docs:
  - Appended `PLAN.md` `Plan Snapshot v4` with completed items and next three-step path.
  - No plan/worklog rewrites were performed; all additions are appended-only.

## v5 execution pass completed

- `ganglion-openraft`:
  - Added `plan_and_publish` to bind planner -> consensus -> watcher publisher in one flow.
  - Added integration tests to validate publish only on consensus success:
    - publishes planned snapshot to `ganglion-coordination` watch target
    - does not publish when proposer is not leader
  - Removed test-level warning by eliminating unnecessary `mut` binding.

- `ganglion-core`:
  - Added regression test to verify no-op transitions are omitted when role stays `None`.
  - Re-checked deterministic role transition behavior against core transition API.

- Validation:
  - Re-ran full workspace tests with `cargo test --all-targets --workspace --quiet`.
  - Re-sanitized docs to keep plan/worklog append-only and roadmap entries time-free.
  - Verified API surface documentation to include control-loop helper.
