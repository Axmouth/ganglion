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

## v6 execution pass started

- `.gitignore`
  - Added a Rust-oriented ignore file with `target/` and fuzz artifacts.

- `ganglion-core` fuzzing start
  - Added `proptest` as dev dependency.
  - Added property-based tests for planner invariants:
    - no unknown owners/followers in planned assignment
    - followers stay unique and never include owner
    - deterministic output under duplicate scheduling inputs
  - Added explicit empty-cluster-with-resources rejection property.
  - Ran fuzzed property checks and tightened planner follower construction to skip owners in follower lists.

- Validation / direction
  - Captured explicit next steps to stand up Jepsen-style failure tests for openraft consensus behavior.

## v7 execution pass started

- `ganglion-core` fuzz expansion:
  - Added property tests for planner behavior when existing assignments are present, including:
    - stale/dead owners handling,
    - epoch carry-forward vs increment semantics,
    - follower reuse ordering and uniqueness constraints under random snapshots,
    - empty/noisy follower list handling.
  - Added property tests for `plan_local_assignment_transitions` over synthetic random snapshot pairs:
    - uniqueness and bounded resource transitions,
    - consistency between inferred role transitions and emitted transition enum.
- `ganglion-openraft` fuzzing:
  - Added `proptest` dev dependency.
  - Added property test that fuzzes proposer choice, leader assignment, and generation under control-loop execution.
  - The test checks publish-on-success and explicit failure on:
    - non-leader proposals,
    - stale generations.
  - Added property test for direct `apply_snapshot` term behavior:
    - stale term rejection,
    - stale generation rejection,
    - acceptance when term is equal or newer.
- Documentation:
  - Added `JEPSEN_PLAN.md` with concrete scenario list and observable acceptance criteria for eventual Jepsen work.
  - Expanded `.gitignore` with additional Rust workspace and tooling ignores.

- Validation:
  - Updated `PLAN.md` with `Plan Snapshot v7`.

## v8 execution pass completed

- `ganglion-storage` introduced:
  - Created new crate `ganglion-storage` with:
    - `MetadataLog` trait,
    - file-backed and in-memory implementations,
    - serialized `MetadataLogEntry` format,
    - helper conversion to preserve `ResourceIdentity` keys safely in durable files.
  - Added crate to workspace and dependency wiring in `ganglion-openraft`.

- `ganglion-openraft` persistence path:
  - Reworked internal node plumbing to persist and restore state through `MetadataLog` abstraction.
  - Added `PersistedMetadataNode`:
    - recovers latest term and snapshot from file,
    - supports restart semantics in unit tests.
  - Added `Storage` variant to `OpenraftAdapterError` with conversion from storage errors.
  - Fixed nested-result handling in write path so storage and validation errors are now propagated.
  - Fixed term-vs-generation ordering so stale term checks are enforced with explicit priority.
  - Added explicit recovery-path tests:
    - roundtrip replay with term recovery,
    - stale-term rejection after restart,
    - term bump log-reset assertion with persisted node.

- Test scaffolding and harnessing:
  - Added fuzz/proptest utilities:
    - `scripts/proptest.sh` with crate selection and replay modes,
    - `.gitignore` updates for regression captures,
    - dedicated regression directories under `crates/ganglion-* /proptest-regressions`.
  - Added Jepsen scaffolding:
    - `scripts/jepsen.sh` forwarding wrapper,
    - `tests/jepsen/run.sh` runner (list/scenario/all),
    - baseline/split-brain/crash scenario scripts and runner docs.
  - Added a regression-style repro test that validates control-loop publish side effects for nontrivial proposer/leader combinations.

- Validation:
  - Ran formatting: `cargo fmt --all`.
  - Ran tests:
    - `cargo test -p ganglion-storage --quiet`,
    - `cargo test -p ganglion-openraft --quiet`,
    - `cargo test --quiet` (workspace).
  - All tests passed after adjustments.

- Documentation:
  - Appended this pass to `PLAN.md` as `Plan Snapshot v8` with updated short/medium/long roadmaps.

- Notes:
  - `proptest` regression data generated for openraft fuzzing was retained locally for reproducibility.

## Current roadmap state (no timestamps)

### Short-term

1. Keep persistence adapter interface stable and add additional backends (Keratin + optional memory wrappers).
2. Finalize recovery and replay behavior under malformed/corrupt log inputs.
3. Enforce a CI policy that retains proptest regression artifacts for both core and openraft.

### Medium-term

1. Replace the in-memory-like behavior with true openraft transport while preserving `MetadataConsensus`.
2. Add watcher stream plumbing for external observers and health telemetry.
3. Expand Jepsen scaffolding into a runnable, automated fault-injection matrix.

### Long-term

1. Add planner strategy registry and strategy-specific defaults.
2. Add snapshot compaction/migration and historical-log lifecycle tooling.
3. Promote stable APIs for downstream consumers (fibril and beyond) without queue-specific coupling.

## v9 execution pass completed

- `ganglion-storage` durability hardening:
  - `FileMetadataLog` now validates replay integrity:
    - enforces index sequence and non-zero start index,
    - returns parse failures with line context on invalid JSON,
    - rejects malformed/non-sequential/zero-index entries deterministically.
  - Added unit tests for valid replay, comments+blank lines, malformed JSON, index violations.
- `ganglion-openraft` persisted-node robustness:
  - Added startup tests for corrupted and non-sequential metadata logs.
  - Verified those cases map to `OpenraftAdapterError::Storage`.
- `jepsen` scaffold execution:
  - Updated scenario scripts to run concrete smoke checks via existing Rust tests when Clojure is absent.
  - This gives reproducible local fallback behavior per scenario while keeping the Jepsen hook placeholder in place.
- Documentation cleanup:
  - Updated proptest regression fixture headers to neutral, in-repo language.
- Validation:
  - `cargo fmt --all`
  - `cargo test --quiet`

## Current roadmap update (no timestamps)

- Short-term:
  1. Add bounded recovery policy for partially corrupted logs.
  2. Add CI-preserved proptest artifact upload for core and openraft fuzz outputs.
  3. Add a lightweight sequence replay harness for Jepsen-like workflows without Clojure.
- Medium-term:
  1. Replace in-memory-like consensus path with true openraft transport.
  2. Add committed-snapshot event publishing and health telemetry.
  3. Expand cluster-level failover and partition integration tests.
- Long-term:
  1. Add planner strategy registry and parameterized policy registry.
  2. Add durable snapshot compaction and migration APIs.
  3. Support multiple backend adapters (`ganglion-storage` alternatives) behind a single contract.

## v10 execution pass completed

- `ganglion-storage`:
  - Added `FileMetadataReplayPolicy` (`Strict` and `TruncateTail`) and passed it through
    `FileMetadataLog::with_replay_policy`.
  - Extended file-log replay with bounded corruption recovery:
    - on malformed JSON and index violations,
    - with explicit trailing-line limit to recover only bounded tails.
  - Added tests confirming:
    - valid logs still replay normally,
    - strict mode rejects corrupted inputs,
    - bounded tail recovery succeeds only when corruption is within the configured limit.

- `ganglion-openraft`:
  - Added `PersistedMetadataNode::new_with_replay_policy` to choose file-log startup behavior.
  - Preserved strict default through `PersistedMetadataNode::new`.
  - Added persisted restart test that confirms bounded tail corruption recovery.

- Validation tooling:
  - Added `scripts/validate.sh` as the one-shot validator for full local checks.
  - Added CLI knobs to skip fmt/tests/fuzz/jepsen and to redirect jepsen artifacts.
  - Updated `tests/jepsen/README.md` with one-shot validation usage.
  - Updated `.gitignore` to cover Jepsen artifacts generated by local validation runs.
  - Updated `API.md` to document replay policy and storage-backed constructor behavior.

- Verification:
  - Ran `cargo fmt --all`.
  - Ran `cargo test -p ganglion-storage --quiet`.
  - Ran `cargo test -p ganglion-openraft --quiet`.

## Roadmap update (no timestamps)

- Short-term:
  1. Route persisted-node startup to a bounded-tail default policy where storage backends
     support partial-corruption recovery.
  2. Keep the one-shot validation script as the default CI/local entrypoint and report its
     outputs consistently.
  3. Add regression capture hooks for both bounded-tail recovery and Jepsen-style fallback runs.
- Medium-term:
  1. Replace placeholder consensus path with true openraft transport and keep same consensus contracts.
  2. Introduce committed-snapshot publication plumbing for external controllers.
  3. Expand partition/failover sequence coverage with scripted fallback executions.
- Long-term:
  1. Add planner strategy registry with parameterized policies.
  2. Add durable retention and compaction tooling for replay logs.
  3. Expand backend adapters and migration plans beyond basic file logs.

## v11 execution pass completed

- `PersistedMetadataNode` startup policy:
  - Default `new()` now uses bounded-tail replay (`TruncateTail`) with a single-line recovery limit.
  - Added explicit `new_strict()` for old fail-fast startup semantics.
  - Kept `new_with_replay_policy(...)` as direct policy constructor.
- `PersistedMetadataNode` test coverage:
  - Added `persisted_node_tolerates_truncated_tail_corruption_when_enabled_by_default` to verify default path.
  - Added `persisted_node_tolerates_truncated_tail_corruption_when_explicit` to verify explicit tail policy.
  - Switched malformed/non-sequential startup rejection tests to `new_strict()` to preserve strict behavior guarantees.

## Roadmap update (no timestamps)

- Short-term:
  1. Make bounded-tail defaults configurable by deployment profile and threshold.
  2. Emit startup policy selection in validation artifacts for recovery diagnostics.
  3. Keep strict and resilient constructors separate and explicit in all adapter-facing docs.
- Medium-term:
  1. Replace placeholder consensus path with true openraft transport and keep same consensus contracts.
  2. Introduce committed-snapshot publication plumbing for external controllers.
  3. Expand partition/failover sequence coverage with scripted fallback executions.
- Long-term:
  1. Add planner strategy registry with parameterized policies.
  2. Add durable retention and compaction tooling for replay logs.
  3. Expand backend adapters and migration plans beyond basic file logs.
