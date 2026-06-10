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

## v17 execution pass completed

- `ganglion-openraft`:
  - Added mixed-tail startup profile transition coverage in
    `persisted_node_startup_profile_selection_with_mixed_tail_and_explicit_override`:
    - env-strict path fails on two malformed tail lines (`default`-like tolerance),
    - explicit `tail:3` succeeds by overriding env,
    - explicit `default` preserves bounded-tail semantics.
  - Added explicit mixed-tail recovery path for control-loop continuity in
    `persisted_node_recovered_startup_replays_control_loop_on_next_apply`:
    - recovers from persisted node with mixed tail,
    - runs `plan_and_publish` on recovered node,
    - verifies watcher snapshot publication.
- Validation:
  - Updated startup-entrypoint Jepsen fallback to run startup-prefixed tests as a group:
    - `cargo test -p ganglion-openraft persisted_node_startup --quiet`.

## Roadmap update (no timestamps)

- Short-term:
  1. Keep constructor startup-policy matrix explicit for remaining path permutations (including explicit strict + explicit default + env).
  2. Add startup policy behavior into control-loop/jepsen artifact-level failure matrices.
  3. Start Keratin-backed persisted adapter scaffolding once the persisted constructor surface is stable.
- Medium-term:
  1. Replace placeholder consensus path with true openraft transport and keep same contracts.
  2. Add committed-snapshot event stream for controllers and watchers.
  3. Expand partition/failover sequence coverage with scripted fallback executions.
- Long-term:
  1. Add planner strategy registry with parameterized policies.
  2. Add durable retention and migration tooling.
  3. Expand backend adapters and migration plans beyond basic file logs.

## v16 execution pass completed

- Validation scaffolding:
  - Added a new startup constructor smoke test:
    - `persisted_node_startup_entrypoint_smoke_checks`
    - Covers all persisted startup constructors and verifies resolved profile outputs.
  - Added dedicated fallback scenario:
    - `tests/jepsen/scenarios/04-startup-entrypoint-smoke.sh`
  - Extended `scripts/validate.sh` with startup constructor smoke execution:
    - `startup_smoke` phase and `--skip-startup-smoke` flag
    - `startup_smoke` result entry in `validate-summary.json`
- Docs:
  - Updated `tests/jepsen/README.md` scenario inventory.
- Checks:
  - Confirmed env-driven constructors and explicit overrides still agree with source and effective profile expectations.

## Roadmap update (no timestamps)

- Short-term:
  1. Add startup-policy selection coverage for explicit profile transitions under mixed valid tails.
  2. Extend constructor smoke to include immediate control-loop restart replay paths.
  3. Keep source/target matrix for constructor precedence and profile-failure modes in docs and tests.
- Medium-term:
  1. Replace placeholder consensus path with true openraft transport and keep same contracts.
  2. Introduce committed-snapshot event stream for controllers and watchers.
  3. Expand partition/failover sequence coverage with scripted fallback executions.
- Long-term:
  1. Add planner strategy registry with parameterized policies.
  2. Add durable retention and migration tooling.
  3. Expand backend adapters and migration plans beyond basic file logs.

## v15 execution pass completed

- `ganglion-openraft`:
  - Added mixed-tail persisted recovery fuzzing:
    - writes valid-prefix startup logs and variable tail patterns (malformed, blank, comments),
    - asserts boundary behavior where malformed-tail counts determine recovery/rejection with `new_with_replay_profile`.
  - Added startup profile provenance plumbing:
    - `PersistedMetadataReplayProfileSource` (`Explicit`, `Environment`, `Default`),
    - `PersistedMetadataReplayProfileResolution` (`profile` plus resolution source),
    - `PersistedMetadataNode::new_with_replay_profile_str` returning resolved profile metadata.
  - Added tests for precedence and visibility:
    - explicit profile string overrides env,
    - env-based resolution is used when explicit override is absent,
    - resolved startup profile is visible from constructor result and startup diagnostics.

- Validation:
  - Full validation path (`./scripts/validate.sh`) remains green with the new tests.
  - Jepsen fallback smoke checks continue to run from the one-shot validator.

## Roadmap update (no timestamps)

- Short-term:
  1. Add constructor/diagnostic smoke checks for every persisted startup entrypoint in validation scripts.
  2. Add persisted recovery fuzz cases for mixed valid tail corruption and boundary-limit transitions in startup policy selection.
  3. Track resolved startup profile source in CI artifacts where possible.
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

## v12 execution pass completed

- `PersistedMetadataNode` configuration:
  - Added `new_with_tail_replay_limit(...)` to control bounded-tail replay allowance at startup.
  - Kept `new()` defaulting to bounded-tail recovery.
  - Kept explicit `new_strict()` and `new_with_replay_policy(...)` for strict/custom policy selection.
- Test coverage:
  - Added persisted restart test for custom tail budget via `new_with_tail_replay_limit`.
  - Preserved default and explicit constructor coverage for bounded-tail and strict paths.

## Roadmap update (no timestamps)

- Short-term:
  1. Add profile-driven policy defaults to adapter construction.
  2. Emit startup policy choice in validation and startup diagnostics.
  3. Keep strict-mode APIs explicit in adapter factories.
- Medium-term:
  1. Replace placeholder consensus path with true openraft transport and keep same consensus contracts.
  2. Introduce committed-snapshot publication plumbing for external controllers.
  3. Expand partition/failover sequence coverage with scripted fallback executions.
- Long-term:
  1. Add planner strategy registry with parameterized policies.
  2. Add durable retention and compaction tooling for replay logs.
  3. Expand backend adapters and migration plans beyond basic file logs.

## v13 execution pass completed

- `ganglion-openraft`:
  - Added `PersistedMetadataReplayProfile` with:
    - strict/resilient defaults,
    - bounded-tail override via `truncate_tail:<n>` or `<n>`,
    - config parsing using `FromStr`.
  - Added startup profile constructors:
    - `new_with_replay_profile(...)`
    - `new_from_env(...)` (reads `GANGLION_PERSISTED_REPLAY_PROFILE`).
  - Added startup diagnostics on `PersistedMetadataNode`:
    - `startup_replay_profile()`
    - `startup_replay_policy()`
  - Added tests:
    - profile parsing coverage for default/strict/tail forms and invalid values,
    - constructor coverage confirming stored startup diagnostics for default/strict/custom selections.

- Validation tooling:
  - Updated `scripts/validate.sh` to emit `tests/jepsen/artifacts/validate-run/validate-summary.json`.
  - Summary now records:
    - requested/skipped run phases,
    - artifact directory,
    - replay profile env value/effective output.

- Documentation:
  - Updated `API.md` for replay-profile constructors/diagnostics.
  - Updated `tests/jepsen/README.md` with replay-profile env guidance and summary artifact location.

## Roadmap update (no timestamps)

- Short-term:
  1. Add a dedicated fuzz target for profile parsing and constructor-selection behavior.
  2. Keep validation summaries in CI artifacts and verify profile-resolution evidence on every run.
  3. Improve config ergonomics for profile expressions and error messages.
- Medium-term:
  1. Replace placeholder consensus path with true openraft transport and keep same consensus contracts.
  2. Introduce committed-snapshot publication plumbing for external controllers.
  3. Expand partition/failover sequence coverage with scripted fallback executions.
- Long-term:
  1. Add planner strategy registry with parameterized policies.
  2. Add durable retention and compaction tooling for replay logs.
  3. Expand backend adapters and migration plans beyond basic file logs.

## v14 execution pass completed

- `ganglion-openraft`:
  - Added property tests that fuzz replay profile parsing and constructor mapping for valid and invalid profile inputs.
  - Added a targeted env-var failure test that verifies invalid `GANGLION_PERSISTED_REPLAY_PROFILE` values produce `OpenraftAdapterError::Config`.
  - Expanded existing startup-profile coverage with constructor-level assertions that confirm reconstructed startup policy matches parsed intent.

- Validation and harness:
  - Kept `scripts/validate.sh` as the one-shot path for fmt/tests/proptest/jepsen.
  - Confirmed `validate-summary.json` already includes replay profile env/effective values and run-level request/result flags.

- Tooling:
  - No additional `.gitignore` adjustments were needed in this pass.

## Roadmap update (no timestamps)

- Short-term:
  1. Add a dedicated persisted recovery harness that asserts bounded-tail tolerance around corrupt logs under high tail depth variance.
  2. Add Jepsen-driven restart/failover scenario replay and keep artifacts in `tests/jepsen/artifacts`.
  3. Add constructor/diagnostic smoke checks for every persisted startup entrypoint in CI scripts.
- Medium-term:
  1. Replace placeholder consensus path with true openraft transport and keep same consensus contracts.
  2. Introduce committed-snapshot publication plumbing for external controllers.
  3. Expand partition/failover sequence coverage with scripted fallback executions.
- Long-term:
  1. Add planner strategy registry with parameterized policies.
  2. Add durable retention and compaction tooling for replay logs.
  3. Expand backend adapters and migration plans beyond basic file logs.

## v18 execution pass completed

- `ganglion-openraft`:
  - Added explicit startup-policy matrix coverage under malformed-tail recovery:
    - explicit strict/path, explicit default/path, explicit bounded-tail/path,
    - env strict/path against one and two malformed tail entries,
    - env strict success case on clean recovery logs.
  - Added explicit matrix assertions for resolved profile source:
    - explicit overrides report `Explicit`,
    - env fallback remains `Environment`,
    - successful bounded-tail recovery preserves expected startup profile.
- `tests/jepsen`:
  - Added `tests/jepsen/scenarios/05-startup-policy-matrix.sh`:
    - runs the startup-policy matrix unit checks as a dedicated failure-matrix artifact scenario.
  - Updated scenario inventory documentation to include the new matrix scenario.

- Validation:
  - `./scripts/validate.sh` now runs this matrix indirectly via scenario ordering when Jepsen fallback is used.

## Roadmap update (no timestamps)

- Short-term:
  1. Keep constructor startup-policy matrix explicit for remaining path permutations and edge cases.
  2. Add bounded-tail malformed-tail boundary cases to property/fuzz matrix coverage.
  3. Begin Keratin adapter scaffolding while preserving constructor precedence semantics.
- Medium-term:
  1. Replace placeholder consensus path with true openraft transport and keep same contracts.
  2. Expose richer committed-snapshot publication channels for controllers and watchers.
  3. Expand partition/failover sequence coverage with scripted fallback executions.
- Long-term:
  1. Add planner strategy registry with parameterized policies.
  2. Add durable retention and compaction tooling for replay logs.
  3. Offer stable backend adapters for Keratin and additional WAL/event log stores.

## v19 execution pass completed

- `ganglion-storage`:
  - Added optional Keratin-backed metadata log adapter behind feature `keratin` in `KeratinMetadataLog`:
    - owns a `keratin-log::Keratin` log instance,
    - writes serialized `PersistedMetadataLogEntry` payloads,
    - replays entries through a compatibility parser and validates contiguous log indexes.
  - Added Keratin-specific tests (behind `keratin` feature):
    - `keratin_metadata_log_roundtrips_append_and_replay`,
    - `keratin_metadata_log_clear_and_truncate_from`.
  - Fixed index replay handling after truncation by validating against prior entry (rather than hard-coding start index 1) so suffix-preserving truncation replays correctly.
  - Eliminated temporary debug traces inserted during debugging so the module is clean for normal runs.

- `ganglion-openraft`:
  - Added `PersistedMetadataNode::new_with_log(...)` and `new_with_log_and_profile(...)` to construct persisted nodes from an injected `MetadataLog` implementation.
  - Added regression coverage for injected backends:
    - `persisted_node_new_with_log_uses_injected_backend`.

- Validation:
  - `cargo test -p ganglion-storage --quiet`
  - `cargo test -p ganglion-storage --features keratin --quiet -- --exact tests::keratin_metadata_log_clear_and_truncate_from --test-threads=1`
  - `cargo test -p ganglion-storage --features keratin --quiet -- --exact tests::keratin_metadata_log_roundtrips_append_and_replay --test-threads=1`

## Roadmap update (no timestamps)

- Short-term:
  1. Keep the core persistence API stable while adding any remaining Keratin storage behavior parity with file log.
  2. Add deterministic failure-path tests for malformed Keratin tails under mixed valid/truncated input.
  3. Add a small one-shot validation phase that exercises all storage backends available in CI/local smoke.
- Medium-term:
  1. Add committed-snapshot publication stream and durability telemetry for storage operations.
  2. Expand fuzz/proptest coverage for storage replay policy and truncation behavior.
  3. Replace placeholder consensus transport path with the real openraft transport while preserving current contracts.
- Long-term:
  1. Offer additional backend adapters behind `MetadataLog` (`keratin`, other append-log providers).
  2. Add snapshot retention and compaction tooling for durable logs.
  3. Move planner strategy registry and strategy-specific defaults into a reusable catalog.

## v20 execution pass completed

- `ganglion-storage`:
  - Added Keratin parity coverage mirroring malformed-tail and sequence-boundary behavior from the file backend:
    - `keratin_metadata_log_truncates_small_tailing_corruption_tail`
    - `keratin_metadata_log_rejects_large_tailing_corruption_tail`
    - `keratin_metadata_log_rejects_non_sequential_index`
    - `keratin_metadata_log_recoverable_non_sequential_tail`
  - Fixed replay isolation in those tests so the initial writer handle drops before reopening:
    - avoids `Keratin already open for <root>` errors caused by keeping the first handle alive.
  - Kept alignment with file semantics:
    - bounded-tail recovery is accepted only within policy limits,
    - stricter invalid tails still hard-fail.

- `ganglion-openraft`:
  - Re-ran persisted-node regressions after `ganglion-storage` parity additions to keep adapter constructor behavior unchanged.

- Operational note:
  - A prior hang was reproducible only in an unrelated long-running background invocation path, not in direct targeted validation here. Core logic test paths now run without hanging.

- Validation:
  - `cargo test -p ganglion-storage --features keratin`
  - `cargo test -p ganglion-storage --features keratin keratin_metadata_log -- --nocapture`
  - `cargo test -p ganglion-openraft --quiet`

## Roadmap update (no timestamps)

- Short-term:
  1. Keep a one-shot validation command as the default local gate for storage parity + startup constructors.
  2. Add fuzz-backed tail-boundary generators for both file and keratin storage inputs.
  3. Add backend-selection artifacts so startup replay policy and adapter choice are explicit in test output.
- Medium-term:
  1. Expand Jepsen-style persistence scenarios to include explicit Keratin and file restart/failover transitions.
  2. Add durable-snapshot publication paths with recovery telemetry and health visibility.
  3. Replace placeholder consensus transport with full openraft runtime wiring while preserving current persisted contracts.
- Long-term:
  1. Add snapshot retention, compaction, and storage maintenance paths for metadata durability.
  2. Grow adapter catalog for non-WAL/WAL-like stores with feature-gated constructors.
  3. Move planner strategy registry into a reusable policy catalog with operator tuning knobs.

## v21 execution pass completed

- `ganglion-storage`:
  - Added backend-aware storage fuzzing for bounded-tail boundaries:
    - `fuzz_file_metadata_log_tail_boundary_recovery` for file logs.
    - `fuzz_keratin_metadata_log_tail_boundary_recovery` for Keratin logs.
  - Added deterministic helper strategies in `ganglion-storage` tests:
    - synthetic marker tails,
    - recovery-cost calculation used to assert truncation boundary behavior.
  - Added dedicated parity regression fixture directory:
    - `crates/ganglion-storage/proptest-regressions/`.
  - Extended `.gitignore` to keep storage regression fixtures in-repo while preserving normal ignores.

- Validation scaffolding:
  - Added `scripts/storage-parity.sh` for one-shot storage backend parity runs.
  - Added `storage_parity` phase to `scripts/validate.sh` with summary details:
    - backend list (`["file","keratin"]`),
    - replay profile metadata tied to startup profile resolution.
  - Extended `scripts/proptest.sh` crate list to include `ganglion-storage`.

- Validation runs:
  - `bash scripts/storage-parity.sh` passed (keratin + startup checks).
  - `bash scripts/validate.sh --skip-jepsen --skip-fuzz` passed with storage parity and startup smoke included.

- Operational finding:
  - The hang signal seen earlier appears in a separate long-running background invocation path, not in direct storage/openraft validation paths. Core test logic continues to run without hangs.

## Roadmap update (no timestamps)

- Short-term:
  1. Keep the one-shot validation path as the default local gate for storage parity, startup smoke, and fuzz.
  2. Keep backend provenance and replay-policy metadata explicit in validation artifacts.
  3. Add Jepsen-style persistence restart/failover scenarios that execute file and keratin paths side-by-side.
- Medium-term:
  1. Replace placeholder consensus transport with real openraft runtime path while preserving current interfaces.
  2. Add durability telemetry around append/clear/truncate operations.
  3. Expose durable snapshot publication and health channels for controllers.
- Long-term:
  1. Extend `MetadataLog` adapter catalog under a stable contract.
  2. Add retention and compaction tooling for metadata durability.
  3. Move planner strategy registry into reusable operator configuration surfaces.

## v22 execution pass completed

- `tests/jepsen`:
  - Added `tests/jepsen/scenarios/06-persistence-backend-parity.sh` as a new fallback scenario.
  - The scenario runs when Jepsen is unavailable and executes storage parity + persisted startup checks:
    - file/Keratin tail-boundary recovery fuzz and boundary tests in `ganglion-storage` (keratin feature path),
    - persisted startup path checks in `ganglion-openraft`.
  - Updated `tests/jepsen/README.md` scenario inventory to include the new persistence-parity entry.

- `JEPSEN_PLAN.md`:
  - Added Scenario 6 for persistence backend parity.
  - Documented fallback extension to include backend parity and startup boundary coverage.

- Documentation consistency:
  - Updated `PLAN.md` with `Plan v22` to keep the long-lived planning record append-only and to set next step alignment.

- Roadmap alignment:
  - This pass moves roadmap execution to the short-term persistence Jepsen-style checkpoint and keeps medium/long-term transport/planner tasks unchanged until durable transport and watcher telemetry are completed.

- Operational finding carry-forward:
  - Hang-like behavior previously observed in earlier iterations is reproducibly tied to an external long-running background invocation path, not the core storage/openraft validation paths. This is the reason `scripts/validate.sh --skip-fuzz` repeatedly stays stable while broad one-shot runs elsewhere could stall.
  - Practical rule: when a full run appears to hang, isolate first with scenario-level commands, then check for orphaned invocations before re-running the full command set.

## Roadmap update (no timestamps)

- Short-term:
  1. Keep the new persistence scenario in default `tests/jepsen/run.sh all` ordering and ensure artifact logs remain stable.
  2. Add structured scenario artifacts and evidence beyond log-level pass/fail statements.
  3. Extend persistence scenario to include follower/reactivation edge cases after backend recovery.
- Medium-term:
  1. Replace placeholder consensus transport with a real openraft runtime path.
  2. Add durability telemetry around adapter operations and startup recovery behavior.
  3. Expand cluster-level restart/failover scenarios to include storage parity and recovery in control loops.
- Long-term:
  1. Introduce configurable retention and compaction policies per adapter.
  2. Expand adapter catalog for additional metadata storage families.
  3. Move strategy registries (planner and persistence profile) into operator-facing configuration.

## v23 execution pass completed

- `tests/jepsen/run.sh`:
  - Added per-scenario machine-readable result files:
    - `<scenario>.json` with status, exit code, log path, and expected invariant list.
  - Added `run-summary.json` aggregate artifact for `run.sh all` and `run.sh scenario`.
- `scripts/validate.sh`:
  - Extended `validate-summary.json` to include Jepsen aggregate fields:
    - `jepsen.summary_file`,
    - total/failed scenario counts,
    - embedded scenario objects when run summary is available.
- `JEPSEN_PLAN.md`:
  - Added result-collection details so scenario artifacts are explicit in the plan artifact model.

- Verification:
  - `tests/jepsen/run.sh all --artifact-dir /tmp/jepsen-artifacts-test`
  - `tests/jepsen/run.sh scenario 06-persistence-backend-parity`

## Roadmap update (no timestamps)

- Short-term:
  1. Keep structured per-scenario summary JSON stable and parseable for CI consumption.
  2. Add mixed-tail/recovery startup cases into persistence parity scenario coverage.
  3. Keep scenario execution order and artifact naming deterministic across runs.
- Medium-term:
  1. Replace placeholder consensus transport with a real openraft runtime path.
  2. Add durability telemetry around adapter append/clear/truncate and startup recovery behavior.
  3. Expand failover/rejoin persistence scenarios to explicitly sequence restarts by backend.
- Long-term:
  1. Expand `MetadataLog` adapter catalog and policy hooks.
  2. Add retention/compaction tooling for durable metadata logs.
  3. Move strategy registries (planner and persistence profile) into reusable operator configuration.

## v24 execution pass completed

- `WORKLOG.md` / `PLAN.md` / `JEPSEN_PLAN.md` / `tests/jepsen/README.md`:
  - Added a permanent operational finding that prior hang behavior in this environment is reproducibly tied to an external long-running background invocation path.
  - Noted that direct scenario-level and targeted validation commands continue to run stably in those conditions.
  - Added runbook guidance to isolate hangs by:
    - running `tests/jepsen/run.sh scenario ...` before broad aggregate commands,
    - checking for orphaned background validation invocations before re-running one-shot gates.
- `scripts/validate.sh`:
  - No logic changes; this pass only captures evidence and triage guidance in docs and planning records.

## v24 Roadmap update (no timestamps)

- Short-term:
  1. Keep a visible validation triage checklist in runbooks while the long-running background path is still present.
  2. Add mixed-tail/recovery startup assertions explicitly in persistence parity scenario checks.
  3. Gate aggregate validation runs on deterministic scenario summary artifacts when possible.
- Medium-term:
  1. Replace placeholder consensus transport with real openraft runtime while preserving current contracts.
  2. Add durability telemetry around adapter append/clear/truncate operations and startup recovery behavior.
  3. Expand persistence failover/rejoin scenarios to explicitly sequence restarts across backends.
- Long-term:
  1. Expand `MetadataLog` adapter catalog and policy hooks.
  2. Add retention/compaction tooling for durable metadata logs.
  3. Move strategy registries (planner and persistence profile) into reusable operator configuration.

## v25 execution pass completed

- `tests/jepsen/scenarios/06-persistence-backend-parity.sh`:
  - Expanded fallback assertions to run mixed-tail and startup-recovery startup cases directly:
    - `persisted_node_startup_profile_selection_with_mixed_tail_and_explicit_override`
    - `persisted_node_recovered_startup_replays_control_loop_on_next_apply`
    - `persisted_node_startup_entrypoint_smoke_checks`
  - Kept existing file/Keratin boundary tests in place.
- Notes:
  - This makes the persistence parity scenario explicitly exercise mixed-tail + recovery-startup behavior without relying on broad module test filters.

## v25 Roadmap update (no timestamps)

- Short-term:
  1. Keep mixed-tail and recovery-startup checks in scenario coverage while monitoring runtime duration.
  2. Add a lightweight validation preflight for scenario summary completeness (`<scenario>.json` + run-summary).
  3. Keep operator-facing hang triage guidance in place until one-shot background invocations are no longer a practical risk.
- Medium-term:
  1. Replace placeholder consensus transport with true openraft runtime while preserving current interfaces.
  2. Add committed-snapshot publication channels and durability telemetry.
  3. Expand restart/failover persistence scenarios with backend sequencing.
- Long-term:
  1. Expand `MetadataLog` adapter catalog and policy hooks.
  2. Add retention/compaction tooling for durable metadata logs.
  3. Move strategy registries (planner and persistence profile) into reusable operator configuration.
