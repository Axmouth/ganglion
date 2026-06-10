# Ganglion Plans and Roadmap

This file is append-only.

Plan snapshots are recorded in-place and treated as immutable.  
New changes are added as new sections at the bottom.  
`WORKLOG.md` contains the detailed chronology of work performed.

## Plan Snapshot v0

### Goal

Build `ganglion` as an embedded consensus + placement primitive with no queue-specific assumptions in core.

`fibril` is a first-party consumer of `ganglion`, not its driver.

### Architecture Baseline

- `ganglion-core`
  - Generic term/epoch model.
  - Consensus and linearizable metadata store traits.
  - Membership and assignment snapshots.
  - Pluggable planning traits and default deterministic policy.
- `ganglion-openraft`
  - Adapter using `openraft` for embedded consensus.
  - Separate transport/storage concerns from domain payloads.
- `ganglion-storage` (or `ganglion-keratin`)
  - raft hard-state and log storage adapter over append-only local storage.
- `ganglion-coordination`
  - Coordination snapshot interfaces and watch abstraction.

### Scope Boundary

- In Core:
  - Node identity, term/epoch/generation.
  - Opaque command payload typing.
  - Assignment topology models, planner outputs, durable snapshots.
  - Error and status contracts.
- Out of Core:
  - queue/topic semantics, ack state, DLQ logic, producer IDs.
  - transport decisions and broker/client policy decisions.

### v0 Delivery Sequence

1. Implement `ganglion-core` data model, traits, and tests.
2. Add deterministic planner with pure transition outputs.
3. Add minimal `openraft` adapter with in-memory transport test harness.
4. Add Keratin-backed storage adapter (optional until validated with integration tests).
5. Add first `fibril` integration: map `coordination.rs` ownership shapes onto ganglion primitives.

### Exit Criteria

- The plan-model and traits are stable and tested.
- `openraft` adapter elects leader and enforces monotonic generation/epoch fencing.
- One deterministic planner computes owner/follower sets from a snapshot.
- Planner output can be serialized to/from storage or transport safely.

## Short-Term Roadmap

#### Resolution target: working baseline metadata consensus plane

1. Create workspace/crates for `ganglion-core`, `ganglion-planner`, and `ganglion-openraft`.
2. Add docs and examples for node identity, generation epochs, and membership snapshots.
3. Implement pure planner policy with test vectors for stability and no movement churn.
4. Add basic role assignment transitions:
   - owner demotion intent
   - follower promotion intent
   - no-op refresh intent
5. Add minimal coordination snapshot watcher contract.
6. Produce an example mapping from existing `fibril` planning types to ganglion core.

## Medium-Term Roadmap

#### Resolution target: usable HA metadata plane

1. Add local persistence and WAL durability paths with deterministic recovery.
2. Add coordination-level CAS/transaction style apply semantics.
3. Add controller-loop style transition planner integration and operation journaling.
4. Add follower-status style observability: tails, lag, applied event offsets, epoch.
5. Expand failure policies:
   - clean vs degraded ownership handoff pre-conditions
   - reject unsafe promotions with explicit refusal reasons.
6. Publish integration notes for single-cluster operation.

## Long-Term Roadmap

#### Resolution target: production-ready pluggability and portability

1. Add alternative coordination backends via the same trait interfaces.
2. Add advanced placement policies:
   - cooldown-aware balancing
   - health-aware movement
   - rack/zone diversification
3. Add richer operator tooling for plan visualisation and safe dry-run.
4. Formalize schema evolution and snapshot migration.
5. Provide optional replication helpers as downstream examples, not defaults.

## Preservation Rules

- Never rewrite existing plan snapshots.
- Add new snapshots instead of editing earlier sections.
- Keep the plan directory-local to this repo under `/home/george/code/ganglion`.

## Plan Snapshot v1

### Goal

Introduce the first concrete API scaffold in-repo so implementation can start without external coordination.

### Completed in this snapshot

- Bootstrapped a Rust workspace in `/home/george/code/ganglion`.
- Added `ganglion-core` crate with first generic API:
  - resource identity
  - node metadata
  - assignments
  - durability policy + resolution
  - deterministic planning trait and baseline implementation
  - local transition planner between snapshots
- Added `ganglion-openraft` crate with an in-memory metadata adapter scaffold and consensus trait.
- Added a mutable API reference in `API.md`.

### Next in this sequence

- Replace the in-memory adapter with true openraft wiring.
- Add workspace-level docs/examples for planner and openraft lifecycle.
- Add minimal integration points for a coordinator-style assignment loop.

## Plan Snapshot v2

### Goal

Lock down API behavior with deterministic tests before introducing transport-specific openraft code.

### Completed in this snapshot

- Added core planner/transition test coverage in `ganglion-core`:
  - deterministic owner retention
  - owner reassign with epoch increment when the prior owner disappears
  - empty-node failure mode
  - local transition derivation for demotion scenarios
- Added adapter lifecycle tests in `ganglion-openraft`:
  - non-leader updates rejected
  - stale generations rejected
  - generation advances through in-memory apply path

### Next low-resolution steps

- Add a small coordination-facing contract crate for watch/read/update operations:
  - snapshot observer abstraction
  - static fixture provider
  - role-specific callback interface
- Replace `InMemoryMetadataNode` behavior with a real openraft-backed store implementation in a second pass, keeping the same trait surface.

## Plan Snapshot v3

### Goal

Introduce a shared coordination provider contract consumed by coordinators and metadata watchers.

### Completed in this snapshot

- Added `ganglion-coordination` crate and workspace membership.
- Added `CoordinationProvider` with `snapshot`, `owns_resource`, `follows_resource`, and `watch`.
- Added mutable `InMemoryCoordination` and immutable `StaticCoordination`.
- Added helper filters for owned/followed resources and test coverage.

### Next low-resolution steps

- Replace the `InMemoryMetadataNode` internals with real openraft-backed storage while preserving `MetadataConsensus`.
- Add a small control-loop integration example that plans using `DeterministicPartitionPlacement` and applies via watcher updates.
- Add external backend adapter surfaces behind the coordination trait.

## Plan Snapshot v4

### Goal

Harden the initial consensus scaffold before introducing real openraft transport.

### Completed in this snapshot

- Stabilized `ganglion-openraft` in-memory adapter internals:
  - Added local node identity retention on each adapter instance.
  - Added raft-like write guards: leader-only proposals, stale term rejection, and generation monotonicity.
  - Added log entry growth with per-term sequence restart semantics (term bump clears old in-memory command history).
  - Added explicit helpers for leader term, last index, last term, and log length.
- Added/updated unit tests in `ganglion-openraft`:
  - reject non-leader proposals
  - reject stale generation updates
  - reject stale term updates
  - reset log on term bump
  - verify ID visibility (`local_node_id` / `leader_id`)
- Cleaned transition planning behavior in `ganglion-core`:
  - `(None, None)` role transitions are now treated as no-op and do not emit transition entries.
  - Kept planner transition test expectations aligned with deterministic follower retention.

### Next low-resolution steps

1. Add a small control-loop integration (planner -> `MetadataConsensus` apply -> watcher update) to exercise the full flow.
2. Introduce storage adapter traits and a Keratin-backed persistence path for recovery semantics.
3. Replace the in-memory consensus internals with an openraft-powered storage+transport layer while preserving `MetadataConsensus`.

## Plan Snapshot v5

### Goal

Complete the v4 control-loop minimum and document where to continue.

### Completed in this snapshot

- Added a control-loop helper in `ganglion-openraft`:
  - `plan_and_publish(consensus, proposer, planner, input, publish)`
  - computes a planner output, applies it through consensus, and only publishes on success.
- Added integration-style tests covering publish behavior:
  - publishes only after successful consensus commit
  - does not invoke publish callback when consensus rejects.
- Added explicit no-op guard for local transition planning behavior:
  - `(LocalRole::None, LocalRole::None)` yields no transition entry in core tests.
- Cleaned warning-level test hygiene in control-loop test scaffolding.
- Appended this pass in `PLAN.md`, `WORKLOG.md`, and `API.md`.

### Next low-resolution steps

1. Introduce pluggable persistence abstractions (storage traits / log adapters) that can be implemented by Keratin.
2. Replace the in-memory consensus node with a real openraft-backed implementation behind the same `MetadataConsensus` trait.
3. Add a controller-facing crate utility that wires planning, consensus proposal, and watch notification into a reusable loop.

## Plan Snapshot v6

### Goal

Strengthen reliability and fault-injection confidence before introducing real openraft/keratin.

### Completed in this snapshot

- Added Rust `.gitignore` entries for build artifacts and fuzz-related outputs.
- Added initial property-based fuzzing coverage for the planner in `ganglion-core` using `proptest`.
- Added test scaffolding to keep the empty-cluster failure path explicit:
  - non-empty resources with no nodes are rejected by planner.
- Tightened planner follower invariants:
  - follower selection now guarantees followers are unique and never include owner.
- Captured validation direction for external fault-injection:
  - add Jepsen-style cluster tests in a separate test harness.

### Next low-resolution steps

1. Add more fuzz targets:
   - planner with `existing` assignments as input
   - `plan_local_assignment_transitions` consistency under random snapshot pairs
   - `control_loop` failure modes across proposer identity and stale generations
2. Stand up a shared Jepsen scenario scaffold (binary + scripts) for openraft failover/rejoin/partition cases.
3. Add a keratin-backed storage adapter behind new persistence traits, then wire it into `InMemoryMetadataNode` integration tests as a replacement path.

## Plan Snapshot v7

### Goal

Increase confidence breadth before moving to real transport/storage and keep test scaffolding actionable.

### Completed in this snapshot

- Added deeper fuzz coverage to `ganglion-core`:
  - planner behavior with randomized existing assignments,
  - local transition consistency checks over randomized previous/next snapshots,
  - preservation of follower ordering where existing follower assignment can be reused.
- Added fuzz coverage for `ganglion-openraft` control-loop rejection matrix:
  - rejects non-leader proposals,
  - rejects stale generations after leader/proposer validation.
- Added fuzz coverage for direct `apply_snapshot` term handling:
  - stale term rejection,
  - stale generation rejection,
  - acceptance with equal/newer term overrides.
- Added Jepsen planning artifact:
  - `JEPSEN_PLAN.md` with scenario scaffolding and acceptance checks.
- Expanded `.gitignore` Rust/dev-ignore coverage for IDE/temp/fuzzer artifacts.

### Next in this sequence

1. Add persistent proptest regression capture and replay tooling (`proptest-regressions/` conventions in CI).
2. Materialize the Jepsen scaffold directory with commandable scenarios and a CI gateable target.
3. Implement the Keratin-backed persistence adapter and run the same fuzz/control-loop suites through it.

## Plan Snapshot v8

### Goal

Stabilize the persistence + testing substrate so `ganglion` has a recoverable, pluggable metadata plane before replacing the in-memory adapter.

### Completed in this snapshot

- Added `ganglion-storage` crate and wired it into workspace and `ganglion-openraft`.
- Added two persistence implementations:
  - `InMemoryMetadataLog` for tests and non-durable mode.
  - `FileMetadataLog` for append-only, newline-delimited JSON replay files.
- Added storage schema conversion for `CoordinationSnapshot` that preserves `ResourceIdentity` keys in a durable, deterministic form.
- Added `PersistedMetadataNode` as a persistence-backed constructor around the same consensus surface:
  - restores `current_term` from file,
  - restores latest snapshot,
  - validates recovery path through replay tests.
- Added `Storage` error plumbing in openraft adapter:
  - `OpenraftAdapterError::Storage(String)`,
  - conversion from `MetadataLogError`,
  - storage-backed tests for restart + stale write cases.
- Expanded and stabilized test harnesses:
  - `scripts/proptest.sh` now supports `list`, `run`, and per-crate replay workflows and regression directories.
  - `scripts/jepsen.sh` forwards to scenario runner.
  - `tests/jepsen/run.sh` supports `list`, `all`, and single scenario invocation.
  - `tests/jepsen/scenarios/*.sh` created for baseline/failover, split-brain, and crash/recovery.
- Cleaned `proptest` callback capture logic in openraft fuzz tests to ensure success-path publication assertions are checking the same witness.
- Validated end-to-end by running `cargo test --quiet` successfully for storage, openraft, and workspace crates.

### Short-Term Roadmap

#### Resolution target: persistence confidence on a single-node plane

1. Keep persistence adapter interfaces stable while we wire in additional backends:
   - file append log,
   - Keratin append-only segment backend,
   - optional in-memory/ephemeral mode.
2. Consolidate recovery and corruption behavior:
   - explicit malformed-log error handling,
   - bounded replay windows for startup tests.
3. Promote proptest regression artifacts to CI-preserved, reviewable fixtures.

### Medium-Term Roadmap

#### Resolution target: consensus transport and observability integration

1. Add true openraft integration behind `MetadataConsensus` while preserving current method contracts.
2. Attach a watcher/event stream for committed snapshot publication and node health state.
3. Add a cluster-level control-plane smoke path with leader transfer and partition simulation.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability and ergonomics

1. Provide backend-neutral planner policy registry with strategy names and parameterized options.
2. Add richer snapshot compaction and migration hooks before large historical logs.
3. Publish reusable Jepsen execution package for consensus and failover validation.

## Plan Snapshot v9

### Goal

Make durable recovery behavior deterministic under malformed logs and make fault-injection entrypoints executable today.

### Completed in this snapshot

- Hardened `FileMetadataLog` replay validation:
  - explicit non-sequential index checks (`1,2,3...`),
  - zero-index rejection,
  - parse-context on malformed JSON lines.
- Added `ganglion-storage` file-log tests:
  - round-trip append/reload,
  - comments/blank lines support,
  - malformed JSON rejection,
  - non-sequential index and zero-index rejections.
- Added `ganglion-openraft` persisted-node tests for startup against corrupted and non-sequential logs.
- Updated Jepsen scenario scripts to run focused local Rust smoke checks as a fallback when Jepsen/Clojure is unavailable.
- Updated regression fixture header comments to neutral, repo-internal wording.

### Short-term Roadmap

#### Resolution target: deterministic recovery and actionable failure-injection gates

1. Define and test a bounded replay policy for partially corrupted logs.
2. Add CI-native artifact preservation for `proptest-regressions` outputs.
3. Add a minimal wrapper for replaying known Jepsen-like sequences without Clojure dependency.

### Medium-Term Roadmap

#### Resolution target: transport-real metadata plane

1. Replace placeholder consensus behavior with real openraft transport.
2. Add event/stream publication API for committed snapshots.
3. Introduce cluster-level role transfer and partition recovery scenarios.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability

1. Provide planner strategy registry with parameterized pluggable policies.
2. Add snapshot compaction and migration utilities around durable logs.
3. Expand backend adapters (Keratin + optional external stores) with stable trait compatibility.

## Plan Snapshot v10

### Goal

Expose configurable startup recovery behavior and reduce validation friction by adding one-shot checks.

### Completed in this snapshot

- `ganglion-storage` recovery policy:
  - Added `FileMetadataReplayPolicy` (`Strict`, `TruncateTail { max_tail_lines }`).
  - Added bounded-tail recovery in file-log replay for malformed/non-sequential/zero-index tails.
  - Added tests for:
    - strict behavior remains rejecting malformed inputs,
    - bounded tail corruption recovery succeeds when inside limit,
    - bounded tail corruption recovery fails when it exceeds limit.
- `ganglion-openraft` persistence integration:
  - Added `PersistedMetadataNode::new_with_replay_policy`.
  - Kept default constructor strict behavior by default.
  - Added test showing persisted nodes can recover state from bounded corrupt tails using
    `TruncateTail`.
- Validation ergonomics:
  - Added `scripts/validate.sh` one-shot entrypoint that can run:
    - `cargo fmt --all --check`,
    - `cargo test --workspace --quiet`,
    - `scripts/proptest.sh run`,
    - `tests/jepsen/run.sh all`.
  - Added skip flags and configurable Jepsen artifact directory support.
  - Added `tests/jepsen/artifacts/**` and related script artifacts to `.gitignore`.
- Documentation:
  - Updated `tests/jepsen/README.md` with one-shot validation usage.
  - Updated `API.md` to include configurable storage replay policy and constructor.

### Short-term Roadmap

#### Resolution target: confidence before transport swap

1. Use bounded-tail recovery policy from the default persisted constructor when operating
   with known append-only segment backends.
2. Make proptest and Jepsen runs part of one scriptable local/CI default path.
3. Add one small malformed-tail regression that validates openraft restart behavior end-to-end.

### Medium-Term Roadmap

#### Resolution target: transport-real metadata plane

1. Replace placeholder consensus path with true openraft transport and persistence lifecycle hooks.
2. Expose committed-snapshot events with optional watchers or event sinks.
3. Expand Jepsen fallback scenario replay and make it script-driven from the same validator.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability

1. Add planner strategy registry and parameterized strategy options.
2. Add snapshot compaction/migration and history retention tooling.
3. Offer stable backend adapters for Keratin and additional WAL/event log stores.

## Plan Snapshot v11

### Goal

Route persisted startup semantics to resilience-first defaults while keeping strict-mode for strict environments.

### Completed in this snapshot

- `PersistedMetadataNode` startup behavior:
  - `new()` now defaults to `FileMetadataReplayPolicy::TruncateTail` with a bounded tail limit.
  - Added `new_strict()` as an explicit strict-startup constructor.
  - Preserved `new_with_replay_policy(...)` for direct policy selection.
- Recovery test coverage:
  - Added default-startup test that verifies malformed single-line tails are tolerated and state is recovered.
  - Added strict-mode coverage for malformed and non-sequential startup payloads.
  - Kept explicit policy override coverage for bounded-tail behavior.
- Documentation:
  - Updated API docs for default-vs-strict persisted constructors.

### Short-Term Roadmap

#### Resolution target: resilience and operator control

1. Add configurable recovery policy selection by deployment profile.
2. Record recovery-policy choices in validation/diagnostic outputs.
3. Keep strict-mode explicit in test scaffolding and adapter factories where needed.

### Medium-Term Roadmap

#### Resolution target: transport-real metadata plane

1. Replace placeholder consensus path with true openraft transport and persistence lifecycle hooks.
2. Expose committed-snapshot events with optional watchers or event sinks.
3. Expand Jepsen fallback sequence coverage for restart/recovery and leader contention.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability

1. Add planner strategy registry and parameterized strategy options.
2. Add snapshot compaction/migration and durable retention tooling.
3. Offer stable backend adapters for Keratin and additional WAL/event log stores.

## Plan Snapshot v16

### Goal

Make persisted startup constructor behavior explicit and validated on the one-shot validation path.

### Completed in this snapshot

- Added a dedicated persisted startup smoke test:
  - `persisted_node_startup_entrypoint_smoke_checks` exercises all `PersistedMetadataNode` startup constructors from one path:
    - `new`
    - `new_strict`
    - `new_with_replay_profile`
    - `new_with_replay_profile_resolution`
    - `new_with_replay_profile_str` (explicit and env-driven)
    - `new_with_profile_env`
    - `new_from_env`
    - `new_with_replay_policy`
    - `new_with_tail_replay_limit`
- Added a dedicated Jepsen fallback scenario:
  - `tests/jepsen/scenarios/04-startup-entrypoint-smoke.sh`
- Extended one-shot validation to run that constructor smoke with explicit request/result tracking:
  - `scripts/validate.sh` new `startup_smoke` phase
  - `--skip-startup-smoke` opt-in skip flag
- Updated `tests/jepsen/README.md` scenario inventory.

### Short-Term Roadmap

#### Resolution target: operational confidence

1. Add startup-policy selection coverage for explicit profile transitions under mixed valid tails.
2. Expand startup smoke coverage to include startup constructor + control-loop replay end-to-end.
3. Keep source/target matrices for constructor precedence and failure modes in docs and tests.

### Medium-Term Roadmap

#### Resolution target: transport-real metadata plane

1. Replace placeholder consensus path with true openraft transport and persistence lifecycle hooks.
2. Add committed-snapshot event stream for controllers and watchers.
3. Expand partition/failover sequence coverage with scripted fallback executions.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability

1. Add planner strategy registry and parameterized strategy options.
2. Add snapshot compaction/migration and durable retention tooling.
3. Offer stable backend adapters for Keratin and additional WAL/event log stores.

## Plan Snapshot v14

### Goal

Add fuzz-backed and diagnostic confidence for persisted startup profile behavior, then keep the next recovery/consensus work moving.

### Completed in this snapshot

- `ganglion-openraft`:
  - Added proptest fuzzing for replay profile parsing:
    - valid forms (`default`, `resilient`, `strict`, padded/upper-case, `tail:n`, `truncate_tail:n`, numeric `n`)
    - invalid inputs that must fail.
  - Added profile/constructor property assertions that validate:
    - parsed profile maps back to expected replay policy,
    - startup profile diagnostics are consistent with constructor selection.
  - Added env-var validation for invalid `GANGLION_PERSISTED_REPLAY_PROFILE` values.
- Validation:
  - Kept one-shot validation summary behavior and ensured replay profile details are preserved in test artifacts.
- Documentation:
  - Appended this snapshot to `WORKLOG.md` and `PLAN.md` without rewriting earlier entries.

### Short-Term Roadmap

#### Resolution target: operational robustness

1. Add persisted recovery fuzz cases for mixed good/bad tails around corruption depth boundaries.
2. Add a structured replay-profile source of truth for adapters that use env/config precedence chains.
3. Add persisted startup constructor smoke tests to every release-like validation path.

### Medium-Term Roadmap

#### Resolution target: transport-real metadata plane

1. Replace placeholder consensus path with true openraft transport and keep same consensus contracts.
2. Expose committed-snapshot event plumbing for controller observers.
3. Expand partition/failover sequence coverage with scripted fallback executions.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability

1. Add planner strategy registry and parameterized strategy options.
2. Add snapshot compaction/migration and durable retention tooling.
3. Offer stable backend adapters for Keratin and additional WAL/event log stores.

## Plan Snapshot v15

### Goal

Harden persisted recovery boundaries and make startup profile selection explicit across env/config input paths.

### Completed in this snapshot

- `ganglion-openraft`:
  - Added property coverage for bounded-tail startup recovery with mixed tail patterns (bad JSON, comments, blank lines) under varying limits and valid-prefix sizes.
  - Added explicit startup-profile resolution types:
    - `PersistedMetadataReplayProfileSource` (`Explicit`, `Environment`, `Default`),
    - `PersistedMetadataReplayProfileResolution` (profile + provenance).
  - Added constructor `PersistedMetadataNode::new_with_replay_profile_str(...)` to resolve an optional raw profile against env/default in one place.
  - Added tests validating precedence (`explicit` vs env) and resolved source reporting.

### Short-Term Roadmap

#### Resolution target: operational robustness

1. Add constructor/diagnostic smoke checks for every persisted startup entrypoint in validation scripts.
2. Add persisted recovery fuzz cases for mixed valid tail corruption and boundary-limit transitions in startup policy selection.
3. Track resolved startup profile source in CI artifacts where possible.

### Medium-Term Roadmap

#### Resolution target: transport-real metadata plane

1. Replace placeholder consensus path with true openraft transport and keep same consensus contracts.
2. Expose committed-snapshot event plumbing for controller observers.
3. Expand partition/failover sequence coverage with scripted fallback executions.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability

1. Add planner strategy registry with parameterized strategy options.
2. Add snapshot compaction/migration and durable retention tooling.
3. Offer stable backend adapters for Keratin and additional WAL/event log stores.

## Plan Snapshot v13

### Goal

Make persisted startup policy pluggable through explicit profiles and surface startup diagnostics in one-shot validation output.

### Completed in this snapshot

- `PersistedMetadataReplayProfile` added in `ganglion-openraft` with `FromStr` support and env integration.
- Added constructors:
  - `PersistedMetadataNode::new_with_replay_profile(...)`
  - `PersistedMetadataNode::new_from_env(...)`
- Added startup diagnostics on `PersistedMetadataNode`:
  - `startup_replay_profile()`
  - `startup_replay_policy()`
- Validation: `scripts/validate.sh` now writes a `validate-summary.json` artifact containing:
  - selected `--skip-*` behavior,
  - jepsen artifact directory,
  - resolved persisted replay profile metadata from `GANGLION_PERSISTED_REPLAY_PROFILE`.
- Tests:
  - added parsing tests for replay profile strings and defaults,
  - added constructor-level profile diagnostics tests.
- Documentation:
  - updated `API.md` and `tests/jepsen/README.md` for profile parsing, env var, and summary output.

### Short-Term Roadmap

#### Resolution target: resilience and operator control

1. Extend validation diagnostics to include actual resolved startup policy for persisted adapters per run.
2. Add a small fuzz target for profile parsing and mixed startup constructor coverage.
3. Keep strict-mode APIs explicit and visible in adapter factories.

### Medium-Term Roadmap

#### Resolution target: transport-real metadata plane

1. Replace placeholder consensus path with true openraft transport and persistence lifecycle hooks.
2. Expose committed-snapshot events with optional watchers or event sinks.
3. Expand Jepsen fallback sequence coverage for restart/recovery and leader contention.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability

1. Add planner strategy registry and parameterized strategy options.
2. Add snapshot compaction/migration and durable retention tooling.
3. Offer stable backend adapters for Keratin and additional WAL/event log stores.

## Plan Snapshot v12

### Goal

Make persisted startup recovery policy configuration explicit without removing bounded-tail defaults.

### Completed in this snapshot

- `PersistedMetadataNode`:
  - Added `new_with_tail_replay_limit(...)` for deployment-specific tolerance tuning.
  - Kept default bounded-tail startup and strict constructor behavior unchanged.
  - Reused existing policy override path for arbitrary replay strategies.
- Test coverage:
  - Added default + explicit + custom-limit restart recovery assertions in openraft persisted node tests.

### Short-Term Roadmap

#### Resolution target: resilience and operator control

1. Make profile-driven policy defaults available at adapter construction sites (for example, env/config file).
2. Capture chosen startup policy in validation artifacts and local startup diagnostics.
3. Keep strict-mode APIs explicit in all persisted adapter call paths.

### Medium-Term Roadmap

#### Resolution target: transport-real metadata plane

1. Replace placeholder consensus path with true openraft transport and persistence lifecycle hooks.
2. Expose committed-snapshot events with optional watchers or event sinks.
3. Expand Jepsen fallback sequence coverage for restart/recovery and leader contention.

### Long-Term Roadmap

#### Resolution target: production-ready pluggability

1. Add planner strategy registry and parameterized strategy options.
2. Add snapshot compaction/migration and durable retention tooling.
3. Offer stable backend adapters for Keratin and additional WAL/event log stores.
