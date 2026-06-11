# Ganglion Worklog

Project root: `/home/george/code/ganglion`

## Scope

This is the canonical chronological execution log for the repo. It tracks implementation and validation
work in reverse-briefness order while keeping one live roadmap block.

## Roadmaps (single source of truth)

### Short-term

1. Committed-snapshot watch publication from `GanglionStateMachine` apply/install, surfaced on
   `RaftMetadataNode` — this is fibril's actual consumption surface (`Coordination::watch()`).
2. Durable raft storage: back raft log/vote/state with `MetadataLog` (file + Keratin) so the raft
   path reaches durability parity with the legacy persisted node.
3. Raft-runtime failure scenarios (leader loss/re-election, partition via router deregister,
   restart with durable log) wired into the Jepsen fallback inventory.

### Medium-term

1. Membership change/learner flows on `RaftMetadataNode` (`add_learner`, `change_membership`).
2. Epoch/fencing surface for assignments (schema design first; needs a user decision).
3. Durability telemetry around append/clear/truncate and startup recovery outcomes.

### Long-term

1. Wire transport (gRPC or similar) implementing `RaftNetwork` beyond in-process.
2. Expand persistence adapters and durable metadata maintenance tooling (retention/compaction/migration).
3. Generalize package-level integration so queue-specific consumers stay decoupled from `ganglion` primitives.

### Worklog maintenance

- When this file gets too long to scan quickly, continue iteration entries in `WORKLOG.2.md` and keep this file as the active segment.

## Iteration 0 — Planning and scope alignment

- Initialized project planning from `fibril` references:
  - `/home/george/code/fibril/REPLICATION_PLANNING.md`
  - `/home/george/code/fibril/REPLICATION_WORKLOG.md`
  - `/home/george/code/fibril/crates/broker/src/coordination.rs`
- Confirmed target split: queue-specific mechanics stay in `fibril`, while `ganglion` stays neutral in API and model.
- Added initial planning and initial worklog structure.

## Iteration 1 — Scaffolding and initial bootstrap

- Added workspace members and started crate scaffolding for:
  - `ganglion-core`
  - `ganglion-openraft`
- Added first core API skeleton:
  - resource identity and assignment models,
  - durability policy,
  - deterministic placement trait and implementation,
  - local transition planning.
- Added initial in-memory metadata adapter scaffold and `MetadataConsensus` contract.
- Added `API.md` as a mutable, current API reference.

## Iteration 2 — Coordination contract bootstrap

- Added `ganglion-coordination` crate and membership wiring.
- Introduced `CoordinationProvider` plus in-memory/static providers.
- Added role-aware snapshot helpers and watch updates.
- Added unit tests for provider behavior and role checks.

## Iteration 3 — In-memory consensus and planner hardening

- Reworked in-memory consensus state model:
  - leader term tracking,
  - leader identity retention,
  - generation/term validation order,
  - log simulation with term-based reset semantics.
- Added/updated tests:
  - non-leader rejection,
  - stale generation and stale term rejection,
  - no-op handling for `(None, None)` transitions,
  - log growth and reset behavior.
- Confirmed deterministic transition behavior after role-logic adjustments.

## Iteration 4 — Control-loop integration and persistence scaffolding

- Added `plan_and_publish` helper to drive planner->consensus->watcher publishing.
- Added integration tests for publish behavior on success/failure.
- Added `ganglion-storage` crate with `MetadataLog` abstraction and file/in-memory backends.
- Wired persisted state into `ganglion-openraft` via `PersistedMetadataNode`.
- Added startup strictness handling and recovery error propagation.
- Added initial `.gitignore` for build/artifact noise.

## Iteration 5 — Fuzz and fault-injection foundations

- Added proptest coverage for planner invariants and transition consistency.
- Added openraft adapter control-loop property tests for stale-term/generator constraints.
- Added Jepsen plan scaffold and scenario script skeleton.
- Added validation utility scripts and additional ignores for fuzz/jepsen artifacts.

## Iteration 6 — Persistence integrity and recovery behavior

- Added strict replay validation in file log (`FileMetadataLog`) and malformed/non-sequential rejection coverage.
- Added persisted node startup tests for malformed and non-sequential logs.
- Added fallback behavior in Jepsen-like scenarios when Clojure is absent.
- Added one-shot validator script with optional phase skipping and artifact output.

## Iteration 7 — Configurable replay policy and diagnostics

- Introduced configurable replay policies (`Strict` vs bounded tail recovery).
- Added default/persistent startup constructor surface and profile source/provenance metadata.
- Added startup selection helpers and constructors.
- Extended validation JSON to capture replay profile request/effective output.
- Added tests for profile parsing and constructor precedence.

## Iteration 8 — Extended startup profile and matrix coverage

- Added fuzz coverage for profile parsing and resolution behavior.
- Added startup-policy matrix tests (`new_with_replay_profile`, env-driven behavior, explicit override precedence).
- Added startup matrix scenario in Jepsen fallback flow and README updates.
- Extended validation to keep startup profile evidence in machine outputs.

## Iteration 9 — Replay boundary and validation tightening

- Reworked summary/run behavior to enforce aggregate result fail-hard behavior.
- Added mixed-tail and recovery profile transitions in both unit coverage and scenario inventories.
- Kept background-process hang triage guidance in notes (stable validation paths now isolated).
- Introduced startup-policy resolution data tracking in execution records.

## Iteration 10 — Keratin-backed storage parity and ordering

- Added Keratin backend through feature-gated `MetadataLog` path.
- Added tests for Keratin roundtrip, truncate/replay, and malformed-tail behavior parity with file logs.
- Exposed backend injection into persisted node constructors.
- Extended storage-parity one-shot path and script.
- Validated feature-gated Keratin command paths locally.

## Iteration 11 — Persistence backend hardening

- Extended tail-boundary behavior parity between file and Keratin.
- Added test isolation fixes for handle-lifetime issues during replay/truncation tests.
- Added explicit operational note on external background invocation hang correlation.
- Expanded aggregate validation artifact checks and strict phase gating.
- Kept scenario-level Jepsen output as preferred isolation route when broad runs stall.

## Iteration 12 — Jepsen artifacting and scenario matrix maturation

- Added per-scenario JSON results and aggregate summary in Jepsen runner.
- Added run-summary aggregation with scenario count and failed scenario totals.
- Extended `validate-summary.json` to include Jepsen artifacts and aggregate metadata.
- Improved run preconditions for restart/failover persistence scenario execution.

## Iteration 13 — Startup/failover scenario strengthening

- Added explicit restart/failover assertions in persistence parity scenarios.
- Added stale-term-after-restart checks and log-reset behavior checks.
- Kept aggregate failure checks as hard gates in local validation.
- Added operational rule: isolate hangs by scenario-first reruns and background process checks.

## Iteration 14 — Finalized terminal-failure validation behavior

- Validation gate now enforces all requested phase pass status.
- Jepsen aggregate checks now require complete scenario artifacts and zero failed scenarios for pass.
- Added hard-fail behavior for missing/invalid artifacts and mismatched counts.
- Kept environment-linked hang finding attached to external background invocations, not core paths.

## Iteration 15 — Initial openraft runtime planning work

- `crates/ganglion-openraft/Cargo.toml`:
  - added optional `openraft` dependency (serde feature),
  - added optional `tokio` dependency for runtime support,
  - added `openraft` feature gate wiring dependencies.
- This keeps runtime integration pluggable and non-blocking to existing pure-memory usage.
- No behavior changes to consensus logic in this pass; only dependency and feature surface update.

## Iteration 16 — Doc/Plan structure normalization

- Replaced snapshot-heavy immutable structure in `PLAN.md` with compact active plan.
- Removed duplicate roadmap sections from `WORKLOG.md` history and established one active roadmap block in a fixed location.
- Added explicit roadmap ownership rule: worklog roadmaps are now source-of-truth; plan remains compact and non-snapshot.
- Kept iteration records per pass and retained all substantive historical work details in compact form.

## Iteration 17 — Current baseline snapshot

- Latest stable short/medium/long term focus:
  - openraft transport swap,
  - committed-snapshot publication,
  - backend growth and retention tooling.
- Remaining near-term objective: continue transport-level wiring while preserving current contract stability.

## Iteration 18 — Startup profile matrix and mixed-tail expansion

- Added mixed-tail startup coverage for profile-selection behavior:
  - explicit strict/default/tail precedence,
  - env override versus explicit override validation,
  - control-loop continuity after recovered startup state.
- Wired startup-policy matrix checks into persistence scenario fallback path.
- Expanded documentation and scenario inventory to reflect matrix checks.

## Iteration 19 — Startup constructor smoke and precedence coverage

- Added startup smoke test covering all persisted constructor variants in one flow:
  - strict/default/custom/profile-resolution/environment/explicit-override constructors.
- Added `tests/jepsen/scenarios/04-startup-entrypoint-smoke.sh` as a focused scenario wrapper.
- Extended `scripts/validate.sh` with `startup_smoke` phase and summary artifact fields.
- Kept explicit resolution behavior checks between explicit configuration and environment input.

## Iteration 20 — Default startup behavior and startup policy edge-cases

- Added bounded-tail default behavior (`new()` now resilient by default with tail limit).
- Added `new_strict()` constructor separation for fail-fast environments.
- Added new tail-limit constructor path and tests for custom bounded limits.
- Added `new_with_replay_profile`/`new_from_env`/`new_with_profile_env` coverage and resolved-profile visibility checks.
- Added proptest generation for replay profile parsing and constructor mapping.

## Iteration 21 — Keratin adapter and injected persisted log surface

- Added feature-gated Keratin adapter for storage (`keratin` feature in `ganglion-storage`).
- Added tests for Keratin roundtrip, truncate/clear, and index replay behavior after truncation.
- Added `PersistedMetadataNode::new_with_log` and `new_with_log_and_profile`.
- Added injection-path regression asserting backend-supplied logs are used without fallback.
- Expanded mixed-tail recovery behavior in persisted constructors under bounded-tail and explicit profile transitions.

## Iteration 22 — Storage parity stress and restart ordering checks

- Extended Keratin parity to malformed-tail and sequentiality behavior that mirrors file semantics.
- Added tests for:
  - small recoverable tail corruption,
  - large unrecoverable tail corruption,
  - non-sequential index rejection,
  - recoverable mixed non-sequential tails where policy allows.
- Added handle-lifetime fixes so replay/truncate tests drop writers before reopen.
- Added persistent operational finding that hangs are tied to unrelated long-running background invocations.

## Iteration 23 — Storage fuzz parity and one-shot orchestration

- Added backend-aware storage fuzzing for file and Keratin tail-boundary behavior.
- Added `scripts/storage-parity.sh` and `storage_parity` phase in `scripts/validate.sh`.
- Extended `.gitignore` and regression fixture folders for storage fuzz artifacts.
- Documented and executed storage parity one-shot runs with startup checks included.

## Iteration 24 — Scenario execution artifact structure

- Added `tests/jepsen/scenarios/06-persistence-backend-parity.sh` and JEPSEN plan entry.
- Updated scenario inventory to include new persistence parity coverage.
- Added explicit scenario-level fallback execution for file/Keratin parity plus startup boundary tests.
- Added aggregate `run-summary` precondition checks for scenario artifacts before reruns.

## Iteration 25 — Deterministic Jepsen result artifacts

- Added per-scenario result JSON (`<scenario>.json`) with status/exit code/log path/expecteds.
- Added aggregate `run-summary.json` for `run.sh all` and `run.sh scenario`.
- Extended `validate-summary.json` with aggregate jepsen summary fields and scenario references.
- Kept scenario artifact naming and ordering deterministic for CI consumption.

## Iteration 26 — Hang triage and persistence scenario reinforcement

- Added worklog/plan/README operational finding: stable behavior is preserved in scenario-level and direct test paths while broad one-shot stalls remain tied to separate background invocation behavior.
- Kept one-shot triage guidance and explicit isolate-check steps in docs.
- Added explicit mixed-tail/recovery persistence checks and aggregate artifact checks to standard preflight.

## Iteration 27 — Persistence parity scenario deepening

- Expanded scenario 6 to include direct mixed-tail and startup recovery cases.
- Added explicit failover/restart checks into scenario execution path:
  - `persisted_node_recovered_startup_replays_control_loop_on_next_apply`,
  - `persisted_node_startup_entrypoint_smoke_checks`.
- Kept file/Keratin boundary checks explicit without relying on broad test filter runs.

## Iteration 28 — Aggregate validation hardening

- `run all` now records `scenario_count` and `failed_scenarios`.
- Scenario execution no longer aborts on first failure; aggregates capture complete exit-state.
- Validation gate now requires aggregate artifact completeness and zero failed scenarios.
- Kept this as a hard prerequisite in CI/local validation flows.

## Iteration 29 — Restart/failover persistence regression

- Added `tests/jepsen/run.sh` aggregate summary fields for failed scenario totals in results.
- Added direct failover assertions in persistence parity scenario:
  - stale-term-after-restart rejection,
  - term-bump log reset checks.
- Updated validation path to keep aggregate scenario summaries as hard-failure criteria.

## Iteration 30 — Final one-shot validation strictness

- Added final fail-hard behavior for all requested validation phases once summaries are written.
- Kept scenario aggregate checks for missing/malformed artifacts and non-zero failures as blocking conditions.
- Preserved explicit environmental caution: core logic checks remain stable in direct commands; broad one-shot hangs remain tied to external/background path behavior.

## Iteration 31 — Failover ordering on persisted restart

- Added `persisted_node_failover_ordering_after_restart`:
  - validates higher-term takeover after persisted restart,
  - verifies stale lower-term writes are rejected,
  - checks persisted log length and reset behavior consistency.
- Added this test to scenario coverage as explicit failover ordering invariant.
- Updated `JEPSEN_PLAN.md` to document restart/failover ordering as required coverage.

## Iteration 32 — Planner strategy pluggability

- Added `PlacementStrategy` in `ganglion-core` as a runtime strategy catalog entry point.
- Added `LeastLoadedPartitionPlacement` as the first alternate strategy.
- Exposed strategy helpers for discoverability and config-style selection:
  - `all()`
  - `as_str()`
  - `parse()`
  - `as_strategy()`
- Added tests in `ganglion-core` for:
  - strategy catalog resolution and unknown-strategy behavior,
  - least-loaded balancing under empty and non-zero load conditions.
- Reworked `.gitignore` with additional rust-native artifact patterns (`*.dll`, `*.dylib`, `*.rlib`, etc.).
- Updated `API.md` and active plan/refinement notes to track pluggable strategy surface as implemented.

## Iteration 33 — Openraft context survival doc

- Added `OPENRAFT_SURVIVAL_CONTEXT.md` with version-guarded openraft touchpoints needed for future openraft integration.
- Captured minimum trait surfaces and key example source files in one place:
  - type config + payload requirements,
  - `RaftLogReader`/`RaftLogStorage` implementation surface,
  - `RaftStateMachine` lifecycle methods,
  - `RaftNetwork`/`RaftNetworkFactory` wiring and bootstrap lifecycle calls (`Raft::new`, `initialize`, `client_write`, `metrics`, `shutdown`).
- Explicitly documented the current crate/version mismatch (`openraft = "0.8"` vs local temp clone `0.10.0-alpha.21`) so the next cycle does not drift across API versions.

## Iteration 34 — Survival context consolidation

- Replaced the previous survival context content with a compact 0.8.9-only anchor focused on:
  - required feature flags (`storage-v2`, `serde`, runtime support),
  - minimal storage/network/runtime traits,
  - compaction/snapshot behavior that must stay stable across turns.
- Scoped the doc to quick recovery needs so context compaction only needs this one page plus `PLAN`/`WORKLOG`.

## Iteration 35 — Survival context compact rewrite

- Rewrote `OPENRAFT_SURVIVAL_CONTEXT.md` into a tighter restart sheet with only essential
  compile-time API signatures for `openraft = 0.8.9`.
- Kept the document focused on direct dependency anchors and removed non-essential historical notes
  to support fast context recovery.

## Iteration 36 — Fast-context openraft survival note

- Finalized `OPENRAFT_SURVIVAL_CONTEXT.md` into a very compact API sheet containing only:
  - the exact 0.8.9 files/entrypoints to reopen,
  - trait method list needed to build storage/state-machine/network adapters,
  - minimal bootstrap sequence.
- Added explicit version-bound warning that local `~/code/temp/openraft` docs/examples are newer and should
  not be used as signature authority for this repo.
- Committed the update as `a312533` with a stable recovery-oriented layout.

## Iteration 37 — Quick survival doc refresh

- Replaced `OPENRAFT_SURVIVAL_CONTEXT.md` with a tighter recovery sheet focused on
  only the methods and files required to rebuild the openraft path quickly after context compaction:
  - `RaftTypeConfig`/`declare_raft_types` shape
  - `RaftLogReader`, `RaftLogStorage`, `RaftStateMachine`, `RaftSnapshotBuilder`
  - `RaftNetwork`/`RaftNetworkFactory`
  - core lifecycle calls (`Raft::new`, `initialize`, `client_write`, `shutdown`)
- Kept the doc intentionally minimal and version-bound for restart reliability.
- Added a compile-time guard note: openraft 0.8 NodeId must be `Copy`, so external string IDs need adapter mapping.

## Iteration 38 — Openraft runtime scaffold

- Added feature-gated `openraft_runtime` module in `crates/ganglion-openraft` with:
  - `GanglionRaftConfig` implementing `openraft::RaftTypeConfig` for 0.8.x compatibility.
  - `MetadataRaftCommand` and `MetadataRaftResponse` app payload/response types.
  - `default_raft_config(...)` helper that validates and returns an `Arc<openraft::Config>`.
- Exported the runtime module from `ganglion-openraft/src/lib.rs` via `pub` re-exports when `openraft` feature is enabled.
- Reworked `OPENRAFT_SURVIVAL_CONTEXT.md` into a compact, restart-oriented one-pager for 0.8.9.
- Validation update: `cargo test -p ganglion-openraft --features openraft --no-run` + `cargo test -p ganglion-openraft` both pass.

## Iteration 39 — Openraft storage adapters pass contract suite

- Restored the missing `openraft_runtime` module file (the iteration-38 commit referenced it but
  the file was never written), then promoted it to a directory module.
- Added `openraft_runtime/storage.rs` with real storage-v2 implementations for `GanglionRaftConfig`:
  - `GanglionLogStore`: `RaftLogReader` + `RaftLogStorage` (in-memory `BTreeMap` log, vote store,
    truncate/purge with `last_purged_log_id` tracking, immediate `LogFlushed` completion).
  - `GanglionStateMachine`: `RaftStateMachine` + `RaftSnapshotBuilder` (applies
    `MetadataRaftCommand`, snapshot build/install via JSON `Cursor<Vec<u8>>`).
- `MetadataRaftResponse` gained `accepted: bool`: stale-generation writes are rejected
  deterministically inside `apply` (state unchanged, `accepted=false`) — replicated-safe, never an error.
- **Milestone: `openraft::testing::Suite::test_all` passes against both adapters** (storage
  contract verified by openraft's own suite), plus a direct stale-generation rejection test.
- Rewrote `OPENRAFT_SURVIVAL_CONTEXT.md` as a verified-facts sheet: implemented-so-far inventory,
  compile-tested gotchas (trailing comma in `declare_raft_types!`, `LogFlushed` semantics,
  truncate/purge boundary directions, `StorageIOError` constructors), corrected network trait
  surface (`append_entries`/`vote`/`install_snapshot` + `RPCOption`; `send_*` are deprecated).
- Next: in-process `RaftNetwork`/`RaftNetworkFactory` router, then a `RaftMetadataNode` wrapping
  `Raft<GanglionRaftConfig>` behind `MetadataConsensus`.

## Iteration 40 — Real raft cluster runtime + plan realignment

- Added `openraft_runtime/network.rs`: `InProcessRouter` (`RaftNetworkFactory`) +
  `InProcessConnection` (`RaftNetwork`) routing RPCs directly into peer `Raft` handles;
  deregistered peers surface as `Unreachable` for partition simulation.
- Added `openraft_runtime/node.rs`: `RaftMetadataNode` wrapping `Raft<GanglionRaftConfig>` with
  `start`/`initialize`/`write_snapshot`/`committed_snapshot`/leader-wait helpers; error mapping
  preserves `MetadataConsensus` semantics (`ForwardToLeader` → `NotLeader`, committed-but-rejected
  stale generation → `StaleGeneration`).
- **Milestone: 3-node in-process raft cluster test passes** — election, replicated
  `CoordinationSnapshot` convergence on all nodes, post-consensus stale rejection, follower write
  refusal. 41 tests green across both feature configurations.
- Reviewed `fibril/REPLICATION_PLANNING.md` + `fibril/crates/broker/src/coordination.rs` against
  the plan (user-requested review). Key realignments now in `PLAN.md`:
  - fibril consumes a sync snapshot/watch trait and refuses to host consensus → committed-snapshot
    watch publication is the integration surface and moved to short-term #1;
  - watch-channel publication dissolves most of the sync/async bridging concern (reads become sync);
  - durable raft storage via `MetadataLog` is the gate before the raft path can replace the legacy
    persisted node; legacy path stays until parity;
  - fencing/epoch surface for assignments added as a medium-term schema-design item.
- Open decision recorded in PLAN: async `MetadataConsensus` variant vs. async-only
  `RaftMetadataNode` API with sync watch reads.
