# Ganglion Detailed Design (implementation-ready)

Companion to `PLAN.md` (which stays compact). This file holds enough detail that the remaining
work is mostly mechanical. Sections are ordered by execution phase. Each phase lists API shapes,
semantics, and the tests that gate it. Update this file as phases land; move finished phases into
`WORKLOG.md` iteration entries.

## Phase G1 — Epoch issuance + guarded proposals — DONE (iteration 49)

Spike-verified context: fibril and ganglion both carry `PartitionAssignment.epoch` and the
mapping is 1:1. What is missing is *issuance discipline* (who bumps, when) and *race safety*
(two controllers proposing concurrently must not interleave destructively).

### G1.1 Pure epoch rules (`ganglion-core`)

```rust
/// Why an assignment's epoch advanced (or didn't).
pub enum EpochTransition { Unchanged, OwnerChanged, NewAssignment, ExplicitFence }

/// Pure rule: next epoch for a desired assignment given the committed one.
pub fn next_assignment_epoch(
    committed: Option<&PartitionAssignment>,
    desired_owner: &str,
) -> (u64, EpochTransition);
```

Rules (fibril `REPLICATION_PLANNING.md`: "Every leadership change increments an epoch"):
- No committed assignment → epoch 1, `NewAssignment`.
- Owner unchanged → same epoch, `Unchanged` (follower-only changes do NOT fence).
- Owner changed → committed.epoch + 1, `OwnerChanged`.
- `ExplicitFence` is caller-driven (operator "fence now"): committed.epoch + 1 with same owner —
  separate helper `fence_assignment_epoch(committed: &PartitionAssignment) -> u64`.

Plus a snapshot-level helper that applies the rule across a planned snapshot:

```rust
/// Stamp epochs on `desired` based on `committed`; returns transitions for telemetry/logs.
pub fn stamp_assignment_epochs(
    committed: &CoordinationSnapshot,
    desired: &mut CoordinationSnapshot,
) -> Vec<(ResourceIdentity, EpochTransition)>;
```

Invariants (tested): epochs never decrease; owner change always bumps; follower churn never
bumps; removing + re-adding a resource continues from max(seen epoch)+1 — which requires the
helper to consider `committed` even for resources missing from `desired`'s previous generation
(i.e. tombstone awareness stays the caller's job; document this).

### G1.2 Guarded (CAS) proposals (`ganglion-openraft`)

New replicated command variant (state-machine-deterministic, like stale-generation):

```rust
pub enum MetadataRaftCommand {
    ApplySnapshot(CoordinationSnapshot),
    /// Commit only if the committed generation still equals `expected_generation`.
    ApplySnapshotGuarded { expected_generation: u64, snapshot: CoordinationSnapshot },
}
```

Response grows a structured rejection (replacing the bare bool — small breaking change, fine
pre-1.0):

```rust
pub struct MetadataRaftResponse {
    pub accepted: bool,
    pub rejection: Option<MetadataRejection>,   // None iff accepted
    pub snapshot: CoordinationSnapshot,
}
pub enum MetadataRejection { StaleGeneration, GenerationMismatch { expected: u64, actual: u64 } }
```

`RaftMetadataNode` surface:

```rust
/// CAS write: rejects with `OpenraftAdapterError::GenerationMismatch` if another
/// proposal committed in between. Controller loops: re-read, re-plan, retry.
pub async fn write_snapshot_guarded(&self, expected_generation: u64, snapshot: CoordinationSnapshot)
    -> Result<MetadataRaftResponse, OpenraftAdapterError>;
```

`OpenraftAdapterError` gains `GenerationMismatch`. Semantics: the check runs inside `apply`
(replicated, deterministic), NOT at proposal time — proposal-time checks cannot be race-free.

### G1.3 Controller-loop helper (`ganglion-openraft`)

The fibril controller loop shape (read → pure plan → CAS write → retry) as one helper so every
consumer gets the race-safe pattern:

```rust
/// Run one guarded control iteration: read committed state, produce a desired
/// snapshot via `plan` (pure!), stamp epochs, propose guarded; retry on
/// GenerationMismatch up to `max_retries`.
pub async fn plan_and_propose_guarded<F>(node: &RaftMetadataNode<LS>, plan: F, max_retries: usize)
    -> Result<MetadataRaftResponse, OpenraftAdapterError>
where F: Fn(&CoordinationSnapshot) -> CoordinationSnapshot;
```

Increments `generation` itself (read.generation + 1) so planners don't hand-roll it.

### G1 tests (gate)

- Unit matrix for `next_assignment_epoch` / `stamp_assignment_epochs` (owner change, follower
  churn, new, fence, mixed snapshot).
- SM determinism: guarded command in the running-max model fuzz (extend
  `fuzz_state_machine_matches_running_max_model` with guarded variants; model tracks expected
  rejections).
- Race test: two concurrent `plan_and_propose_guarded` loops on one cluster, N iterations each;
  assert final generation == total accepted proposals, no lost updates (each loop's accepted
  writes are all visible in sequence), epochs monotonic per resource throughout (assert via watch
  history).
- Cross-version note: new command variant is additive to the WAL format; old WALs replay fine
  (serde enum tagging). Test: reopen a WAL written pre-G1 (fixture file).

## Phase G2 — Telemetry + topology introspection — DONE (iteration 50)

### G2.1 Durability telemetry

Plain atomic counters, no metrics-crate dependency (consumers map into their own systems):

```rust
#[derive(Debug, Default)]
pub struct StorageTelemetry { /* AtomicU64 fields */ }
pub struct StorageTelemetrySnapshot {
    pub appended_records: u64, pub appended_batches: u64, pub fsyncs: u64,
    pub compactions: u64, pub replayed_records_last_open: u64,
    pub snapshot_persists: u64, pub snapshot_loads: u64,
}
```

- `FileRaftLogStore::telemetry() -> StorageTelemetrySnapshot` (shared `Arc<StorageTelemetry>`
  bumped in append/save_vote/rewrite/open).
- `GanglionStateMachine::telemetry()` for snapshot persist/load counts.
- `RaftMetadataNode::telemetry()` aggregates both when durable.
- Tests: counters move under the bounded-recovery test (appends ≥ writes, compactions ≥ 1 after
  purge, snapshot_persists ≥ 1, replay count bounded after restart).

### G2.2 Topology snapshot (feeds fibril CLI + admin diagram)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftTopology {
    pub local_id: u64,
    pub leader: Option<u64>,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub nodes: BTreeMap<u64, String>,        // raft id -> address (BasicNode.addr)
    pub last_applied_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub committed_generation: u64,
}
impl RaftMetadataNode { pub fn topology(&self) -> RaftTopology; }  // from metrics(), sync
```

Serializable on purpose: this exact JSON is what the fibril admin endpoint returns and the CLI
renders. Test: 3-node cluster topology agrees across nodes (same leader/voters) and updates after
membership change.

## Phase G3 — Cluster playground — DONE (iteration 51)

- `crates/ganglion-openraft/examples/cluster_demo.rs` (feature `openraft`): N durable nodes
  (tempdir or `--data-dir`), in-process router, stdin command loop:
  `status` (render `RaftTopology` + committed snapshot), `write <generation>`, `kill <id>`,
  `restart <id>`, `add <id>`, `remove <id>`, `quit`.
- `scripts/cluster-playground.sh`: wrapper (`cargo run -p ganglion-openraft --features openraft
  --example cluster_demo -- "$@"`).
- This is demo code: keep it under ~300 lines, no new dependencies (hand-rolled stdin parsing).
- Smoke-tested by CI-able script flag `--script "write 1; kill 1; status; quit"` (non-interactive
  mode) asserted in scenario 08.

## Phase G4 — fibril-facing follow-through (lives in fibril; listed for sequencing)

See `fibril/REPLICATION_PLANNING.md` § "Ganglion coordination provider — integration plan".
Order: provider contract suite → broker wiring behind config → CLI `topology` → admin diagram.

## Test-suite map (what makes us believe it works)

Layers, bottom-up; every layer already has or gains a named gate:

1. **Storage contracts** — openraft `Suite::test_all` on both log stores (exists).
2. **Model fuzz** — SM running-max model incl. guarded commands (G1); WAL op-interleaving reopen
   model (exists); epoch rule matrix (G1).
3. **Cluster behavior** — election/replication/stale/membership/failover/partition/restart tests
   (exist); controller race test (G1); topology agreement (G2).
4. **Durability** — bounded recovery + atomic persistence tests (exist); telemetry assertions
   (G2); pre-G1 WAL fixture replay (G1).
5. **Scenario scripts** — Jepsen fallback scenarios 01–07 (exist) + 08 playground smoke (G3).
6. **Cross-repo** — fibril provider contract suite + replication choreography (fibril plan).
7. **One-shot gate** — `scripts/validate.sh` all phases (exists; phases get new tests for free).
