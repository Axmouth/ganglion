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
