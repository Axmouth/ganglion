# Ganglion

**A tiny nervous system for clustered software.**

Ganglion is an embeddable coordination library for software that needs a compact,
replicated view of ownership, liveness, and control-plane metadata. It is built
for projects that already own their domain model and need a reliable substrate
for deciding who owns what, who is alive, and which small pieces of metadata are
currently committed.

Ganglion is not trying to be a general database. Its useful shape is narrower:

- store a committed coordination snapshot
- publish that snapshot to watchers
- register nodes and resources
- assign resources to owners and followers
- fence ownership changes with epochs
- store small opaque attributes with guarded updates
- expose topology and durability telemetry for operators

That scope keeps the library small enough to embed while still covering the
coordination patterns needed by clustered services.

## Current Status

Ganglion is pre-release software. The main implementation pieces exist, but the
public API and packaging are still being shaped.

Implemented today:

- generic resource identities and partition assignments in `ganglion-core`
- deterministic and least-loaded placement strategies
- epoch helpers for ownership fencing
- a replicated coordination snapshot with resources and opaque attributes
- OpenRaft-backed metadata nodes behind the `openraft` feature
- durable WAL and persisted state-machine snapshots
- committed snapshot watches for consumers
- guarded snapshot writes and guarded attribute writes
- membership changes with learner promotion
- TCP transport for real multi-process clusters
- topology and storage telemetry surfaces
- failure-mode notes and scenario scripts

Still settling:

- the final stable crate-level API
- documentation for operators and library consumers
- which generic coordination helpers should move here from downstream projects
- larger-cluster liveness handling without writing every heartbeat through Raft
- finer-grained assignment update commands for large resource sets
- package-level examples and release hygiene

## Crates

- `ganglion-core` contains the domain-neutral metadata model, placement input,
  placement policies, durability policy, and epoch helpers.
- `ganglion-storage` contains metadata log abstractions and file-backed durable
  storage.
- `ganglion-openraft` contains the OpenRaft runtime, durable state machine, TCP
  transport, guarded writes, topology, and telemetry.
- `ganglion-coordination` contains small provider traits and in-memory/static
  coordination providers used by tests and bootstrap flows.

## Design Principles

- Keep Ganglion neutral. Queue, broker, database, or application-specific logic
  belongs in the consumer unless it is useful as a generic coordination tool.
- Prefer snapshot watches for consumers. Consumers should react to committed
  metadata instead of hosting consensus themselves.
- Make ownership changes explicit. Epochs are part of the metadata contract so
  stale owners can be fenced by the system using Ganglion.
- Keep liveness separate from durable topology where possible. Heartbeats should
  not create needless generation churn.
- Make planners pure and testable. Placement policy should be deterministic for
  a given input.
- Keep small opaque attributes available for consumer-owned settings and control
  documents, without teaching Ganglion their meaning.

## Quick Start

Validate the workspace:

```bash
cargo test --workspace
```

Run the broader validation script:

```bash
scripts/validate.sh
```

Run the cluster playground:

```bash
scripts/cluster-playground.sh --script "status; write 1; status; quit"
```

For basic usage examples, see [EXAMPLES.md](EXAMPLES.md). For the current public
surface, see [API.md](API.md). For the reverse roadmap of implemented behavior,
see [SURFACE_INVENTORY.md](SURFACE_INVENTORY.md). For the active design and
roadmap, see [PLAN.md](PLAN.md). For failure behavior and runbook notes, see
[FAILURE_MODES.md](FAILURE_MODES.md).

## Relationship To Consumers

Fibril is the first serious consumer. It uses Ganglion for cluster metadata,
partition ownership, resource catalogue state, runtime attributes, topology, and
controller coordination.

That does not make Ganglion Fibril-specific. When coordination code is generic,
it should move into Ganglion. When logic depends on Fibril concepts such as
queues, topics, groups, message logs, or broker runtime behavior, it should stay
in Fibril and adapt to Ganglion's neutral model.

Good candidates to keep or move into Ganglion over time:

- generic heartbeat and liveness helpers
- guarded attribute publish helpers
- controller-loop helpers built from read, plan, compare, and retry
- catalogue synchronization primitives
- progress-label patterns that do not assume a message queue

Good candidates to keep in consumers:

- domain validation rules
- routing protocols
- data-plane promotion and demotion mechanics
- application-specific settings documents
- queue or database recovery semantics
