# Ganglion Surface Inventory

This is the reverse roadmap for Ganglion: a compact inventory of what is wired,
where it lives, and what is still only planned. Use it when a feature sounds
implemented but you need to know which layer owns it and how complete it is.

## Status Meanings

| Status | Meaning |
| --- | --- |
| Implemented | The main path is wired and has tests, operational surface, or both. |
| Partial | A useful path exists, but important API, docs, tests, or scale work remain. |
| Planned | The design mentions it, but consumers should not rely on it yet. |
| Out of scope | The behavior is intentionally not part of Ganglion's current role. |

## Core Metadata Model

| Item | Status | Implemented surface |
| --- | --- | --- |
| Generic resource identity | Implemented | `ganglion-core::ResourceIdentity` with namespace, name, partition, optional group |
| Node metadata | Implemented | `ganglion-core::NodeInfo` with node id, endpoint, admin endpoint, labels |
| Partition assignment | Implemented | `PartitionAssignment` with owner, followers, epoch, durability |
| Coordination snapshot | Implemented | Nodes, assignments, resources, attributes, generation |
| Resource catalogue | Implemented | `resources: BTreeSet<ResourceIdentity>` plus register/deregister commands |
| Opaque attributes | Implemented | `attributes: BTreeMap<String, String>` plus set/remove/CAS commands |
| Domain-specific schema | Out of scope | Consumers own their documents and validation rules |

Conditions and limits:

- Resource identity is intentionally generic. Consumers map their own concepts into it.
- Attributes are small control documents, not a general data store.
- Snapshot replacement writers must preserve fields they do not own, especially resources,
  attributes, and node labels.

## Assignment and Planning

| Item | Status | Implemented surface |
| --- | --- | --- |
| Pure placement policy trait | Implemented | `PartitionPlacementPolicy` in `ganglion-core` |
| Deterministic placement | Implemented | Preserves live owner and followers where possible |
| Least-loaded placement | Implemented | Selects the least-owned live node for new or displaced resources |
| Strategy selection | Implemented | `PlacementStrategy` parse/list/resolve helpers |
| Epoch stamping | Implemented | `next_assignment_epoch`, `fence_assignment_epoch`, `stamp_assignment_epochs` |
| Local assignment transition planning | Implemented | `plan_local_assignment_transitions` |
| Health-aware placement scoring | Planned | Future scoring may use load, latency, lag, or operator labels |
| Live resource repartitioning | Out of scope | Consumers own partition-count changes and migration semantics |

Conditions and limits:

- Planners are pure and deterministic for a given input.
- Existing live owners are preserved when possible to avoid needless movement.
- Owner changes bump epochs. Follower-only changes do not.
- Tombstone retention for removed and later re-added resources is caller-owned.

## Consensus Runtime

| Item | Status | Implemented surface |
| --- | --- | --- |
| In-memory raft runtime | Implemented | `RaftMetadataNode::start` with `GanglionLogStore` |
| Durable raft runtime | Implemented | `RaftMetadataNode::start_durable` |
| TCP raft runtime | Implemented | `RaftMetadataNode::start_durable_tcp` and format-aware variant |
| Committed snapshot reads | Implemented | `committed_snapshot()` |
| Committed snapshot watch | Implemented | `watch_committed()` |
| Snapshot writes | Implemented | `write_snapshot()` |
| Guarded snapshot writes | Implemented | `write_snapshot_guarded()` with generation CAS |
| Guarded control helper | Implemented | `plan_and_propose_guarded()` |
| Leader-only writes | Implemented | Non-leaders surface `NotLeader` |
| Membership changes | Implemented | `add_learner`, `change_membership` |
| Standalone service wrapper | Planned | Current shape is a library plus examples/scripts |

Conditions and limits:

- Writes are async. Reads and watches are sync-friendly through the committed snapshot.
- CAS checks run inside replicated apply, not before proposal, so races are decided by the
  committed state.
- Durable restart reopens local raft state. Do not reinitialize a node with existing state.

## Merge Commands and Control Documents

| Item | Status | Implemented surface |
| --- | --- | --- |
| Register node | Implemented | `RegisterNode` command and node method |
| Deregister node | Implemented | `DeregisterNode` command and node method |
| Label-only node refresh | Implemented | Heartbeat-like label updates avoid generation bumps and watch wakes |
| Register resource | Implemented | `RegisterResource` command and node method |
| Deregister resource | Implemented | Removes catalogue entry, leaves assignment retirement to controller |
| Set/remove attribute | Implemented | `SetAttribute`, `RemoveAttribute` |
| Compare-and-set attribute | Implemented | `SetAttributeGuarded` with expected value |
| Prefix-level attribute API | Planned | Useful later if attributes become a broader library-facing surface |
| Multi-key transactions | Out of scope | Keep the coordination API small until a real need appears |

Conditions and limits:

- Same-value attribute writes are generation no-ops.
- Label-only node refreshes are intentionally quiet so liveness does not race unrelated CAS writers.
- Consumers own versioning and conflict rules inside attribute values.

## Storage and Recovery

| Item | Status | Implemented surface |
| --- | --- | --- |
| Metadata log abstraction | Implemented | `ganglion-storage::MetadataLog` |
| In-memory metadata log | Implemented | Test and bootstrap paths |
| File metadata log | Implemented | JSON-lines append/replay path |
| Keratin metadata log adapter | Partial | Feature-gated lower-level metadata log backend |
| Durable raft WAL | Implemented | `FileRaftLogStore` |
| Persistent state machine snapshot | Implemented | Atomic snapshot writes and restore on open |
| Bounded raft recovery | Implemented | Snapshot threshold and retained WAL tail constants |
| Strict raft WAL replay | Implemented | Corrupt local raft state fails loudly |
| Automatic peer resync after corrupt local raft state | Planned | Current runbook is wipe local coordination dir and rejoin |

Conditions and limits:

- Raft durability uses strict local replay because silently truncating committed raft history is unsafe.
- State-machine snapshots are persisted atomically with tmp, fsync, rename, and parent-directory fsync.
- Storage telemetry is plain atomics, not tied to a metrics crate.

## Transport and Topology

| Item | Status | Implemented surface |
| --- | --- | --- |
| In-process network | Implemented | `InProcessRouter` for tests and playgrounds |
| TCP network | Implemented | `TcpRaftServer`, `TcpNetworkFactory`, `TcpRaftConnection` |
| MessagePack wire format | Implemented | Default TCP frame body format |
| JSON wire format | Implemented | Debug-friendly alternative, receivers accept both formats |
| Lazy reconnect | Implemented | TCP client connections reconnect on RPC failures |
| Topology snapshot | Implemented | `RaftTopology` with leader, voters, learners, addresses, applied index |
| Listener liveness | Implemented | `TcpRaftServer::is_serving()` |
| TLS/auth on raft transport | Planned | Current trust model assumes a trusted private network |
| Public internet exposure | Out of scope | Coordination transport should not be exposed directly |

Conditions and limits:

- TCP frames are length-prefixed and capped.
- Peer addresses come from raft membership, not a separate static peer table.
- Topology is a per-node observation. Disagreement is diagnostic data.

## Coordination Provider Layer

| Item | Status | Implemented surface |
| --- | --- | --- |
| Provider trait | Implemented | `ganglion-coordination::CoordinationProvider` |
| Static provider | Implemented | Immutable snapshot provider for tests/bootstrap |
| In-memory provider | Implemented | Mutable watch-backed provider |
| Owned/followed helpers | Implemented | Resource filtering helpers |
| Full raft provider wrapper | Partial | Downstream Fibril adapter currently owns more wrapper logic than ideal |
| Generic provider utilities | Planned | Heartbeats, catalogue sync, guarded attribute publish, controller loop helpers |

Conditions and limits:

- The provider crate is intentionally small.
- Domain-free wrapper logic should move here when the boundary is clear.
- Consumer-specific mapping belongs in the consumer adapter.

## Validation and Failure Coverage

| Item | Status | Implemented surface |
| --- | --- | --- |
| OpenRaft storage contract suite | Implemented | In-memory and file-backed raft storage paths |
| Property tests | Implemented | Planner, epoch, storage, and state-machine model checks |
| Jepsen-style fallback scripts | Implemented | Scenario scripts under `tests/jepsen` |
| Cluster playground | Implemented | `scripts/cluster-playground.sh` and `cluster_demo` example |
| Failure-mode catalogue | Implemented | `FAILURE_MODES.md` |
| Leader crash and follower catch-up | Implemented | Runtime tests and scenario coverage |
| Corrupt snapshot and WAL behavior | Implemented | Startup failure and runbook documented |
| Asymmetric partition chaos | Planned | Identified in failure modes, not fully covered |
| Leader-on-minority end-to-end scenario | Planned | Identified in failure modes, not fully covered |

Conditions and limits:

- `scripts/validate.sh` is the broad check entrypoint.
- Some slow cluster tests are intentionally heavier because they cover recovery and snapshot transfer.
- Failure coverage should state whether a behavior is tested, reasoned about, or only documented.

## Consumer Integration Boundary

| Item | Status | Implemented surface |
| --- | --- | --- |
| Fibril resource mapping | Implemented | Downstream adapter maps queue identity to `ResourceIdentity` |
| Fibril runtime attributes | Implemented | Downstream adapter stores runtime settings as opaque attributes |
| Fibril client topology | Consumer-owned | Ganglion exposes generic topology and assignments, Fibril shapes client output |
| Fibril data-plane role transitions | Consumer-owned | Stroma/Keratin enforce queue roles and epochs |
| Consumer-specific routing | Consumer-owned | Ganglion does not route application traffic |
| Generic extraction pass | Planned | Move domain-free downstream coordination helpers into Ganglion on next touch |

Conditions and limits:

- Ganglion should not learn Fibril topics, groups, broker loops, message logs, or DLQ rules.
- Fibril should not duplicate generic coordination behavior once the boundary is stable.
- A future database or clustered service should be able to reuse Ganglion without inheriting queue semantics.

