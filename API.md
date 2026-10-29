# Ganglion API

This file tracks the current public surface, the intended role of each crate, and what is still
partial, planned or out of scope. It is mutable while Ganglion is pre-release. Code wins when this
file and the implementation disagree.

Status words used in the "Status and limits" tables:

| Status | Meaning |
| --- | --- |
| Partial | A useful path exists, but important API, docs, tests or scale work remain. |
| Planned | Mentioned in the plan, but consumers should not rely on it yet. |
| Out of scope | Intentionally not part of Ganglion's current role. |
| Consumer-owned | Left to the consuming system on purpose. |

Everything listed outside those tables is implemented.

## Umbrella crate (`ganglion`)

- Re-exports `ganglion-openraft` with the `openraft` feature enabled, so consumers depend on
  `ganglion` and treat the consensus backend as an internal detail.

## Core (`ganglion-core`)

- `ResourceIdentity`
  - Generic identity for any sharded resource (`namespace`, `name`, `partition`, optional `group`).
  - `new(namespace, name, partition, group)` and `with_namespace_part(namespace)`.
  - Used by planners and coordinators without queue-specific assumptions.

- `NodeInfo`
  - Control-plane metadata for a node: node id, data endpoint, optional admin endpoint, labels.
  - `NodeInfo::new(node_id, endpoint, admin_endpoint)` starts with empty labels.

- `PartitionAssignment`
  - Describes owner, followers, epoch, and durability policy for one resource.
  - Helpers: `is_owned_by`, `is_followed_by`, `replica_set_size`, `durability_requirement`.

- `CoordinationSnapshot`
  - Snapshot of all known nodes and assignments with a monotonic `generation`.
  - Also carries `resources: BTreeSet<ResourceIdentity>` (cluster catalogue) and
    `attributes: BTreeMap<String, String>` (opaque replicated settings). Both are serde-default,
    so older WALs and snapshots load unchanged. Snapshot-replace writers must preserve them.
  - Assignments serialize as a sequence of `(resource, assignment)` pairs because JSON map keys
    must be strings.
  - Helpers: `assignment_for(resource)`, `owned_by(node_id)`, `followed_by(node_id)`.

- `PlacementInput` / `PlacementPlan`
  - Planner input with live node set, desired resources, existing assignments, follower target,
    and generation. The plan is the planner output.

- `PartitionPlacementPolicy`
  - Trait for pluggable placement strategy.
  - Pure function: `(PlacementInput) -> PlacementPlan`. Failures use `PlacementError`.

- `PlacementStrategy`
  - Pluggable strategy key used by consumers to select behavior.
  - Built-ins:
    - `deterministic`: preserves the existing owner when possible.
    - `least-loaded`: spreads owner assignments across live nodes by current ownership load.
  - Helpers: `as_str()`, `parse(&str)`, `all()`, `as_strategy()`.

- `DeterministicPartitionPlacement`
  - Reuses the existing owner if alive, preserves follower order where possible, and fills
    missing follower slots predictably.

- `LeastLoadedPartitionPlacement`
  - Preserves the existing assignment when possible, otherwise picks the least-owned live node.

- `local_assignment_role(snapshot, node_id, resource) -> LocalRole`
  - `Owner`, `Follower` or `None` for one node and resource.

- `plan_local_assignment_transitions(node_id, previous, next) -> Vec<LocalTransition>`
  - Per-node role transitions between two snapshots: `KeepOwner`, `KeepFollower`,
    `PromoteFollowerToOwner`, `DemoteOwnerToFollower`, `StopServing`.
  - For components that need to demote, pause or promote work safely.

- `ReplicationDurabilityPolicy` / `ReplicationDurabilityRequirement`
  - Generic durability contracts for metadata write acceptance, with
    `ReplicationAcknowledgement` and `ReplicationDurabilityError`. No transport coupling.

### Epoch and fencing helpers

- `next_assignment_epoch(committed, desired_owner) -> (u64, EpochTransition)`
  - Owner change bumps. Follower churn holds. New assignments start at 1.
- `fence_assignment_epoch(committed) -> u64`
  - Operator fence without an ownership change.
- `stamp_assignment_epochs(committed, &mut desired) -> Vec<(ResourceIdentity, EpochTransition)>`
  - Applies the rule across a planned snapshot. The caller owns tombstone retention if resources
    can be removed and re-added.

### Status and limits

| Item | Status | Notes |
| --- | --- | --- |
| Domain-specific schema | Out of scope | Consumers own their documents and validation rules |
| Health-aware placement scoring | Planned | Scoring may later use load, latency, lag or operator labels |
| Live resource repartitioning | Out of scope | Consumers own partition-count changes and migration semantics |

- Resource identity is intentionally generic. Consumers map their own concepts into it.
- Attributes are small control documents, not a general data store.
- Snapshot replacement writers must preserve fields they do not own, especially resources,
  attributes and node labels.
- Planners are pure and deterministic for a given input. Existing live owners are preserved when
  possible to avoid needless movement.
- Owner changes bump epochs. Follower-only changes do not.
- Tombstone retention for removed and later re-added resources is caller-owned.

## Openraft adapter crate (`ganglion-openraft`)

- `MetadataConsensus`
  - Minimal sync control-plane trait for the in-memory and persisted nodes below.
  - Supports local role checks, current leader read, and snapshot apply and read.

- `InMemoryMetadataNode` (built directly or with `InMemoryMetadataNodeBuilder`)
  - Local, in-memory implementation of `MetadataConsensus` used by tests.
  - Enforces raft-like write invariants in memory:
    - the local leader must propose writes,
    - stale term and stale generation updates are rejected,
    - term changes can invalidate prior in-memory history.
  - Debug helpers: `current_term()`, `log_len()`, `last_index()`, `last_term()`.
  - Leadership is set by hand with `set_leader_term`, `set_leader` and `clear_leader`.

- `OpenraftAdapterError`
  - Shared error surface: `NotLeader`, `StaleGeneration`, `GenerationMismatch { expected, actual }`,
    `AttributeMismatch { key, actual }`, `PoisonedState`, `StaleTerm`, `Planner(PlacementError)`,
    `Config(String)` (including invalid profile parsing and listener bind failures) and
    `Storage(String)`.

- `plan_and_apply`
  - Runs a planner and applies the resulting snapshot in one call.

- `plan_and_publish`
  - Runs a planner and consensus apply, then publishes the committed snapshot through a callback.

- `PersistedMetadataNode`
  - File-backed `MetadataConsensus` node using durable logs from `ganglion-storage`.
  - Restores current term and latest committed snapshot from disk during construction.
  - Constructors:
    - `new` uses bounded-tail replay by default.
    - `new_strict` keeps strict validation on startup.
    - `new_with_tail_replay_limit` sets an explicit tolerated tail.
    - `new_with_replay_profile` selects behavior from a `PersistedMetadataReplayProfile`
      (`Strict`, `Default` which is `TruncateTail` with one line tolerated, or
      `TruncateTail { max_tail_lines }`).
    - `new_with_replay_profile_resolution` takes an already resolved profile.
    - `new_with_profile_env` and `new_from_env` read `GANGLION_PERSISTED_REPLAY_PROFILE` when set.
    - `new_with_replay_profile_str` takes an optional raw profile string and returns the node
      plus the `PersistedMetadataReplayProfileResolution` it used.
    - `new_with_replay_policy` takes a `FileMetadataReplayPolicy` directly.
    - `new_with_log` and `new_with_log_and_profile` inject any `MetadataLog` implementation, for
      constructor tests and backend parity runs.
  - `startup_replay_profile()` and `startup_replay_policy()` expose the resolved startup choice.
- `PersistedMetadataReplayProfileResolution` / `PersistedMetadataReplayProfileSource`
  - Structured startup-profile decision: `profile` plus `source` (`Explicit`, `Environment` or
    `Default`).
- `PersistedMetadataReplayProfile` parses from `strict`, `default` / `resilient`,
  `tail:<n>` / `truncate_tail:<n>` / `<n>`.

### Openraft runtime (feature `openraft`)

Runs on the vendored openraft 0.9.25. The `openraft` crate is re-exported so consumers can name
`BasicNode`, `Config` and storage traits without their own version-matched dependency.

- `GanglionRaftConfig`
  - `openraft::RaftTypeConfig` for the metadata raft group
    (`NodeId = u64`, `Node = BasicNode`, `SnapshotData = Cursor<Vec<u8>>`).
- `GanglionRaft`
  - Alias for `Raft<GanglionRaftConfig>`.
- `MetadataRaftCommand`
  - Snapshot replace: `ApplySnapshot(CoordinationSnapshot)` and
    `ApplySnapshotGuarded { expected_generation, snapshot }` (generation CAS, checked inside the
    replicated apply so races are decided by committed state).
  - `UpdatePartitionGuarded { expected_generation, assignment, attributes }` publishes one
    partition and consumer-owned attributes atomically while keeping concurrent node-label
    updates.
  - Merge commands that cannot clobber concurrent updates:
    - `RegisterNode` / `DeregisterNode`. A label-only refresh of a known node (same endpoint and
      admin endpoint) does not bump the generation or wake watchers.
    - `RegisterResource` / `DeregisterResource`. Deregistering leaves any assignment in place,
      retiring placements is the controller's job.
    - `RegisterResourceWithInitialAttribute { resource, key, value }` seeds an attribute only for
      a resource absent from both catalogue and assignments.
    - `DeregisterResourceWithAttribute { resource, key, expected }` removes a catalogue entry and
      its attribute only if the attribute still matches.
    - `DeregisterResourceReplacingAttribute { resource, key, expected, value }` removes a
      catalogue entry and swaps its observed attribute for a retirement marker.
    - `SetAttribute` / `RemoveAttribute`. Same-value sets are generation no-ops.
    - `CompareAndSetAttribute { key, expected, value }` (`expected: None` means absent).
    - `CompareAndSetAttributeGuarded { expected_generation, key, expected, value }`.
- `MetadataRaftResponse` / `MetadataRejection`
  - Response: `accepted: bool`, `rejection: Option<MetadataRejection>` and the post-apply
    committed `snapshot`.
  - Rejections: `StaleGeneration`, `GenerationMismatch { expected, actual }`,
    `AttributeMismatch { key, actual }`.
- `GanglionLogStore`
  - In-memory `RaftLogReader` + `RaftLogStorage`. Passes `openraft::testing::Suite`.
- `FileRaftLogStore`
  - Durable WAL-backed `RaftLogStorage` (JSON lines: vote, entry, truncate and purge records).
    Strict replay on open, fsynced appends, purge-time compaction. Writes and syncs on its own
    thread. Also passes the contract suite.
  - `FileRaftLogStore::open(path)`, `telemetry()`, `telemetry_handle()`.
- `GanglionStateMachine`
  - `RaftStateMachine` + `RaftSnapshotBuilder` holding the committed `CoordinationSnapshot`.
  - `GanglionStateMachine::persistent(path)` also writes snapshots to disk atomically (tmp,
    fsync, rename, parent-dir fsync) and restores them on open. This lets state survive log purges
    across full restarts and bounds recovery time.
  - Reads: `committed_snapshot()`, `watch_committed()`, `last_applied()`, and the closure reads
    `read_committed` and `read_committed_at` described under `RaftMetadataNode`.
  - `telemetry()` and `telemetry_handle()`.
- `InProcessRouter` / `InProcessConnection`
  - `RaftNetworkFactory` / `RaftNetwork` routing RPCs between same-process raft handles.
    `register(id, raft)` and `deregister(id)`, the latter simulates an unreachable peer.

#### `RaftMetadataNode`

One raft-backed metadata node. The log store and network are chosen per constructor, so the node
type carries no storage or network generics.

- Startup:
  - `start(id, config, &router)`: in-memory log over an `InProcessRouter`.
  - `start_with_store(id, config, &router, log_store)` and
    `start_with_storage(id, config, &router, log_store, state_machine)`.
  - `start_with_network(id, config, network, log_store, state_machine)`: any
    `RaftNetworkFactory`.
  - `start_durable(id, config, &router, dir)`: WAL plus persisted snapshot under `dir`, with
    bounded restart recovery.
  - `start_durable_tcp(id, config, listen_addr, dir) -> (RaftMetadataNode, TcpRaftServer)`: real
    multi-process clusters. Peers are dialed from membership addresses.
  - `start_durable_tcp_with_format(id, config, listen_addr, dir, wire_format)`: explicit wire
    format for outbound frames and replies.
  - `start_durable_tcp_with_transport(id, config, listen_addr, dir, wire_format, dialer, acceptor)`
    where `D: RaftDialer, A: RaftAcceptor`: injects both transport seams, for example to run the
    raft channel over TLS from the caller's own material. Ganglion takes no TLS dependency.
  - A listener bind failure surfaces as `OpenraftAdapterError::Config`.
  - `initialize(members)`: call once, on one node, with blank state. Durable restart reopens the
    WAL and starts again without re-initializing.
- Writes (leader-only, non-leaders get `NotLeader`):
  - `write_snapshot(snapshot)`. A rejected stale commit surfaces as `StaleGeneration`.
  - `write_snapshot_guarded(expected_generation, snapshot)`. A lost race surfaces as
    `GenerationMismatch`.
  - `plan_and_propose_guarded(plan, max_retries)`: read committed state, run the pure `plan`,
    bump the generation, stamp epochs, propose guarded, and retry on mismatch. The race-safe
    controller-loop primitive.
  - Merge wrappers: `register_node`, `deregister_node`, `register_resource`,
    `deregister_resource`, `set_attribute`, `remove_attribute`,
    `compare_and_set_attribute(key, expected, value)` (lost race surfaces as
    `AttributeMismatch`).
  - `submit_merge(command: MetadataRaftCommand)`: submit any command directly, for provider
    layers that already hold one.
- Reads:
  - `committed_snapshot()`: clone of the committed snapshot on this node.
  - `watch_committed() -> watch::Receiver<CoordinationSnapshot>`: wakes on accepted state changes.
  - `read_committed(|snapshot| ...) -> R`: inspect committed state without cloning it. The
    closure holds the state-machine lock, so it must stay short, must not reenter state-machine
    methods, and cannot return borrowed data.
  - `read_committed_at(|last_applied: Option<LogId<NodeId>>, snapshot| ...) -> R`: same, plus the
    last applied log id. Every committed change moves that id, so it can key derived values.
  - `ensure_linearizable() -> Result<(), OpenraftAdapterError>`: returns once this node has
    confirmed with a quorum that it is still leader. The read barrier for read-after-write
    correctness.
- Leadership:
  - `current_leader()` and `is_leader()` (local metrics view, async).
  - `watch_leader() -> watch::Receiver<Option<NodeId>>`: this node's view of the leader, woken
    only when it changes. The forwarding task ends when the receiver is dropped.
  - `wait_for_leader(leader, timeout)`, `wait_for_any_leader(timeout)`,
    `wait_for_applied_index(index, timeout)`: test and bootstrap helpers.
- Membership (leader-only):
  - `add_learner(id, node, blocking)`: replication without a vote. `blocking` waits for catch-up.
  - `change_membership(voters, retain)`: replaces the voter set. `retain` keeps demoted voters
    as learners.
- Peer transport observations:
  - `peer_transport_failures() -> watch::Receiver<BTreeMap<u64, PeerTransportFailure>>`: explicit
    local connection failures per peer (`since`, `latest`, `attempts`, `kind`). A successful RPC
    clears the peer's entry. In-process transports report nothing. These are suspicion signals
    only and grant no raft or application authority.
  - `set_monitor_idle_peer_connections(enabled)`: also watch idle connections so an explicit
    close is noticed before the next RPC. Off by default.
- Inspection:
  - `node_id()`, `topology() -> RaftTopology`, `telemetry() -> StorageTelemetrySnapshot`,
    `raft()` for the raw handle, `shutdown()`.

#### Supporting runtime types

- `default_raft_config()`
  - Validated `openraft::Config` for the metadata workload: a snapshot every
    `SNAPSHOT_LOGS_SINCE_LAST` (256) entries, with `MAX_IN_SNAPSHOT_LOG_TO_KEEP` (64) entries kept
    after a purge. This bounds WAL size and startup replay.
- `StorageTelemetry` / `StorageTelemetrySnapshot`
  - Plain atomic durability counters (appended records and batches, fsyncs, compactions, records
    replayed on the last open, snapshot persists and loads). Aggregated by
    `RaftMetadataNode::telemetry()`. No metrics-crate dependency.
- `RaftTopology`
  - Serializable per-node view of the raft group: `local_id`, `leader`, `voters`, `learners`,
    raft id to address map, applied and snapshot indexes, committed generation. This JSON is the
    contract for topology CLIs and admin views.

#### TCP transport

- Wire frames: `[1-byte format tag][u32 BE length][body]`, capped at 64 MiB. The body is
  MessagePack (default) or JSON. `WireFormat` parses `msgpack`, `messagepack` or `json`
  (`WireFormatParseError` otherwise). Callers pass the format from their own startup
  configuration, the library reads no environment variable for it. Receivers accept both
  formats, so mixed-format clusters interoperate.
- `TcpRaftServer`
  - `bind(listen_addr, raft, format)` and
    `bind_with_acceptor(listen_addr, raft, format, acceptor: impl RaftAcceptor)`, which wraps
    each accepted connection (for example a TLS handshake) before serving it. A failed wrap drops
    the connection.
  - `local_addr()`, `shutdown()`, and `is_serving()` for listener liveness. Dropping the handle
    stops the listener.
- `serve_connection(stream, raft, format)`
  - Serves raft RPCs on one already accepted stream of any tokio IO type, for simulated
    transports with their own accept loop.
- `RaftDialer` (trait)
  - `type Stream` plus `dial(&self, addr: &str) -> impl Future<Output = io::Result<Self::Stream>>`.
    Production uses `TokioDialer` (real TCP with nodelay). Tests can inject a simulated network.
- `RaftAcceptor` (trait)
  - `type Stream` plus `accept(&self, stream: TcpStream) -> impl Future<Output = io::Result<Self::Stream>>`.
    Production uses `PlainAcceptor`, which passes the stream through.
- `DialerNetworkFactory<D>` / `DialerRaftConnection<D>`
  - Network factory and lazy, reconnecting connection over any `RaftDialer`.
    `with_dialer(dialer)` and `with_dialer_format(dialer, format)`. Peer addresses come from
    membership (`BasicNode.addr`). Failures surface as `Unreachable` so raft backs off.
  - `TcpNetworkFactory` and `TcpRaftConnection` are the `TokioDialer` aliases.
    `TcpNetworkFactory::new()` and `with_format(format)`.
- Forwarded writes
  - `client_write_remote(addr, command, format) -> Result<MetadataRaftResponse, OpenraftAdapterError>`:
    sends one command to the raft node at `addr`. A non-leader answer maps to `NotLeader`.
  - `client_write_remote_with_hint(addr, command, format) -> Result<MetadataRaftResponse, RemoteWriteError>`:
    same, but `RemoteWriteError::NotLeader { leader_addr }` carries the leader hint for a retry.

### Status and limits

| Item | Status | Notes |
| --- | --- | --- |
| Standalone service wrapper | Planned | Today Ganglion is a library plus examples and scripts |
| Prefix-level attribute API | Planned | Useful if attributes become a broader library-facing surface |
| Multi-key transactions | Out of scope | Keep the coordination API small until a real need appears |
| Automatic peer resync after corrupt local raft state | Planned | Runbook today: wipe the local coordination dir and rejoin |
| Built-in TLS or auth on the raft transport | Out of scope | The dialer and acceptor seams let callers add TLS. Without them the transport assumes a trusted private network |
| Public internet exposure | Out of scope | The coordination transport should not be exposed directly |

- Writes are async. Reads and watches are sync-friendly through the committed snapshot.
- CAS checks run inside replicated apply, not before proposal, so races are decided by the
  committed state.
- Durable restart reopens local raft state. Do not reinitialize a node with existing state.
- Raft durability uses strict local replay because silently truncating committed raft history
  is unsafe.
- Same-value attribute writes are generation no-ops. Label-only node refreshes stay quiet so
  liveness does not race unrelated CAS writers. Consumers own versioning and conflict rules
  inside attribute values.
- Peer addresses come from raft membership, not a separate static peer table.
- Topology is a per-node observation. Disagreement between nodes is diagnostic data.

## Storage crate (`ganglion-storage`)

- `MetadataLog`
  - Persistence abstraction: `append_entry`, `latest_entry`, `entries`, `clear`, `truncate_from`.
- `MetadataLogEntry`
  - Term, index and snapshot records stored by durable implementations.
- `MetadataLogError`
  - `Io(String)` or `Parse(String)`.
- `InMemoryMetadataLog`
  - In-memory implementation used by tests.
- `FileMetadataLog`
  - Append-only file log of newline-delimited JSON entries. `new(path)` and
    `with_replay_policy(path, policy)`.
  - Replay validates index continuity and rejects malformed or invalid entries by default.
- `FileMetadataReplayPolicy`
  - `Strict` (default) fails hard. `TruncateTail { max_tail_lines }` recovers from bounded
    trailing corruption.
- `KeratinMetadataLog` (feature `keratin`)
  - Durable metadata backend powered by `keratin-log`, with the same append, read, clear and
    truncate contract and replay-policy behavior as the file log.

These logs back `PersistedMetadataNode`. The openraft runtime uses `FileRaftLogStore` instead.

### Status and limits

| Item | Status | Notes |
| --- | --- | --- |
| Keratin metadata log adapter | Partial | Feature-gated lower-level backend, not the default cluster metadata path |

- State-machine snapshots are persisted atomically with tmp, fsync, rename and parent-directory
  fsync.
- Storage telemetry is plain atomics, not tied to a metrics crate.

## Coordination crate (`ganglion-coordination`)

- `CoordinationProvider`
  - Read and watch abstraction for snapshot consumers: `snapshot()`, `owns_resource(...)`,
    `follows_resource(...)`, `watch()`.
- `SnapshotWatch`
  - Alias for `watch::Receiver<CoordinationSnapshot>`.
- `InMemoryCoordination`
  - Mutable provider backed by `tokio::sync::watch`. `new(initial)`, `update_snapshot(next)`,
    `set_generation(generation)`.
- `StaticCoordination`
  - Immutable fixture provider for tests and bootstrap deployments.
- Helpers
  - `owned_resources(...)`, `followed_resources(...)`, `owned_by_snapshot(...)`.

### Status and limits

| Item | Status | Notes |
| --- | --- | --- |
| Full raft provider wrapper | Partial | The downstream Fibril adapter owns more wrapper logic than ideal |
| Generic provider utilities | Planned | Heartbeats, catalogue sync, guarded attribute publish, controller loop helpers |

- The provider crate is intentionally small.
- Domain-free wrapper logic should move here when the boundary is clear.
- Consumer-specific mapping belongs in the consumer adapter.

## Validation and consumer boundary

Validation entry points are `scripts/validate.sh`, the Jepsen-style fallback scenarios in
[tests/jepsen](tests/jepsen/README.md), the cluster playground (`scripts/cluster-playground.sh`)
and the failure catalogue in [FAILURE_MODES.md](FAILURE_MODES.md).

| Item | Status | Notes |
| --- | --- | --- |
| Asymmetric partition chaos | Planned | Identified in failure modes, not covered |
| Leader-on-minority end-to-end scenario | Planned | Identified in failure modes, not covered |
| Fibril client topology | Consumer-owned | Ganglion exposes generic topology and assignments, Fibril shapes client output |
| Fibril data-plane role transitions | Consumer-owned | Stroma and Keratin enforce queue roles and epochs |
| Consumer-specific routing | Consumer-owned | Ganglion does not route application traffic |
| Generic extraction pass | Planned | Move domain-free downstream coordination helpers into Ganglion on next touch |

- Some slow cluster tests are heavier on purpose because they cover recovery and snapshot
  transfer.
- Failure coverage should state whether a behavior is tested, reasoned about, or only documented.
- Ganglion should not learn Fibril topics, groups, broker loops, message logs or DLQ rules.
- Fibril should not duplicate generic coordination behavior once the boundary is stable.
- A future database or clustered service should be able to reuse Ganglion without inheriting
  queue semantics.
