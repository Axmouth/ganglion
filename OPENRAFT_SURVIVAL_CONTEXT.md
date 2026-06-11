# OpenRaft Survival Context (v0.8.9)

This file is the continuation anchor after context compaction.
OpenRaft target is in `crates/ganglion-openraft/Cargo.toml` and currently pinned to `openraft = "0.8.9"`.
Reference example app: `~/code/temp/openraft/examples/raft-kv-memstore`.

## Source files to reopen first

- `~/.cargo/registry/src/*/openraft-0.8.9/src/raft.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/storage/mod.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/storage/v2.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/network/network.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/network/factory.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/raft_types.rs`

## Needed API surface (non-exhaustive)

### Type config

- `RaftTypeConfig` (`src/raft.rs`):
  - `type D: AppData`
  - `type R: AppDataResponse`
  - `type NodeId: NodeId`
  - `type Node: Node`
  - `type Entry: RaftEntry<Self::NodeId, Self::Node> + FromAppData<Self::D>`
  - `type SnapshotData: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static`

### Storage surface

- `RaftLogReader<C>`:
  - `get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>`
  - `try_get_log_entries(range)`
- `RaftLogStorage<C>`:
  - `get_log_reader()`
  - `save_vote(&mut self, vote)`
  - `read_vote(&mut self)`
  - `append(entries, callback)` (durability callback)
  - `truncate(log_id)`
  - `purge(log_id)`
- `RaftStateMachine<C>`:
  - `applied_state()`
  - `apply(entries)`
  - `get_snapshot_builder()`
  - `begin_receiving_snapshot()`
  - `install_snapshot(meta, snapshot)`
  - `get_current_snapshot()`

### Network surface

- `RaftNetwork<C>`:
  - `append_entries(rpc, option)`
  - `vote(rpc, option)`
  - `install_snapshot(rpc, option)`
  - `backoff()`
- `RaftNetworkFactory<C>`:
  - `type Network: RaftNetwork<C>`
  - `new_client(target, node) -> Network`

### Runtime/control surface (`Raft`)

- `Raft::new(id, config, network, log_store, state_machine)`
- `Raft::append_entries(...)`
- `Raft::vote(...)`
- `Raft::install_snapshot(...)`
- `Raft::initialize(members)`
- `Raft::client_write(app_data)`
- `Raft::add_learner(id, node, blocking)`
- `Raft::change_membership(members, retain)`
- `Raft::current_leader()`
- `Raft::is_leader()`
- `Raft::metrics()`
- `Raft::shutdown().await`

## Hard rules to preserve

- `append` and `save_vote` are durability-sensitive (must persist before the relevant completion).
- Log index continuity is strict; no holes.
- `truncate` and `purge` must not leave holes.
- For non-test flows, close raft handles with `shutdown().await`.

## Ganglion bridge notes

- `openraft::NodeId` in this stack is typically numeric (`u64` in examples), while existing ganglion node ids are string-based.
- `CoordinationSnapshot` is already serde-serializable in `ganglion-core`.
- Existing local adapter in `ganglion-openraft` is still not a real Raft runtime yet; this doc is specifically to keep the required OpenRaft contract pinned while continuing implementation.

