# OpenRaft 0.8.9 Survival Sheet

Open this file first after context loss. Then open only the listed source files.

## Signature anchors

- `.../openraft-0.8.9/src/raft.rs`
- `.../openraft-0.8.9/src/storage/v2.rs`
- `.../openraft-0.8.9/src/storage/mod.rs`
- `.../openraft-0.8.9/src/network/network.rs`
- `.../openraft-0.8.9/src/network/factory.rs`
- `.../openraft-0.8.9/src/testing/suite.rs`
- `.../openraft-0.8.9/src/node.rs`
- `.../openraft-0.8.9/src/docs/getting_started/getting-started.md` (design notes only)

Keep `~/code/temp/openraft` as reference for ideas, not ABI/signature source of truth (`0.10.0-alpha.21`).

## Required type surface

```rust
impl openraft::RaftTypeConfig for TypeConfig {
    type D: AppData;
    type R: AppDataResponse;
    type NodeId: NodeId;
    type Node: Node; // usually `BasicNode { addr: String }`
    type Entry: RaftEntry<Self::NodeId, Self::Node> + FromAppData<Self::D>;
    type SnapshotData: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static;
}
```

Use `openraft::declare_raft_types!(pub TypeConfig: D = ..., R = ..., NodeId = ..., Node = ..., Entry = ..., SnapshotData = ...);` if available.

## API you must keep stable in `ganglion-openraft`

### Raft API

From `raft.rs`:
- `Raft::new(id, config, network_factory, log_store, state_machine).await`
- `initialize(members)`
- `client_write(app_data)`
- `append_entries(rpc)`
- `vote(rpc)`
- `install_snapshot(rpc)`
- `add_learner(id, node, blocking)`
- `change_membership(members, retain)`
- `current_leader() -> Option<NodeId>`
- `is_leader()`
- `shutdown().await`

`Raft` is `Clone` in spirit through `Arc` internals; operations return `RaftError`/`Raft::ClientWriteResponse` style results.

### Storage v2

From `storage/v2.rs` and `storage/mod.rs`:

`RaftLogReader`:
- `get_log_state(&mut self) -> Result<LogState<C>, StorageError<NodeId>>`
- `try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(&mut self, range: RB)`

`RaftLogStorage`:
- `type LogReader: RaftLogReader<C>`
- `get_log_reader(&mut self) -> Self::LogReader`
- `save_vote(&mut self, vote: &Vote<NodeId>)`
- `read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>>`
- `append<I>(&mut self, entries: I, callback: LogFlushed<NodeId>)`
- `truncate(log_id: LogId<NodeId>)`
- `purge(log_id: LogId<NodeId>)`

`RaftStateMachine`:
- `applied_state(&mut self) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>>`
- `apply<I>(&mut self, entries: I) -> Result<Vec<R>, StorageError<NodeId>>`
- `get_snapshot_builder(&mut self) -> Self::SnapshotBuilder`
- `begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotData>, StorageError<NodeId>>`
- `install_snapshot(&meta, snapshot)`
- `get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<NodeId>>`

`RaftSnapshotBuilder`:
- `build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<NodeId>>`

## Network contracts

From `network/network.rs`:
- `append_entries(rpc, option: RPCOption)`
- `vote(rpc, option: RPCOption)`
- `install_snapshot(rpc, option: RPCOption)`
- optional override: `backoff(&self) -> Backoff`

From `network/factory.rs`:
- `type Network: RaftNetwork<C>`
- `new_client(&mut self, target: NodeId, node: &Node) -> Network`

## Recovery/validation

- `openraft::testing::Suite::test_all(builder)` is the official local storage/state-machine contract check.
- In this repo we should keep the adapter behind `cfg(feature = "openraft")` and maintain parity checks before removing the feature.
- A node shutdown path uses `Raft::shutdown()`. Background/process hangs should be treated as external orchestration first (separate long-running invocations).
