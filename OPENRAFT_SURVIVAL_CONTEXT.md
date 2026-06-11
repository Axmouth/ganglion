# OpenRaft 0.8.9 Survival Context

Open this first when context is compacted.
Read files in this order and only consult the sections below.

## 0) Version anchors

Primary source: `openraft-0.8.9` cache under Rust cargo registry.
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/raft.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/storage/v2.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/storage/mod.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/network/network.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/network/factory.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/testing/suite.rs`

Reference for patterns and transport wiring (local clone; current clone is newer than 0.8.x, so only use as design guidance):
- `/home/george/code/temp/openraft/examples/raft-kv-memstore/src/lib.rs`
- `/home/george/code/temp/openraft/examples/raft-kv-memstore/src/store/mod.rs`
- `/home/george/code/temp/openraft/examples/raft-kv-rocksdb/src/lib.rs`
- `/home/george/code/temp/openraft/examples/raft-kv-memstore/src/test.rs`

## 1) Critical constraints for this repo

- `NodeId` in openraft 0.8 must be copyable (`u64` is the safest path).
- This crate currently depends on `openraft = "0.8"` with `storage-v2` + `serde`.
- Use `openraft::declare_raft_types!` only after defining all required associated types.

## 2) Minimum API surface for first run

### Type config

- `RaftTypeConfig` requires all of:
  - `D: AppData`
  - `R: AppDataResponse`
  - `NodeId: NodeId` (copyable)
  - `Node: Node`
  - `Entry: RaftEntry<NodeId, Node> + FromAppData<D>`
  - `SnapshotData: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static`

### Storage traits (`storage-v2`)

- `RaftLogReader<C>`
  - `get_log_state(&mut self)`
  - `try_get_log_entries(range)`
- `RaftLogStorage<C>`
  - `get_log_reader()`
  - `save_vote()`
  - `read_vote()`
  - `append(entries, callback)`
  - `truncate(log_id)`
  - `purge(log_id)`
- `RaftStateMachine<C>`
  - `applied_state()`
  - `apply(entries)`
  - `get_snapshot_builder()`
  - `begin_receiving_snapshot()`
  - `install_snapshot(meta, snapshot)`
  - `get_current_snapshot()`
- `RaftSnapshotBuilder<C>`
  - `build_snapshot()`

### Network traits

- `RaftNetwork<C>`
  - `append_entries(rpc, option)`
  - `vote(rpc, option)`
  - `install_snapshot(rpc, option)`
- `RaftNetworkFactory<C>`
  - `new_client(target, node) -> Self::Network`

### Runtime calls used by bootstrap/control

- `Raft::new(id, config, network, log_store, state_machine).await`
- `initialize(members)`
- `client_write(payload)`
- `append_entries`, `vote`, `install_snapshot`
- `add_learner`, `change_membership`
- `current_leader`, `is_leader`
- `shutdown().await`

### Contract test

- Run through `openraft::testing::Suite::test_all(builder)` for storage + state machine contract checks.
