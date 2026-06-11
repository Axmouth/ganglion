# OpenRaft Survival Context (0.8.9)

Use this as the quick anchor after context compaction. It tracks only the API required for the
first working openraft runtime in `ganglion-openraft`.

## Version / feature lock

- `openraft = "0.8.9"` is pinned in `crates/ganglion-openraft/Cargo.toml`.
- Current `features` for trait implementation:
  - `serde` enabled
  - `storage-v2` must be enabled to implement `RaftLogStorage`/`RaftStateMachine`
- `tokio` is required for `Raft::new`, task runtime, and `shutdown`.
- Temp cloned examples currently in `~/code/temp/openraft` are newer (`0.10.0-alpha.21`), so keep this repo to 0.8.9 signatures.

## Core source files to reopen

- `~/.cargo/registry/src/*/openraft-0.8.9/src/raft.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/raft_types.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/storage/mod.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/storage/v2.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/network/network.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/network/factory.rs`
- `~/.cargo/registry/src/*/openraft-0.8.9/src/testing/suite.rs`
- Example baseline: `~/code/temp/openraft/examples/raft-kv-memstore`

## What we must implement (minimum to function)

### Type config (`RaftTypeConfig`)

- Required associated types:
  - `D: AppData`
  - `R: AppDataResponse`
  - `NodeId: NodeId` (copy-able + ordered + hashable + display + serde when feature enabled)
  - `Node: Node` (at least `Clone + Default + Eq + Debug + serde`)
  - `Entry: RaftEntry<NodeId, Node> + FromAppData<Self::D>`
  - `SnapshotData: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static`
- Typically define with `openraft::declare_raft_types!(...)`.

### Storage (`storage/v2.rs`)

- `RaftLogReader<C>`:
  - `get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>`
  - `try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<C::Entry>, ...>`
- `RaftLogStorage<C>`:
  - `type LogReader`
  - `get_log_reader(&mut self)`
  - `save_vote(&mut self, vote)`
  - `read_vote(&mut self)`
  - `append(entries, callback)`  (callback indicates flush completion)
  - `truncate(log_id)`
  - `purge(log_id)`
- `RaftStateMachine<C>`:
  - `applied_state(&mut self)`
  - `apply(entries)`
  - `get_snapshot_builder(&mut self)`
  - `begin_receiving_snapshot(&mut self)`
  - `install_snapshot(&mut self, meta, snapshot)`
  - `get_current_snapshot(&mut self)`

### Network (`network/*.rs`)

- `RaftNetwork<C>`:
  - `append_entries(rpc, option)`
  - `vote(rpc, option)`
  - `install_snapshot(rpc, option)`
  - `backoff() -> Backoff`
- `RaftNetworkFactory<C>`:
  - `type Network: RaftNetwork<C>`
  - `new_client(target, node) -> Network`

### Runtime API (`Raft::...`)

- `Raft::new(id, config, network_factory, log_store, state_machine)`
- `Raft::initialize(members)`
- `Raft::client_write(app_data)`
- `Raft::append_entries(...)`, `vote(...)`, `install_snapshot(...)` (incoming RPC handlers)
- `Raft::add_learner(id, node, blocking)`
- `Raft::change_membership(members, retain)`
- `Raft::current_leader()`, `Raft::is_leader()`, `Raft::metrics()`
- `Raft::shutdown().await` must be called to cleanly exit background tasks.

## Context compaction-safe behavior checklist

- `append`, `save_vote`, and `truncate/purge` are strict order/durability boundaries.
- No index holes in the log.
- `install_snapshot` path must replace state/snapshot atomically before returning.
- Snapshot build/install path for compaction:
  1) `get_snapshot_builder().build_snapshot()`
  2) `client_write(...)`/replication drives install
  3) follower receives `install_snapshot(...)`
  4) follower `begin_receiving_snapshot` + `install_snapshot`.
- For behavior sanity in early phases, use `openraft::testing::Suite<C, LS, SM>::test_all(builder)` and keep
  the signatures aligned to this version.

## Ganglion note

- Ganglion node IDs in existing APIs are strings; openraft `NodeId` on 0.8 examples uses numeric IDs by default.
  Use one conversion strategy and keep it consistent across membership config and RPC node addressing.
