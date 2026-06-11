# OpenRaft Survival Context (0.8.9) — quick reference

Use this when context is compacted. It is intentionally minimal and scoped to what `ganglion-openraft`
needs now.

## Version contract

- The crate is pinned to `openraft = "0.8"` in `crates/ganglion-openraft/Cargo.toml` (not 0.10).
- `openraft` feature set required for adapter work:
  - `serde`
  - `storage-v2`
- `tokio` runtime is required for `Raft::new(...)`, async state, and `shutdown()`.

## Only the minimum trait surface to implement

### `RaftTypeConfig` / entry types

- Use `openraft::declare_raft_types!(...)` when possible.
- Required associated types for `Ganglion`:
  - `D: AppData`
  - `R: AppDataResponse`
  - `NodeId: NodeId`
  - `Node: Node`
  - `Entry: RaftEntry<NodeId, Node> + FromAppData<D>`
  - `SnapshotData: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static`

### Storage (`storage/v2.rs`) — core methods

- `RaftLogReader<C>`:
  - `get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>`
  - `try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>`
- `RaftLogStorage<C>`:
  - `type LogReader: RaftLogReader<C>`
  - `get_log_reader(&mut self) -> Self::LogReader`
  - `save_vote(&mut self, vote: &Vote<C::NodeId>)`
  - `read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, ...>`
  - `append(entries, callback: LogFlushed<C::NodeId>)`
  - `truncate(log_id)`
  - `purge(log_id)`
- `RaftStateMachine<C>`:
  - `type SnapshotBuilder: RaftSnapshotBuilder<C>`
  - `applied_state(&mut self) -> (Option<LogId>, StoredMembership)`
  - `apply(entries)`
  - `get_snapshot_builder(&mut self) -> SnapshotBuilder`
  - `begin_receiving_snapshot()`
  - `install_snapshot(&meta, snapshot)`
  - `get_current_snapshot()`

### Network (`network/network.rs` + `network/factory.rs`)

- `RaftNetwork<C>`:
  - `append_entries(rpc, option) -> AppendEntriesResponse`
  - `install_snapshot(rpc, option) -> InstallSnapshotResponse`
  - `vote(rpc, option) -> VoteResponse`
  - optional `backoff() -> Backoff`
- `RaftNetworkFactory<C>`:
  - `type Network: RaftNetwork<C>`
  - `new_client(target: C::NodeId, node: &C::Node) -> Network`

### Runtime API (`raft.rs`) — startup and lifecycle

- `Raft::new(id, config, network_factory, log_store, state_machine).await`
- `initialize(members).await`
- `add_learner(id, node, blocking).await`
- `change_membership(members, retain).await`
- `client_write(app_data).await`
- incoming RPC handlers on the running node:
  - `append_entries(rpc).await`
  - `vote(rpc).await`
  - `install_snapshot(rpc).await`
- `current_leader().await` / `is_leader().await`
- `shutdown().await` must be called on close.

## Testing harness for store correctness

- `openraft::testing::Suite::test_all(builder)` is the smallest broad contract check for storage + state machine.
- Keep trait behavior aligned with this suite; start there before wiring transport/integration tests.

## Source anchors (local, not internet)

- OpenRaft 0.8.9 sources available in:
  - `/home/george/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src`
- Prioritize reading only:
  - `storage/v2.rs`
  - `storage/mod.rs`
  - `network/network.rs`
  - `network/factory.rs`
  - `raft.rs`
  - `testing/suite.rs`
- Example scaffolds in `~/code/temp/openraft/examples` are newer (`0.10+`) and not authoritative for 0.8 signatures.
