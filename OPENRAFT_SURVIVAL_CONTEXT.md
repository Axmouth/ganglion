# OpenRaft Survival Notes (0.8.9)

Targeted reference for quick recovery after context reset.

## Use these files first

- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/raft.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/storage/v2.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/storage/mod.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/network/network.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/network/factory.rs`
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/testing/suite.rs`

Important: `/home/george/code/temp/openraft/examples/*` is newer OpenRaft. Keep this repo to 0.8 signatures.

## Core type definition (0.8)

`openraft::RaftTypeConfig` + `openraft::declare_raft_types!`:

- `D: AppData`
- `R: AppDataResponse`
- `NodeId: NodeId`
- `Node: Node`
- `Entry: RaftEntry<NodeId, Node> + FromAppData<D>`
- `SnapshotData: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static`

Typical in this repo:
- `NodeId = u64`
- `Node = BasicNode` for transport address metadata (`addr: String`)

## Must-have Raft calls

- `Raft::new(id, config, network_factory, log_store, state_machine).await`
- `Raft::initialize(members).await`
- `Raft::client_write(app_data).await`
- `Raft::add_learner(id, node, blocking).await`
- `Raft::change_membership(members, retain).await`
- inbound endpoints: `append_entries`, `vote`, `install_snapshot`
- status: `current_leader`, `is_leader`, `shutdown().await`

## Storage contracts to implement (`storage-v2`)

`RaftLogReader<C>`
- `get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>`
- `try_get_log_entries<RB: RangeBounds<u64> + ...>(&mut self, range: RB) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>`

`RaftLogStorage<C>`
- `type LogReader: RaftLogReader<C>`
- `get_log_reader(&mut self) -> Self::LogReader`
- `save_vote(&mut self, vote: &Vote<C::NodeId>)`
- `read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>>`
- `append<I>(&mut self, entries: I, callback: LogFlushed<C::NodeId>)`
- `truncate(log_id: LogId<C::NodeId>)`
- `purge(log_id: LogId<C::NodeId>)`

`RaftStateMachine<C>`
- `applied_state(&mut self) -> Result<(Option<LogId<C::NodeId>>, StoredMembership<C::NodeId, C::Node>), StorageError<C::NodeId>>`
- `apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>`
- `get_snapshot_builder(&mut self) -> Self::SnapshotBuilder`
- `begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>>`
- `install_snapshot(&meta: &SnapshotMeta<C::NodeId, C::Node>, snapshot: Box<C::SnapshotData>)`
- `get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>>`

`RaftSnapshotBuilder<C>`
- `build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<C::NodeId>>`

## Network contracts

`RaftNetwork<C>`
- `append_entries(rpc: AppendEntriesRequest<C>, option: RPCOption) -> Result<AppendEntriesResponse<C::NodeId>, ...>`
- `vote(rpc: VoteRequest<C::NodeId>, option: RPCOption) -> Result<VoteResponse<C::NodeId>, ...>`
- `install_snapshot(rpc: InstallSnapshotRequest<C>, option: RPCOption) -> Result<InstallSnapshotResponse<C::NodeId>, ...>`
- optional override: `backoff(&self) -> Backoff`

`RaftNetworkFactory<C>`
- `type Network: RaftNetwork<C>`
- `new_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network`

## Validation path

- `openraft::testing::Suite::test_all(store_or_builder)` validates storage + state machine contract.

## Minimal bootstrap skeleton

1. `let config = Arc::new(Config::default().validate()?)`
2. `Raft::new(id, config, network, log_store, state_machine).await?`
3. call `initialize(members)` for first node or add learners + change_membership for expansion
4. expose inbound RPC handlers: `append_entries`, `vote`, `install_snapshot`
