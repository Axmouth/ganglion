# OpenRaft Survival Context (0.8.9) — minimal quick sheet

Keep this short and restart-safe for context compaction.

## Version contract

- `crates/ganglion-openraft/Cargo.toml`: `openraft = "0.8"`  
- features: `serde`, `storage-v2`
- `tokio` required for async bootstrap and `shutdown()`.

## Core type config

Use `openraft::declare_raft_types!` for ergonomics.

- `type D: AppData`
- `type R: AppDataResponse`
- `type NodeId: NodeId`
- `type Node: Node`
- `type Entry: RaftEntry<NodeId, Node> + FromAppData<D>`
- `type SnapshotData: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static`

## Required runtime calls (`raft.rs`)

- `Raft::new(id, config, network_factory, log_store, state_machine).await`
- `Raft::initialize(members).await`
- `Raft::client_write(app_data).await`
- `Raft::add_learner(id, node, blocking).await`
- `Raft::change_membership(members, retain).await`
- `Raft::append_entries(rpc).await` (incoming RPC entrypoint)
- `Raft::vote(rpc).await` (incoming RPC entrypoint)
- `Raft::install_snapshot(rpc).await` (incoming RPC entrypoint)
- `Raft::current_leader().await`
- `Raft::is_leader().await`
- `Raft::shutdown().await` must be called on close

## Log + state machine traits (`storage/v2.rs`, `storage/mod.rs`)

- `RaftLogReader<C>`:
  - `get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>>`
  - `try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(&mut self, range: RB) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>`
- `RaftLogStorage<C>`:
  - `type LogReader: RaftLogReader<C>`
  - `get_log_reader(&mut self) -> Self::LogReader`
  - `save_vote(&mut self, vote: &Vote<C::NodeId>)`
  - `read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>>`
  - `append<I>(&mut self, entries: I, callback: LogFlushed<C::NodeId>)`
  - `truncate(log_id: LogId<C::NodeId>)`
  - `purge(log_id: LogId<C::NodeId>)`
- `RaftStateMachine<C>`:
  - `type SnapshotBuilder: RaftSnapshotBuilder<C>`
  - `applied_state(&mut self) -> Result<(Option<LogId<C::NodeId>>, StoredMembership<C::NodeId, C::Node>), StorageError<C::NodeId>>`
  - `apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>`
  - `get_snapshot_builder(&mut self) -> Self::SnapshotBuilder`
  - `begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>>`
  - `install_snapshot(&meta: &SnapshotMeta<C::NodeId, C::Node>, snapshot: Box<C::SnapshotData>)`
  - `get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C::NodeId>>`

## Network traits (`network/network.rs`, `network/factory.rs`)

- `RaftNetwork<C>`:
  - `append_entries(rpc, option: RPCOption) -> Result<AppendEntriesResponse<C::NodeId>, ...>`
  - `install_snapshot(rpc, option: RPCOption) -> Result<InstallSnapshotResponse<C::NodeId>, ...>`
  - `vote(rpc, option: RPCOption) -> Result<VoteResponse<C::NodeId>, ...>`
  - `backoff(&self) -> Backoff` (default okay)
- `RaftNetworkFactory<C>`:
  - `type Network: RaftNetwork<C>`
  - `new_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network`

## Storage correctness check

- `openraft::testing::Suite::test_all(builder)` is the short broad contract check for storage + SM behavior.

## Source anchors (local)

- `/home/george/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src`
  - `raft.rs`, `storage/v2.rs`, `storage/mod.rs`, `network/network.rs`, `network/factory.rs`, `testing/suite.rs`
- `~/code/temp/openraft/examples` is 0.10+ and not authoritative for these signatures.
