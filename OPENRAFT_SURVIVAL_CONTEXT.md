# OpenRaft 0.8.9 Survival Context

Open this first when context is compacted. Everything below is **verified against
openraft 0.8.9 source and compiles in this repo** (see `crates/ganglion-openraft/src/openraft_runtime/`).

## 0) Version anchors

Pinned: `openraft = "0.8"` (resolves 0.8.9), features `["serde", "storage-v2"]`, default-features off.

Source of signature truth (registry cache):
- `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.8.9/src/raft.rs` — `RaftTypeConfig`, `declare_raft_types!`, `Raft` API
- `.../src/storage/mod.rs` — `RaftLogReader`, `RaftSnapshotBuilder`, `LogState`, `Snapshot`, `SnapshotMeta`
- `.../src/storage/v2.rs` — `RaftLogStorage`, `RaftStateMachine`
- `.../src/storage/callback.rs` — `LogFlushed`
- `.../src/network/network.rs`, `.../src/network/factory.rs` — network traits
- `.../src/testing/suite.rs` — contract test suite + `StoreBuilder`

WARNING: local clone `/home/george/code/temp/openraft` is 0.10-alpha — design guidance only,
its signatures (e.g. `declare_raft_types` with `LeaderId`, `SnapshotDataOf`) do NOT apply to 0.8.9.

## 1) Already implemented in this repo (do not redo)

- `openraft_runtime/mod.rs`: `GanglionRaftConfig` (D=`MetadataRaftCommand`, R=`MetadataRaftResponse`,
  NodeId=`u64`, Node=`BasicNode`, Entry=`openraft::Entry<...>`, SnapshotData=`Cursor<Vec<u8>>`),
  `default_raft_config()`.
- `openraft_runtime/storage.rs`: `GanglionLogStore` (RaftLogReader + RaftLogStorage) and
  `GanglionStateMachine` (RaftStateMachine + RaftSnapshotBuilder), both `Arc<Mutex<_>>`-shared and Clone.
  **Passing `openraft::testing::Suite::test_all`.** State machine deterministically rejects stale
  generations via `MetadataRaftResponse::accepted = false` (state unchanged) — never via error.

Still to build: `RaftNetwork`/`RaftNetworkFactory` (in-process router first), runtime node that
wraps `Raft<...>` and implements `MetadataConsensus`.

## 2) Verified API gotchas (cost time once; don't rediscover)

- `declare_raft_types!` does NOT accept a trailing comma after the last `Type = ...` entry.
- The macro emits `#[cfg_attr(feature = "serde", ...)]` which warns `unexpected_cfgs` in our crate
  (our feature is named `openraft`, not `serde`) — harmless, ignore.
- `Entry<C>: Clone` only when `C::D: Clone` — `MetadataRaftCommand` derives Clone, so `entry.clone()` works.
- `openraft::async_trait` is re-exported (`use openraft::async_trait::async_trait;`) — no direct
  async-trait dependency needed. All storage/network traits are `#[async_trait]`.
- tokio's `io` module (AsyncRead/Write/Seek + impls for `std::io::Cursor<Vec<u8>>`) is unconditional —
  no extra tokio features needed for `SnapshotData = Cursor<Vec<u8>>`.
- `append(entries, callback: LogFlushed<NodeId>)`: must call `callback.log_io_completed(Ok(()))`
  once data is durable (immediately for in-memory). Not calling it stalls raft.
- `truncate(log_id)` deletes `>= log_id.index` (`BTreeMap::split_off(&index)` keeps the head).
  `purge(log_id)` deletes `<= log_id.index` AND must record `last_purged_log_id`.
- `get_log_state()`: `last_log_id` = last entry in log, falling back to `last_purged_log_id` when empty.
- `StorageIOError` constructors: `read_logs/write_logs/read_vote/write_vote/read_state_machine/
  write_state_machine/read_snapshot(Option<SnapshotSignature>, e)/write_snapshot/apply(log_id, e)`;
  all take `impl Into<AnyError>`, and `&dyn std::error::Error` converts (`&error` works).
  `StorageError: From<StorageIOError>` so `?` after `.map_err(|e| StorageIOError::...(&e))` is fine.
- `SnapshotMeta { last_log_id, last_membership, snapshot_id: String }`. Snapshot data carries app
  state only; meta carries last_applied/membership — restore both in `install_snapshot`.
- `install_snapshot` may hand you an empty cursor in edge paths — treat empty data as default state.
- State machine `apply` must be infallible business-wise: rejections must be encoded in the response
  type (deterministic across replicas), never returned as `StorageError`.

## 3) Contract test wiring (storage)

```rust
use openraft::testing::{StoreBuilder, Suite};

struct B;
#[async_trait]
impl StoreBuilder<GanglionRaftConfig, GanglionLogStore, GanglionStateMachine, ()> for B {
    async fn build(&self) -> Result<((), GanglionLogStore, GanglionStateMachine), StorageError<u64>> {
        Ok(((), GanglionLogStore::default(), GanglionStateMachine::default()))
    }
}
#[test]
fn suite() -> Result<(), StorageError<u64>> { Suite::test_all(B) }  // plain #[test]
```
`Suite` internally calls `run_fut` which creates its **own** tokio multi-thread runtime —
do NOT wrap in `#[tokio::test]`. Requires `C::NodeId: From<u64>` (u64 satisfies).

## 4) Network traits (next to implement)

```rust
#[async_trait]
impl RaftNetwork<GanglionRaftConfig> for Conn {
    async fn append_entries(&mut self, rpc: AppendEntriesRequest<C>, option: RPCOption)
        -> Result<AppendEntriesResponse<NID>, RPCError<NID, Node, RaftError<NID>>>;
    async fn install_snapshot(&mut self, rpc: InstallSnapshotRequest<C>, option: RPCOption)
        -> Result<InstallSnapshotResponse<NID>, RPCError<NID, Node, RaftError<NID, InstallSnapshotError>>>;
    async fn vote(&mut self, rpc: VoteRequest<NID>, option: RPCOption)
        -> Result<VoteResponse<NID>, RPCError<NID, Node, RaftError<NID>>>;
}
#[async_trait]
impl RaftNetworkFactory<GanglionRaftConfig> for Router {
    type Network = Conn;
    async fn new_client(&mut self, target: NID, node: &BasicNode) -> Self::Network;
}
```
VERIFIED in 0.8.9 `network/network.rs`: the `send_*` variants are DEPRECATED defaults
(removed in 0.9) — implement `append_entries`/`install_snapshot`/`vote` taking `RPCOption`.
RPC request/response types live in `openraft::raft::{AppendEntriesRequest, AppendEntriesResponse,
InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse}`.
For an in-process router, forward each RPC directly into the target `Raft` handle's
`raft.append_entries(rpc)/vote(rpc)/install_snapshot(rpc)` and wrap raft errors in
`RPCError::RemoteError(RemoteError::new(target, e))` / unreachable peers in `RPCError::Unreachable`.

## 5) Runtime lifecycle

- `Raft::new(node_id, config, network_factory, log_store, state_machine).await?`
- `raft.initialize(BTreeMap<NodeId, BasicNode>).await?` — once, on one node, blank state.
- `raft.client_write(MetadataRaftCommand::...).await?` → `ClientWriteResponse { data: MetadataRaftResponse, .. }`
- `raft.is_leader().await` / `raft.current_leader().await` / `raft.metrics()` (watch channel)
- `raft.shutdown().await`
- `default_raft_config()` already validates `openraft::Config::default()`.
