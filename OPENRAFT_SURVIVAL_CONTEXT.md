# Openraft Survival Notes (Context Compact)

This file is the compact reference for resuming implementation work after context compaction.

## Version guard

- `ganglion-openraft` currently depends on `openraft = "0.8"` (see `crates/ganglion-openraft/Cargo.toml`).
- The local clone under `~/code/temp/openraft` is `0.10.0-alpha.21`, which uses the v2 network traits (`RaftNetworkV2`).
- Before wiring anything, confirm which API surface is being targeted:
  - If we keep `openraft = "0.8"` in place, bind to that crate’s exact traits for this codebase.
  - If we upgrade openraft, rebase the integration on the v2 examples in this clone.

## What we need from openraft (minimal)

For the first step, keep the implementation intentionally small:

1. Type config and data models
2. Raft log store (`RaftLogStorage`) + reader (`RaftLogReader`)
3. State machine (`RaftStateMachine`)
4. Network factory/client (`RaftNetworkFactory`, `RaftNetwork`/`RaftNetworkV2`)
5. Node bootstrap lifecycle (`Raft::new`, membership initialization, write path, metrics/query, shutdown)

### 1) Type config and payloads

Reference file:
- `~/code/temp/openraft/examples/raft-kv-memstore/src/lib.rs`

Keep the contract tight:
- `Request` payload implements `AppData`
- `Response` payload implements `AppDataResponse`
- `declare_raft_types!(...)` to define:
  - `D` (request type)
  - `R` (response type)
  - `Node` (`BasicNode` for this project initially)
  - optional: custom `Entry`, `SnapshotData`, runtime alias config when needed

### 2) Log store

Reference file:
- `~/code/temp/openraft/examples/log-mem/src/log_store.rs`

Required methods from `RaftLogReader`:
- `try_get_log_entries(range)`
- `read_vote()`

Required methods from `RaftLogStorage`:
- `get_log_state()`
- `get_log_reader()`
- `read_committed()`
- `save_committed(log_id)`
- `save_vote(&vote)`
- `append(entries, callback)`
- `truncate_after(last_log_id)`
- `purge(log_id)`

Important behavior constraints:
- persisted callback must only be called once appended entries are durable
- log must stay contiguous (no holes)
- operations must be serialized per node lifecycle

### 3) State machine

Reference file:
- `~/code/temp/openraft/examples/sm-mem/src/lib.rs`

Required methods from `RaftStateMachine`:
- `applied_state()`
- `apply(entries)`
- `begin_receiving_snapshot()`
- `install_snapshot(meta, snapshot)`
- `get_current_snapshot()`
- `get_snapshot_builder()` (or `try_create_snapshot_builder(force)` if targeting newer API)

For our metadata plane:
- keep `applied_state` as simple `{last_log_id, last_membership}`
- implement deterministic snapshot encode/decode around `CoordinationSnapshot`

### 4) Network layer

Reference file:
- `~/code/temp/openraft/examples/network-v2-http/src/client.rs`
- `~/code/temp/openraft/examples/network-v2-http/src/server.rs`

For v2 network API (clone target), required network methods:
- `append_entries(rpc, option)`
- `vote(req, option)`
- `full_snapshot(vote, snapshot, cancel, option)`
- `transfer_leader(req, option)` optional default if not used
- `RaftNetworkFactory::new_client(target, node)`

For v0.8 compatibility:
- use the available equivalent request/response methods from that exact `openraft` version and keep
  names consistent with the server/client handlers.

### 5) Lifecycle and cluster control

Reference file:
- `~/code/temp/openraft/openraft/src/raft/mod.rs`
- `~/code/temp/openraft/examples/raft-kv-memstore/src/tests/...` for call patterns

Core calls used by ganglion integration:
- `Raft::new(node_id, config, network, log_store, state_machine)`
- `raft.initialize(...)` to bootstrap first configuration
- `raft.client_write(...)` for proposal submission
- `raft.metrics().await` for node/leader visibility
- `raft.is_initialized().await`
- `raft.shutdown().await`

Note:
- If `shutdown` is not called on owned raft handles, background tasks can stay alive beyond test/process boundaries.

## Current ganglion placement

- `crates/ganglion-openraft/src/lib.rs` currently keeps the local/placeholder consensus adapter.
- Openraft-backed runtime is still to be added behind the existing feature gate (`openraft`).

## Keep this alive across compaction

When context is compacted, reopen only these references and the listed method names.
If the `openraft` version changes, re-run this note against the new API surface before implementation.
