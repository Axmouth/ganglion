# Openraft Survival Context

This is the compact anchor for continuing work after context compaction.  
Current repo target: `openraft = "0.8"` in `crates/ganglion-openraft/Cargo.toml`.

## Source files (what to reopen first)

- `~/.cargo/registry/src/.../openraft-0.8.9/src/raft.rs`
- `~/.cargo/registry/src/.../openraft-0.8.9/src/storage/v2.rs`
- `~/.cargo/registry/src/.../openraft-0.8.9/src/storage/mod.rs`
- `~/.cargo/registry/src/.../openraft-0.8.9/src/network/network.rs`
- `~/.cargo/registry/src/.../openraft-0.8.9/src/network/factory.rs`
- `~/.cargo/registry/src/.../openraft-0.8.9/src/raft_types.rs`
- examples for shape/reference: `~/code/temp/openraft/examples/raft-kv-memstore`

## Required integration surface (0.8)

### 1) Raft type config and payloads

- `TypeConfig`: `impl openraft::RaftTypeConfig`
- associated types:
  - `D: AppData` (request payload)
  - `R: AppDataResponse` (response payload)
  - `NodeId`, `Node` (often `BasicNode`)
  - `Entry`, `SnapshotData`

### 2) Log subsystem

Traits to provide:
- `RaftLogReader`
  - `get_log_state() -> Result<LogState>`
  - `try_get_log_entries(range)`
- `RaftLogStorage`
  - `get_log_reader()`
  - `save_vote(&vote)`
  - `read_vote()`
  - `append(entries, callback)`
  - `truncate(log_id)`
  - `purge(log_id)`

### 3) State machine + snapshots

Trait: `RaftStateMachine`
- `applied_state()`
- `apply(entries)`
- `get_snapshot_builder()`
- `begin_receiving_snapshot()`
- `install_snapshot(meta, snapshot)`
- `get_current_snapshot()`

### 4) Network

Trait: `RaftNetworkFactory`
- `type Network: RaftNetwork`
- `new_client(target, node) -> Network`

Trait: `RaftNetwork`
- `append_entries(rpc, option)`
- `vote(rpc, option)`
- `install_snapshot(rpc, option)`
- `backoff()`

### 5) Node/runtime control

From `Raft`:
- `Raft::new(id, config, network, log_store, state_machine)`
- `initialize(members)`
- `client_write(app_data)`
- `add_learner(id, node, blocking)`
- `change_membership(members, retain)`
- `current_leader()` / `is_leader()`
- `metrics()`
- `shutdown()`

## Important behavior rules (do not skip)

- Log/index continuity: no holes.
- `append` must not return before entries are readable; callback must fire only when durable.
- `truncate`/`purge` must avoid leaving holes.
- Persisted vote has to be durable before return.
- Always stop raft handles with `shutdown().await` to avoid long-lived background tasks.
- In v0.8, traits in `v2.rs` are sealed unless feature `storage-v2` is enabled.
  - If implementing `RaftStorage` first, `storage::Adaptor` provides a bridge to required v2 traits.

## One-line continuation note

For this repo, wire toward the above API names first, then move to richer cluster-management flows when the metadata adapter is stable.
