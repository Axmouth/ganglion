# OpenRaft Migration Guide (Ganglion-specific)

Scope: migrating `ganglion-openraft` off the **0.8.9** line. The current openraft
stable line is **0.9.24** (target this); **0.10** is alpha-only (`0.10.0-alpha.x`)
with no official upgrade guide yet, so its section here is provisional.

This is not a generic openraft guide. It maps openraft's documented API changes
onto the exact places Ganglion touches openraft, so the migration is a checklist,
not an investigation.

## Why this is bounded for Ganglion

- **`ganglion-core` has zero openraft dependency.** All model, epoch, placement,
  snapshot, and durability-policy logic is version-free and does not change.
- **Consumers are behind the wrapper.** `RaftMetadataNode` + the `ganglion`
  umbrella crate insulate Fibril/arbiter. The only downstream blast radius is code
  that names openraft types via Ganglion's `pub use openraft` re-export
  (`crates/ganglion-openraft/src/openraft_runtime/mod.rs:24`). Grep consumers for
  `openraft::` before assuming a clean recompile.
- **The storage suite is your oracle.** `openraft::testing::Suite::test_all`
  already gates both log stores. Port, run the suite, iterate to green. Most raft
  storage upgrades are blind; this one is not.

## Ganglion's openraft surface (what the migration has to touch)

Trait implementations:

| Trait | File | Methods implemented |
| --- | --- | --- |
| `RaftLogReader` | `storage.rs:41`, `durable.rs:278` | `get_log_state`, `try_get_log_entries` |
| `RaftLogStorage` | `storage.rs:73`, `durable.rs:312` | `get_log_reader`, `save_vote`, `read_vote`, `append`, `truncate`, `purge` |
| `RaftStateMachine` | `storage.rs:308` | `applied_state`, `apply`, `get_snapshot_builder`, `begin_receiving_snapshot`, `install_snapshot`, `get_current_snapshot` |
| `RaftSnapshotBuilder` | `storage.rs:275` | `build_snapshot` |
| `RaftNetwork` | `network.rs:138`, `tcp.rs:372` | `append_entries`, `install_snapshot`, `vote` |
| `RaftNetworkFactory` | `network.rs:102`, `tcp.rs:330` | `new_client` |

Type config: `declare_raft_types!` at `mod.rs:104`.
Public `Raft` handle: `node.rs:565` (`raft()` returns `&Raft<GanglionRaftConfig, NF, LS, GanglionStateMachine>`).
Symbols in use: `Vote<NodeId>`, `LogId<NodeId>`, `LogState`, `StorageError<NodeId>`,
`StorageIOError`, `Entry`, `EntryPayload`, `Snapshot`, `LogFlushed`,
`CommittedLeaderId`, `RPCOption`, `RaftError`, `ClientWriteError`, `BasicNode`,
`SnapshotPolicy::LogsSinceLast`.

Note: Ganglion is already on **storage-v2** (`RaftLogStorage` + `RaftStateMachine`
split) and the **`option`-carrying `RaftNetwork`** API (`RPCOption`). Those were the
v0.8.4 changes, so they are already done. Everything below is the **v0.9.0** delta.

---

# Early preparation (safe on 0.8.9 today)

Two refactors you can land *before* any version bump. Both shrink future migration
pain and are good hygiene regardless of which openraft changes actually arrive.

Sequencing: both touch `ganglion-openraft`, so land them **after** the replication
branch cleanup merges -- not woven into it. Neither changes behavior.

## A. Narrow the `pub use openraft` re-export (highest leverage, version-agnostic)

Today `ganglion-openraft` does `pub use openraft;` (`mod.rs:24`) and the `ganglion`
umbrella does `pub use ganglion_openraft::*`, so consumers (Fibril, arbiter) *can*
name openraft types directly. That makes every openraft bump a potential downstream
break.

Re-export only the handful of openraft types consumers legitimately need (or wrap
them in Ganglion newtypes/aliases). Then 0.9, 0.10, and every later bump become
**consumer-invisible** -- the blast radius stays inside `ganglion-openraft`
permanently. This pays off at every version, not just the next one.

Action: grep Fibril/arbiter for `openraft::` to see what they actually reach for,
then expose exactly those through Ganglion's own surface and drop the blanket
re-export.

## B. Centralize the `<NodeId>`-parameterized spellings behind local aliases

This pre-stages the likely (unconfirmed) 0.10 `NodeId -> C` change AND reads better
today. Define the parameterized types once:

```rust
type GStorageError = openraft::StorageError<NodeId>;
type GLogId        = openraft::LogId<NodeId>;
type GVote         = openraft::Vote<NodeId>;
```

Use these aliases in every signature across `storage.rs`, `durable.rs`,
`network.rs`, `tcp.rs`, `node.rs`. If 0.10 flips the parameter, you edit three alias
definitions instead of dozens of signatures.

Limit (be honest about scope): aliases absorb *parameter substitution* only, not
signature *reshape*. If a method gains or loses an argument in 0.10, the alias will
not save that site. They also do not touch constructors (`Vote::new`,
`CommittedLeaderId::new`) -- only type spellings.

Do **not** build anything else 0.10-specific until the official `upgrade_09_10`
guide exists; aliasing is worth it on hygiene grounds alone, which is why it is safe
to do now.

---

# 0.8.9 -> 0.9.24

Source: openraft `upgrade_08_09` upgrade guide and the v0.9.0 change log. Each item
lists the verbatim change, then the exact Ganglion edit.

## 1. `AsyncRuntime` type parameter on `RaftTypeConfig` (required)

> "add AsyncRuntime type parameter to `RaftTypeConfig`"

`declare_raft_types!` must declare the runtime.

Before (`mod.rs:104`):

```rust
openraft::declare_raft_types!(
    pub GanglionRaftConfig:
        D = MetadataRaftCommand,
        R = MetadataRaftResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<GanglionRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>
);
```

After:

```rust
openraft::declare_raft_types!(
    pub GanglionRaftConfig:
        D = MetadataRaftCommand,
        R = MetadataRaftResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<GanglionRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime
);
```

Keeping `SnapshotData = Cursor<Vec<u8>>` is deliberate (see item 5).

## 2. Async-trait macro change (mechanical, every impl block)

> "async traits in Openraft are declared with `#[openraft-macros::add_async_trait]`
> ... `#[async_trait::async_trait]` are no longer needed."

Ganglion currently uses `openraft::async_trait::async_trait` (storage.rs, durable.rs,
network.rs, tcp.rs). Replace each `#[async_trait::async_trait]` /
`#[openraft::async_trait::async_trait]` annotation on the impl blocks above with the
new macro.

> VERIFY the exact re-export path in 0.9.24 (`openraft::add_async_trait` vs the
> `openraft-macros` crate) from docs.rs before bulk-replacing.

## 3. `Raft<C, N, LS, SM>` collapses to `Raft<C>` (public-surface change)

> "Generic types parameters `N, LS, SM` are removed from `Raft<C, N, LS, SM>`."

Before (`node.rs:565`):

```rust
pub fn raft(&self) -> &Raft<GanglionRaftConfig, NF, LS, GanglionStateMachine> { ... }
```

After:

```rust
pub fn raft(&self) -> &Raft<GanglionRaftConfig> { ... }
```

Ripple to assess: `RaftMetadataNode<LS = ..., NF = ...>` carries `LS`/`NF` type
params largely to parametrize the stored `Raft` handle. With `Raft<C>` the handle no
longer needs them, so Ganglion's own node generics may simplify substantially. This
is an opportunity (smaller public API) but also a breaking change to *Ganglion's*
surface, so do it deliberately and update the `start*` constructors and the
`network.rs`/`tcp.rs` bound declarations (`LS: RaftLogStorage<...>` etc.) to match.
`Raft::new(id, config, network, log_store, state_machine)` still takes the concrete
implementations as arguments; only the *type* dropped the params.

## 4. `get_log_state` moves from `RaftLogReader` to `RaftLogStorage`

> "Implementation of `RaftLogReader::get_log_state()` is moved to
> `RaftLogStorage::get_log_state()`."

Move the method body out of the `RaftLogReader` impl and into the `RaftLogStorage`
impl in both stores:

- `storage.rs`: from `impl RaftLogReader` (around `:42`) to `impl RaftLogStorage`
  (around `:73`).
- `durable.rs`: from `impl RaftLogReader` (around `:279`) to `impl RaftLogStorage`
  (around `:312`).

Signature is unchanged; only the trait it lives on changes.

## 5. Snapshot transfer overhaul (highest-risk item)

> "`RaftNetwork::full_snapshot()` to send a complete snapshot ... feature flag
> `generic-snapshot-data` ... Openraft provides a default implementation in
> `Chunked`."

0.9 makes whole-snapshot transfer (`full_snapshot`) the primary path. Two viable
routes for Ganglion:

- **Route A (lower risk, recommended): keep chunked.** Ganglion's
  `SnapshotData = Cursor<Vec<u8>>` already satisfies `AsyncRead + AsyncWrite +
  AsyncSeek`, so you do **not** need `generic-snapshot-data`. Keep the chunked
  receiving path (`begin_receiving_snapshot` / `install_snapshot` /
  `get_current_snapshot` on the state machine, `storage.rs:466-500`) and provide
  `full_snapshot` on `RaftNetwork` by delegating to the built-in `Chunked` adapter.
- **Route B: go generic.** Enable `generic-snapshot-data`, implement `full_snapshot`
  to ship the whole `Cursor<Vec<u8>>` in one shot, drop the chunk plumbing. Cleaner
  long-term, more code churn now.

Affected impls either way: `RaftNetwork::install_snapshot` (`network.rs:153`,
`tcp.rs:387`) and the state-machine snapshot methods.

> VERIFY against docs.rs `RaftNetwork` for 0.9.24 exactly which methods are
> *required* vs defaulted, and the `Chunked` delegation signature. This is the one
> area to confirm from the live API rather than this doc. Lean on
> `Suite::test_all` plus the existing snapshot-transfer cluster tests here as the
> gate.

## 6. `save_committed` / `read_committed` on `RaftLogStorage` (recommended)

> "save committed log id" -- both storage APIs gain `save_committed()` /
> `read_committed()` with default dummy implementations.

Optional to *compile* (defaults exist) but recommended to *implement* for the
durable store: persisting the committed log id lets committed-but-not-yet-applied
entries apply on restart, tightening recovery. Add real impls in `durable.rs`
alongside `save_vote`/`read_vote`; the in-memory store (`storage.rs`) can keep the
defaults or store it in memory. Cross-check the interaction with Ganglion's
snapshot-based bounded recovery so you do not double-count on replay.

## 7. `is_leader()` deprecated in favor of `ensure_linearizable()`

> "add `Raft::ensure_linearizable()` to ensure linearizable read -- replaces
> deprecated `is_leader()`."

Check whether Ganglion's own `RaftMetadataNode::is_leader` (`node.rs:431`) /
`current_leader` (`node.rs:427`) call `raft.is_leader()` internally or read
`metrics()`. If they call the deprecated method, switch to `metrics()`-based leader
check or `ensure_linearizable()`.

> Forward pointer: `ensure_linearizable()` is the confirmed-leader read primitive.
> It is directly relevant to the "no trustworthy primary lease" gap noted for the
> Ganglion-coordinated replicated-sqlite design -- upgrading to 0.9 *adds* the
> primitive that partly closes it. Worth surfacing on `RaftMetadataNode` as a public
> method during this migration.

## 8. Trigger / runtime-config relocation (verify, likely minor)

> "move external command trigger to dedicated `Trigger` struct"; "move runtime
> config API to dedicated `RuntimeConfigHandle`."

If anything in `node.rs` manually triggers elections / snapshots / log purge or
mutates runtime config, route it through `raft.trigger()` / `raft.runtime_config()`.
The `wait_for_*` helpers that read `metrics()` are unaffected. Grep `node.rs` for
`trigger`/`runtime` to confirm scope.

## 0.8 -> 0.9 checklist

- [ ] `cargo update`/Cargo.toml to `openraft = "0.9.24"`, keep
      `features = ["serde", "storage-v2"]` (confirm both still exist in 0.9).
- [ ] Add `AsyncRuntime = openraft::TokioRuntime` to `declare_raft_types!`.
- [ ] Swap async-trait macro on all six impl sites.
- [ ] Collapse `Raft<C,N,LS,SM>` -> `Raft<C>`; simplify `RaftMetadataNode` generics.
- [ ] Move `get_log_state` to `RaftLogStorage` in both stores.
- [ ] Resolve snapshot transfer (Route A or B); confirm required `RaftNetwork`
      methods from docs.rs.
- [ ] Implement `save_committed`/`read_committed` in the durable store.
- [ ] Replace any deprecated `is_leader()` calls; consider exposing
      `ensure_linearizable()`.
- [ ] Audit triggers / runtime-config usage.
- [ ] `Suite::test_all` green on both stores.
- [ ] Full workspace tests + the jepsen-style scenarios green.
- [ ] Grep Fibril/arbiter for `openraft::` re-export usage; recompile downstream.

Estimated effort: a focused 2-4 days, most of it in the storage signatures and the
snapshot-transfer decision.

---

# 0.9 -> 0.10 (PROVISIONAL -- verify at upgrade time)

**Status as of this writing:** 0.10 is alpha-only (`0.10.0-alpha.22`). There is **no
`upgrade_09_10` module** in openraft's docs yet, and the published change-log does not
cleanly surface the 0.10 deltas. Do **not** target 0.10 for a published Ganglion
crate; target 0.9.24. Treat this section as a sketch to revisit when 0.10 stabilizes
and an official guide exists.

How to derive the real list when the time comes:

1. Read `openraft::docs::upgrade_guide::upgrade_09_10` once it exists on docs.rs.
2. Bump to the latest `0.10.0-alpha` on a throwaway branch and let the compiler
   enumerate the breakage against the surface table above.
3. Re-run `Suite::test_all` as the correctness gate, same as the 0.9 migration.

Direction to expect (UNVERIFIED -- confirm before relying):

- **`NodeId` generic removal.** The long-signaled 0.10 direction is dropping the
  standalone `NodeId` type parameter in favor of generality over `C: RaftTypeConfig`,
  i.e. `LogId<NodeId>` -> `LogId<C>`, `Vote<NodeId>` -> `Vote<C>`,
  `StorageError<NodeId>` -> `StorageError<C>`, etc. If so, this is a sweeping but
  mechanical find-and-replace across `storage.rs`, `durable.rs`, `network.rs`,
  `tcp.rs`, and `node.rs` wherever `<NodeId>` appears on these types. Confirm the
  exact replacement parameter (`C` vs `GanglionRaftConfig`) from the alpha docs.
- **Possible `declare_raft_types!` shape change** (the macro changed in both 0.8->0.9
  and is a likely churn point again).
- **Possible storage callback rename** (e.g. `LogFlushed` -> an IO-completion type).
  Watch `append`'s callback parameter specifically.

Because the `NodeId` change (if it lands) is mechanical and the storage suite gates
it, 0.9 -> 0.10 should also be bounded -- but it is genuinely unknown until the
official guide lands. Re-verify everything in this section before acting on it.
