# Ganglion Plan

## Goal

Deliver a neutral, reusable embedded-consensus + metadata-planning library that supports
leader election workflows and similar coordination needs, while keeping `fibril` as a consumer
rather than a design driver.

## Active plan (non-versioned)

### 1) Openraft-backed metadata plane

- Keep the public `MetadataConsensus` trait stable and move in-memory internals toward a real
  openraft-backed runtime.
- Introduce a transport adapter module behind feature-gated optional dependency usage.
- Preserve existing persistence and WAL recovery semantics while replacing the placeholder execution path.

### 2) Pluggable planner strategies

- Keep `ganglion-core` planner APIs pure and deterministic.
- Add a minimal strategy registry and strategy trait for alternative planning policies.
- Keep strategy selection explicit in a small configurable surface so `fibril` can use defaults and
  override where needed.

### 3) Storage and durability path

- Keep storage abstraction stable (`MetadataLog`) and continue compatibility with file-backed and Keratin-backed
  adapters.
- Add missing parity and recovery-path gaps first in adapter surface tests, then in integration paths.
- Keep startup/recovery profile behavior explicit in constructor and diagnostics APIs.

### 4) API and validation work

- Maintain one mutable API reference file (`API.md`) for current public contracts.
- Keep one-shot validation (`scripts/validate.sh`) as the operational check entrypoint.
- Maintain reproducible proptest/Jepsen fallback paths and artifact capture so regressions are always runnable.

## Refinement notes (new or newly detailed items)

- Add a committed-snapshot publication surface (watcher/event stream) as a separate concern from consensus apply.
- Add explicit restart/failover regression coverage for control-loop continuity and multi-backend recovery ordering.
- Keep openraft feature usage optional and fully gated behind explicit Cargo features.
- Add a pluggable planner strategy registry and at least one alternate strategy implementation (`least-loaded`) with deterministic selector helpers.
- Seed a small feature-gated openraft runtime scaffold module now; keep behavior migration to real runtime incremental and backward compatible.

## Short-term roadmap

- Complete openraft transport-path replacement for the consensus adapter, while keeping `MetadataConsensus` semantics unchanged.
- Add pluggable strategy selection controls and keep the catalog discoverable for callers (`deterministic`, `least-loaded`).
- Finalize backend-aware scenario coverage for restart/failover sequencing (file + keratin path parity).

## Medium-term roadmap

- Publish committed snapshot/update events through a stable external observer interface.
- Add durability telemetry around append/clear/truncate and startup recovery outcomes.
- Expand partition/failover/follower reactivation scenarios into scripted reproducible paths.

## Long-term roadmap

- Expand backend adapters and retention/compaction tooling for durable metadata logs.
- Promote operator-oriented strategy/telemetry configuration without binding to a single domain model.
- Provide upgrade-friendly schema and migration hooks for persisted metadata snapshots and replay records.
