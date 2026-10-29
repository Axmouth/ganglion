# Jepsen Scenarios for Ganglion

This directory holds the runnable Jepsen harness surface for this repo.
Scenario scripts are commandable and produce logs under `tests/jepsen/artifacts`.

## Layout

- `README.md` (this file): scenario intent and mapping to expected checks.
- `run.sh`: CLI entrypoint to list scenarios or run one/all.
- `scenarios/`: runnable shell wrappers for Jepsen scenario definitions.
- `artifacts/`: captured run logs and CI summaries.
- `scripts/` (repo root): `scripts/jepsen.sh` is the CI-friendly entrypoint.

## Running

Use:

- `tests/jepsen/run.sh list`
- `tests/jepsen/run.sh all`
- `tests/jepsen/run.sh scenario baseline-failover`
- `scripts/jepsen.sh all` (CI-targetable wrapper).
- `scripts/validate.sh` (runs fmt, tests, proptests, and all Jepsen scenarios).
  It writes `validate-summary.json` under the chosen artifact directory with run and replay-profile metadata.
  Replay-profile metadata includes:
  - `value` (raw env var if set)
  - `effective` (resolved value)
  - `source` (`environment` if env var was set, otherwise `default`).

`scripts/validate.sh` is the preferred one-shot local/CI validation path.  
It accepts:
- `--skip-fmt`
- `--skip-tests`
- `--skip-startup-smoke`
- `--skip-fuzz`
- `--skip-jepsen`
- `--jepsen-artifact-dir <path>`

Environment variable:
- `GANGLION_PERSISTED_REPLAY_PROFILE` (resolved default is `default`).

No Jepsen/Clojure harness is wired yet. Every scenario runs a fallback set of
focused Rust tests (see the inventory below), whether or not `clojure` is on
`PATH`, and writes a log file so orchestration can continue.

`validate-summary.json` also includes `startup_smoke` with `requested` and `result` fields (`pass`/`skipped`/`fail` behavior), aligned with other run phases.

### Hang triage

If a broad `scripts/validate.sh` or `tests/jepsen/run.sh all` invocation appears to stall, use this sequence:

- Start with a focused scenario run:
  - `tests/jepsen/run.sh scenario <scenario-name>`
- Confirm artifacts are produced under the chosen artifact directory.
- Re-run the full aggregate only after checking for orphaned background invocations from prior runs.

### Scenario summary precondition

- `tests/jepsen/run.sh all` always writes:
  - `<scenario>.json` for each executable scenario
  - `run-summary.json` with `scenario_count` and `failed_scenarios`
- `scripts/validate.sh` treats missing/partial Jepsen artifacts as a run failure in `validate-summary.json`.
- Practical sequence before rerunning aggregate:
  1. `tests/jepsen/run.sh scenario <name>` and confirm `<name>.json` and `<name>.log` appear.
  2. `ls <artifact-dir>/run-summary.json` and confirm it includes all scenario entries.
  3. Re-run full aggregate only once the precondition passes.

## Scenario inventory

The fallback scenarios exercise both node families: the legacy `InMemoryMetadataNode` and
`PersistedMetadataNode` (01-06) and the openraft runtime `RaftMetadataNode` (07-08). Each script
prints its expected invariants, which `run.sh` copies into the scenario summary.

- `01-baseline-failover.sh`: control-loop publishing over persisted and in-memory nodes.
  Accepted proposals stay monotonic in committed generation, followers reject stale writes from
  non-leaders, and rejected proposals publish nothing.
- `02-partition-split-brain.sh`: generated publish and rejection matrices. Only one leader's
  writes persist per generation, conflicting generations are never both accepted, and term and
  generation rejections hold.
- `03-crash-recovery.sh`: restart applies the term bump, stale terms are rejected, and corrupt
  or non-sequential file logs are rejected.
- `04-startup-entrypoint-smoke.sh`: every persisted startup constructor runs and resolves its
  replay profile deterministically.
- `05-startup-policy-matrix.sh`: profile precedence is explicit over environment over default,
  and strict, default and tail profiles behave differently on malformed tails.
- `06-persistence-backend-parity.sh`: file and Keratin metadata logs match on bounded-tail
  recovery, strict mode rejects non-sequential boundaries, and restart and failover ordering
  rejects stale terms after a higher-term write. Runs the storage tests with `--features keratin`.
- `07-raft-runtime-failover.sh`: the openraft runtime. A 3-node cluster elects, replicates and
  rejects stale generations after consensus, survivors re-elect after leader loss, a partitioned
  follower rejoins and converges, a durable node recovers from its WAL after restart, a learner
  joins and is promoted, and the file log store passes the openraft contract suite and survives
  reopen after vote, log and purge records.
- `08-playground-smoke.sh`: scripted run of `scripts/cluster-playground.sh` through write, kill,
  write during the outage, restart, add and promote, remove, final write and quit. Each step's
  output is asserted and the playground must exit cleanly within 180 s.

## Artifact contract

- Each scenario writes `<scenario>.log` and `<scenario>.json` under the artifact directory
  (default `tests/jepsen/artifacts/`, override with `--artifact-dir`).
- `<scenario>.json` holds `scenario`, `path`, `status` (`pass` or `fail`), `exit_code`, `log` and
  `expected_invariants`.
- `run-summary.json` is written for both `all` and `scenario` runs and aggregates the scenario
  summaries with `scenario_count` and `failed_scenarios`.
- `scripts/validate.sh` reads these into `validate-summary.json` and fails the run when they are
  missing or partial.

## Exit checks

These hold for any scenario added here, fallback or a future Jepsen harness:

- No test treats a new follower start as a no-op publish path.
- Planner output after every accepted snapshot is deterministically reproducible.
- Consensus-layer rejection paths are exercised with generated inputs, not only fixed fixtures.
