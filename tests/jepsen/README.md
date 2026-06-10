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

`scripts/validate.sh` is the preferred one-shot local/CI validation path.  
It accepts:
- `--skip-fmt`
- `--skip-tests`
- `--skip-fuzz`
- `--skip-jepsen`
- `--jepsen-artifact-dir <path>`

Environment variable:
- `GANGLION_PERSISTED_REPLAY_PROFILE` (resolved default is `default`).

If Clojure/Jepsen is not available in the environment, scenario scripts will emit
`SKIPPED` and still write a log file so orchestration can continue.

## Scenario inventory

- `01-baseline-failover.sh`
- `02-partition-split-brain.sh`
- `03-crash-recovery.sh`

## Current status

- Scenario orchestration is available with a local fallback that runs focused Rust smoke checks when a Jepsen/Clojure runtime is unavailable.
- Placeholder harness hooks remain for future direct Jepsen wiring.
