#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JEPSEN_ARTIFACT_DIR="${ROOT_DIR}/tests/jepsen/artifacts/validate-run"
RUN_FMT=true
RUN_TESTS=true
RUN_STORAGE_PARITY=true
RUN_STARTUP_SMOKE=true
RUN_RAFT_RUNTIME=true
RUN_PROPT=true
RUN_JEPSEN=true

PERSISTED_REPLAY_PROFILE_ENV="GANGLION_PERSISTED_REPLAY_PROFILE"
SUMMARY_FMT="skipped"
SUMMARY_TESTS="skipped"
SUMMARY_STORAGE_PARITY="skipped"
SUMMARY_STARTUP_SMOKE="skipped"
SUMMARY_RAFT_RUNTIME="skipped"
SUMMARY_PROPT="skipped"
SUMMARY_JEPSEN="skipped"

usage() {
  cat <<'EOF'
Usage:
  scripts/validate.sh [options]

Options:
  --skip-fmt              skip cargo fmt --all --check
  --skip-tests            skip cargo test --workspace --quiet
  --skip-storage-parity   skip storage parity + startup constructor smoke
  --skip-startup-smoke    skip persisted startup-entrypoint smoke check
  --skip-raft-runtime     skip openraft runtime feature tests
  --skip-fuzz             skip scripts/proptest.sh run
  --skip-jepsen           skip tests/jepsen/run.sh all
  --jepsen-artifact-dir P artifacts directory for jepsen scenario logs
  -h, --help             show this help
  
Environment:
  GANGLION_PERSISTED_REPLAY_PROFILE
    - default
    - strict
    - resilient
    - tail:<n>
    - truncate_tail:<n>
    - <n>
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-fmt)
      RUN_FMT=false
      shift
      ;;
    --skip-tests)
      RUN_TESTS=false
      shift
      ;;
    --skip-storage-parity)
      RUN_STORAGE_PARITY=false
      shift
      ;;
    --skip-startup-smoke)
      RUN_STARTUP_SMOKE=false
      shift
      ;;
    --skip-raft-runtime)
      RUN_RAFT_RUNTIME=false
      shift
      ;;
    --skip-fuzz)
      RUN_PROPT=false
      shift
      ;;
    --skip-jepsen)
      RUN_JEPSEN=false
      shift
      ;;
    --jepsen-artifact-dir)
      if [[ $# -lt 2 ]]; then
        echo "Missing path for --jepsen-artifact-dir" >&2
        exit 1
      fi
      JEPSEN_ARTIFACT_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

run_fmt() {
  echo "validate: cargo fmt --all --check"
  cargo fmt --all --check
  SUMMARY_FMT="pass"
}

run_tests() {
  echo "validate: cargo test --workspace --quiet"
  cargo test --workspace --quiet
  SUMMARY_TESTS="pass"
}

run_storage_parity() {
  echo "validate: bash scripts/storage-parity.sh"
  bash "$ROOT_DIR/scripts/storage-parity.sh"
  SUMMARY_STORAGE_PARITY="pass"
}

run_startup_smoke() {
  echo "validate: cargo test -p ganglion-openraft persisted_node_startup --quiet"
  cargo test -p ganglion-openraft persisted_node_startup --quiet
  SUMMARY_STARTUP_SMOKE="pass"
}

run_raft_runtime() {
  echo "validate: cargo test -p ganglion-openraft --features openraft --quiet"
  cargo test -p ganglion-openraft --features openraft --quiet
  SUMMARY_RAFT_RUNTIME="pass"
}

run_proptest() {
  echo "validate: scripts/proptest.sh run"
  bash "$ROOT_DIR/scripts/proptest.sh" run
  SUMMARY_PROPT="pass"
}

run_jepsen() {
  mkdir -p "$JEPSEN_ARTIFACT_DIR"
  echo "validate: tests/jepsen/run.sh all --artifact-dir $JEPSEN_ARTIFACT_DIR"
  local jepsen_rc=0

  set +e
  bash "$ROOT_DIR/tests/jepsen/run.sh" all --artifact-dir "$JEPSEN_ARTIFACT_DIR"
  jepsen_rc=$?
  set -e

  if [[ "$jepsen_rc" -eq 0 ]]; then
    SUMMARY_JEPSEN="pass"
  else
    SUMMARY_JEPSEN="fail"
  fi
}

verify_jepsen_artifacts() {
  local summary_file="$JEPSEN_ARTIFACT_DIR/run-summary.json"
  if [[ ! -f "$summary_file" ]]; then
    echo "validate: missing jepsen summary artifact: $summary_file"
    return 1
  fi

  local scenario_count
  local entry_count
  scenario_count="$(jq -r '.scenario_count // (.scenarios | length)' "$summary_file")"
  entry_count="$(jq -r '.scenarios | length' "$summary_file")"
  local failed_count
  failed_count="$(jq -r '[.scenarios[] | select(.status != "pass")] | length' "$summary_file")"

  if [[ "$scenario_count" -ne "$entry_count" ]]; then
    echo "validate: inconsistent scenario-count metadata in $summary_file"
    return 1
  fi

  if [[ "$failed_count" -gt 0 && "$RUN_JEPSEN" == "true" ]]; then
    echo "validate: jepsen run-summary reports failing scenarios in $summary_file"
    return 1
  fi

  if [[ "$entry_count" -eq 0 ]]; then
    echo "validate: jepsen summary has no scenario entries: $summary_file"
    return 1
  fi

  local missing=0
  while IFS= read -r scenario_name; do
    if [[ -z "$scenario_name" ]]; then
      continue
    fi

    local scenario_file="$JEPSEN_ARTIFACT_DIR/${scenario_name}.json"
    if [[ ! -f "$scenario_file" ]]; then
      echo "validate: missing scenario summary file: $scenario_file"
      missing=1
    fi
  done < <(jq -r '.scenarios[].scenario // empty' "$summary_file")

  while IFS= read -r log_path; do
    if [[ -z "$log_path" ]]; then
      continue
    fi

    if [[ ! -f "$log_path" ]]; then
      echo "validate: missing scenario log file: $log_path"
      missing=1
    fi
  done < <(jq -r '.scenarios[].log // empty' "$summary_file")

  if [[ "$missing" -eq 1 ]]; then
    return 1
  fi

  return 0
}

write_summary() {
  local jepsen_summary_file="${JEPSEN_ARTIFACT_DIR}/run-summary.json"
  local jepsen_run_summary='null'
  local jepsen_total_scenarios='0'
  local jepsen_failed_scenarios='0'

  if [[ -f "$jepsen_summary_file" ]]; then
    jepsen_run_summary="$(cat "$jepsen_summary_file")"
    jepsen_total_scenarios="$(jq -r '.scenarios | length' "$jepsen_summary_file")"
    jepsen_failed_scenarios="$(jq -r '[.scenarios[] | select(.status != "pass")] | length' "$jepsen_summary_file")"
  fi

  local summary_file="$JEPSEN_ARTIFACT_DIR/validate-summary.json"
  local replay_profile_raw="${GANGLION_PERSISTED_REPLAY_PROFILE:-<unset>}"
  local replay_profile_effective="$replay_profile_raw"
  if [[ "$replay_profile_effective" == "<unset>" ]]; then
    replay_profile_effective="default"
  fi
  local replay_profile_source="default"
  if [[ -n "${!PERSISTED_REPLAY_PROFILE_ENV+x}" ]]; then
    replay_profile_source="environment"
  fi

  jq -n \
    --arg script "scripts/validate.sh" \
    --arg workspace "$ROOT_DIR" \
    --arg jepsen_artifact_dir "$JEPSEN_ARTIFACT_DIR" \
    --argjson fmt_requested "$RUN_FMT" \
    --arg result_fmt "$SUMMARY_FMT" \
    --argjson tests_requested "$RUN_TESTS" \
    --arg result_tests "$SUMMARY_TESTS" \
    --argjson storage_parity_requested "$RUN_STORAGE_PARITY" \
    --arg result_storage_parity "$SUMMARY_STORAGE_PARITY" \
    --argjson startup_smoke_requested "$RUN_STARTUP_SMOKE" \
    --arg result_startup_smoke "$SUMMARY_STARTUP_SMOKE" \
    --argjson raft_runtime_requested "$RUN_RAFT_RUNTIME" \
    --arg result_raft_runtime "$SUMMARY_RAFT_RUNTIME" \
    --argjson proptest_requested "$RUN_PROPT" \
    --arg result_proptest "$SUMMARY_PROPT" \
    --argjson jepsen_requested "$RUN_JEPSEN" \
    --arg result_jepsen "$SUMMARY_JEPSEN" \
    --arg replay_env_var "$PERSISTED_REPLAY_PROFILE_ENV" \
    --arg replay_raw "$replay_profile_raw" \
    --arg replay_effective "$replay_profile_effective" \
    --arg replay_source "$replay_profile_source" \
    --argjson jepsen_total "$jepsen_total_scenarios" \
    --argjson jepsen_failed "$jepsen_failed_scenarios" \
    --argjson jepsen_summary "$jepsen_run_summary" \
    '{
      "script": $script,
      "workspace": $workspace,
      "jepsen_artifact_dir": $jepsen_artifact_dir,
      "replay_profile": {
        "env_var": $replay_env_var,
        "value": $replay_raw,
        "effective": $replay_effective,
        "source": $replay_source
      },
      "runs": {
      "fmt": {
          "requested": $fmt_requested,
          "result": $result_fmt
        },
        "tests": {
          "requested": $tests_requested,
          "result": $result_tests
        },
        "storage_parity": {
          "requested": $storage_parity_requested,
          "result": $result_storage_parity,
          "backends": ["file","keratin"],
          "startup_replay_profile": {
            "env_var": $replay_env_var,
            "value": $replay_raw,
            "effective": $replay_effective,
            "source": $replay_source
          }
        },
        "startup_smoke": {
          "requested": $startup_smoke_requested,
          "result": $result_startup_smoke
        },
        "raft_runtime": {
          "requested": $raft_runtime_requested,
          "result": $result_raft_runtime
        },
        "proptest": {
          "requested": $proptest_requested,
          "result": $result_proptest
        },
        "jepsen": {
          "requested": $jepsen_requested,
          "result": $result_jepsen,
          "summary_file": ($jepsen_artifact_dir + "/run-summary.json"),
          "total_scenarios": $jepsen_total,
          "failed_scenarios": $jepsen_failed,
          "scenarios": ($jepsen_summary | .scenarios? // [])
        }
      }
    }' > "$summary_file"
  echo "validate: wrote summary to $summary_file"
}

if [[ "$RUN_FMT" == true ]]; then
  run_fmt
fi

if [[ "$RUN_TESTS" == true ]]; then
  run_tests
fi

if [[ "$RUN_STORAGE_PARITY" == true ]]; then
  run_storage_parity
fi

if [[ "$RUN_STARTUP_SMOKE" == true ]]; then
  run_startup_smoke
fi

if [[ "$RUN_RAFT_RUNTIME" == true ]]; then
  run_raft_runtime
fi

if [[ "$RUN_PROPT" == true ]]; then
  run_proptest
fi

if [[ "$RUN_JEPSEN" == true ]]; then
  run_jepsen
  if ! verify_jepsen_artifacts; then
    SUMMARY_JEPSEN="fail"
  fi
fi

mkdir -p "$JEPSEN_ARTIFACT_DIR"
write_summary

if [[ "$RUN_FMT" == true && "$SUMMARY_FMT" != "pass" ]]; then
  echo "validate: fmt check failed"
  exit 1
fi

if [[ "$RUN_TESTS" == true && "$SUMMARY_TESTS" != "pass" ]]; then
  echo "validate: tests failed"
  exit 1
fi

if [[ "$RUN_STORAGE_PARITY" == true && "$SUMMARY_STORAGE_PARITY" != "pass" ]]; then
  echo "validate: storage parity failed"
  exit 1
fi

if [[ "$RUN_STARTUP_SMOKE" == true && "$SUMMARY_STARTUP_SMOKE" != "pass" ]]; then
  echo "validate: startup smoke failed"
  exit 1
fi

if [[ "$RUN_RAFT_RUNTIME" == true && "$SUMMARY_RAFT_RUNTIME" != "pass" ]]; then
  echo "validate: raft runtime tests failed"
  exit 1
fi

if [[ "$RUN_PROPT" == true && "$SUMMARY_PROPT" != "pass" ]]; then
  echo "validate: proptest failed"
  exit 1
fi

if [[ "$RUN_JEPSEN" == true && "$SUMMARY_JEPSEN" != "pass" ]]; then
  echo "validate: jepsen artifacts validation failed"
  exit 1
fi

echo "validate: completed"
