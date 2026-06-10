#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCENARIO_DIR="$ROOT_DIR/tests/jepsen/scenarios"
ARTIFACT_DIR="${ROOT_DIR}/tests/jepsen/artifacts"
mkdir -p "$ARTIFACT_DIR"

usage() {
  cat <<'USAGE'
Usage:
  tests/jepsen/run.sh list
  tests/jepsen/run.sh all [--artifact-dir PATH]
  tests/jepsen/run.sh scenario <scenario> [--artifact-dir PATH]

Examples:
  tests/jepsen/run.sh list
  tests/jepsen/run.sh all
  tests/jepsen/run.sh scenario baseline-failover
USAGE
}

run_scenario() {
  local scenario_path=$1
  local scenario_name=$2
  local log_path="$ARTIFACT_DIR/${scenario_name}.log"
  local summary_path="$ARTIFACT_DIR/${scenario_name}.json"
  local status="pass"
  local exit_code=0
  local invariant_json='[]'

  echo "jepsen-runner: starting $scenario_name"
  echo "jepsen-runner: log -> $log_path"

  set +e
  bash "$scenario_path" 2>&1 | tee "$log_path"
  exit_code=${PIPESTATUS[0]}
  set -e

  if [[ "$exit_code" -ne 0 ]]; then
    status="fail"
  fi

  if [[ -s "$log_path" ]]; then
    invariant_json="$({
      awk '
        /^expected invariants:/ { in_invariants = 1; next }
        in_invariants && /^ - / {
          sub(/^ - /, "", $0)
          print
        }
      ' "$log_path" | jq -R -s 'split("\n") | map(select(length > 0))'
    } )"
  fi

  jq -n \
    --arg scenario "$scenario_name" \
    --arg path "$scenario_path" \
    --arg status "$status" \
    --argjson exit_code "$exit_code" \
    --arg log "$log_path" \
    --argjson expected_invariants "$invariant_json" \
    '{
      "scenario": $scenario,
      "path": $path,
      "status": $status,
      "exit_code": $exit_code,
      "log": $log,
      "expected_invariants": $expected_invariants
    }' > "$summary_path"

  echo "jepsen-runner: summary -> $summary_path"

  return "$exit_code"
}

run_all() {
  local scenario_files=("$SCENARIO_DIR"/*.sh)
  local scenarios_jsons=()
  local failed_count=0
  local scenario_count=0

  for file in "${scenario_files[@]}"; do
    if [[ ! -x "$file" ]]; then
      continue
    fi

    name="$(basename "${file%.sh}")"
    scenarios_jsons+=("$ARTIFACT_DIR/${name}.json")
    scenario_count=$((scenario_count + 1))

    set +e
    run_scenario "$file" "$name"
    local status=$?
    set -e

    if [[ "$status" -ne 0 ]]; then
      failed_count=$((failed_count + 1))
    fi
  done

  if [[ ${#scenarios_jsons[@]} -eq 0 ]]; then
    jq -n \
      --arg runner "tests/jepsen/run.sh" \
      --arg mode "all" \
      --arg artifact_dir "$ARTIFACT_DIR" \
      '{
        "runner": $runner,
        "mode": $mode,
        "artifact_dir": $artifact_dir,
        "scenarios": []
      }' > "$ARTIFACT_DIR/run-summary.json"
    return
  fi

  jq -s \
    --arg runner "tests/jepsen/run.sh" \
    --arg mode "all" \
    --arg artifact_dir "$ARTIFACT_DIR" \
    --argjson failed_count "$failed_count" \
    --argjson scenario_count "$scenario_count" \
    '{
      "runner": $runner,
      "mode": $mode,
      "artifact_dir": $artifact_dir,
      "scenario_count": $scenario_count,
      "failed_scenarios": $failed_count,
      "scenarios": .
    }' \
    "${scenarios_jsons[@]}" > "$ARTIFACT_DIR/run-summary.json"

  return "$failed_count"
}

run_scenario_command() {
  local scenario="$1"
  local scenario_file=""

  for file in "$SCENARIO_DIR"/*.sh; do
    base="$(basename "${file%.sh}")"
    short="${base#*-}"
    short2="${short#*-}"
    if [[ "$base" == "$scenario" || "$short" == "$scenario" || "$short2" == "$scenario" ]]; then
      scenario_file="$file"
      break
    fi
  done

  if [[ -z "$scenario_file" ]]; then
    echo "Scenario not found or not executable: $scenario" >&2
    exit 1
  fi

  run_scenario "$scenario_file" "$scenario"
  jq -s \
    --arg runner "tests/jepsen/run.sh" \
    --arg mode "scenario" \
    --arg artifact_dir "$ARTIFACT_DIR" \
    '{
      "runner": $runner,
      "mode": $mode,
      "artifact_dir": $artifact_dir,
      "scenario_count": 1,
      "failed_scenarios": 0,
      "scenarios": .
    }' \
    "$ARTIFACT_DIR/${scenario}.json" > "$ARTIFACT_DIR/run-summary.json"
}

if [[ $# -lt 1 ]]; then
  usage
  exit 0
fi

case "$1" in
  list)
    for file in "$SCENARIO_DIR"/*.sh; do
      if [[ -x "$file" ]]; then
        basename "${file%.sh}"
      fi
    done
    ;;
  all)
    shift
    if [[ $# -ge 2 ]] && [[ "$1" == "--artifact-dir" ]]; then
      ARTIFACT_DIR="$2"
      mkdir -p "$ARTIFACT_DIR"
      shift 2
    fi

    run_all
    ;;
  scenario)
    if [[ $# -lt 2 ]]; then
      usage
      exit 1
    fi

    scenario="$2"
    shift 2
    if [[ $# -ge 2 ]] && [[ "$1" == "--artifact-dir" ]]; then
      ARTIFACT_DIR="$2"
      mkdir -p "$ARTIFACT_DIR"
      shift 2
    fi

    run_scenario_command "$scenario"
    ;;
  *)
    usage
    exit 1
    ;;
esac
