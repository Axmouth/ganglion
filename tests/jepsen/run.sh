#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCENARIO_DIR="$ROOT_DIR/tests/jepsen/scenarios"
ARTIFACT_DIR="${ROOT_DIR}/tests/jepsen/artifacts"
mkdir -p "$ARTIFACT_DIR"

usage() {
  cat <<'EOF'
Usage:
  tests/jepsen/run.sh list
  tests/jepsen/run.sh all [--artifact-dir PATH]
  tests/jepsen/run.sh scenario <scenario> [--artifact-dir PATH]

Examples:
  tests/jepsen/run.sh list
  tests/jepsen/run.sh all
  tests/jepsen/run.sh scenario baseline-failover
EOF
}

run_scenario() {
  local scenario_path=$1
  local scenario_name=$2
  local log_path="$ARTIFACT_DIR/${scenario_name}.log"

  echo "jepsen-runner: starting $scenario_name"
  echo "jepsen-runner: log -> $log_path"
  bash "$scenario_path" 2>&1 | tee "$log_path"
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

    for file in "$SCENARIO_DIR"/*.sh; do
      if [[ -x "$file" ]]; then
        name="$(basename "${file%.sh}")"
        run_scenario "$file" "$name"
      fi
    done
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

    scenario_file=""
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
    ;;
  *)
    usage
    exit 1
    ;;
esac
