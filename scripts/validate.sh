#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JEPSEN_ARTIFACT_DIR="${ROOT_DIR}/tests/jepsen/artifacts/validate-run"
RUN_FMT=true
RUN_TESTS=true
RUN_STARTUP_SMOKE=true
RUN_PROPT=true
RUN_JEPSEN=true

PERSISTED_REPLAY_PROFILE_ENV="GANGLION_PERSISTED_REPLAY_PROFILE"
SUMMARY_FMT="skipped"
SUMMARY_TESTS="skipped"
SUMMARY_STARTUP_SMOKE="skipped"
SUMMARY_PROPT="skipped"
SUMMARY_JEPSEN="skipped"

usage() {
  cat <<'EOF'
Usage:
  scripts/validate.sh [options]

Options:
  --skip-fmt              skip cargo fmt --all --check
  --skip-tests            skip cargo test --workspace --quiet
  --skip-startup-smoke    skip persisted startup-entrypoint smoke check
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
    --skip-startup-smoke)
      RUN_STARTUP_SMOKE=false
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

run_startup_smoke() {
  echo "validate: cargo test -p ganglion-openraft persisted_node_startup --quiet"
  cargo test -p ganglion-openraft persisted_node_startup --quiet
  SUMMARY_STARTUP_SMOKE="pass"
}

run_proptest() {
  echo "validate: scripts/proptest.sh run"
  bash "$ROOT_DIR/scripts/proptest.sh" run
  SUMMARY_PROPT="pass"
}

run_jepsen() {
  mkdir -p "$JEPSEN_ARTIFACT_DIR"
  echo "validate: tests/jepsen/run.sh all --artifact-dir $JEPSEN_ARTIFACT_DIR"
  bash "$ROOT_DIR/tests/jepsen/run.sh" all --artifact-dir "$JEPSEN_ARTIFACT_DIR"
  SUMMARY_JEPSEN="pass"
}

write_summary() {
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

  cat <<EOF > "$summary_file"
{
  "script": "scripts/validate.sh",
  "workspace": "$ROOT_DIR",
  "jepsen_artifact_dir": "$JEPSEN_ARTIFACT_DIR",
  "replay_profile": {
    "env_var": "$PERSISTED_REPLAY_PROFILE_ENV",
    "value": "$replay_profile_raw",
    "effective": "$replay_profile_effective",
    "source": "$replay_profile_source"
  },
  "runs": {
    "fmt": {
      "requested": $RUN_FMT,
      "result": "$SUMMARY_FMT"
    },
    "tests": {
      "requested": $RUN_TESTS,
      "result": "$SUMMARY_TESTS"
    },
    "startup_smoke": {
      "requested": $RUN_STARTUP_SMOKE,
      "result": "$SUMMARY_STARTUP_SMOKE"
    },
    "proptest": {
      "requested": $RUN_PROPT,
      "result": "$SUMMARY_PROPT"
    },
    "jepsen": {
      "requested": $RUN_JEPSEN,
      "result": "$SUMMARY_JEPSEN"
    }
  }
}
EOF
  echo "validate: wrote summary to $summary_file"
}

if [[ "$RUN_FMT" == true ]]; then
  run_fmt
fi

if [[ "$RUN_TESTS" == true ]]; then
  run_tests
fi

if [[ "$RUN_STARTUP_SMOKE" == true ]]; then
  run_startup_smoke
fi

if [[ "$RUN_PROPT" == true ]]; then
  run_proptest
fi

if [[ "$RUN_JEPSEN" == true ]]; then
  run_jepsen
fi

mkdir -p "$JEPSEN_ARTIFACT_DIR"
write_summary

echo "validate: completed"
