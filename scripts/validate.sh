#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JEPSEN_ARTIFACT_DIR="${ROOT_DIR}/tests/jepsen/artifacts/validate-run"
RUN_FMT=true
RUN_TESTS=true
RUN_PROPT=true
RUN_JEPSEN=true

usage() {
  cat <<'EOF'
Usage:
  scripts/validate.sh [options]

Options:
  --skip-fmt              skip cargo fmt --all --check
  --skip-tests            skip cargo test --workspace --quiet
  --skip-fuzz             skip scripts/proptest.sh run
  --skip-jepsen           skip tests/jepsen/run.sh all
  --jepsen-artifact-dir P artifacts directory for jepsen scenario logs
  -h, --help             show this help
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
}

run_tests() {
  echo "validate: cargo test --workspace --quiet"
  cargo test --workspace --quiet
}

run_proptest() {
  echo "validate: scripts/proptest.sh run"
  bash "$ROOT_DIR/scripts/proptest.sh" run
}

run_jepsen() {
  mkdir -p "$JEPSEN_ARTIFACT_DIR"
  echo "validate: tests/jepsen/run.sh all --artifact-dir $JEPSEN_ARTIFACT_DIR"
  bash "$ROOT_DIR/tests/jepsen/run.sh" all --artifact-dir "$JEPSEN_ARTIFACT_DIR"
}

if [[ "$RUN_FMT" == true ]]; then
  run_fmt
fi

if [[ "$RUN_TESTS" == true ]]; then
  run_tests
fi

if [[ "$RUN_PROPT" == true ]]; then
  run_proptest
fi

if [[ "$RUN_JEPSEN" == true ]]; then
  run_jepsen
fi

echo "validate: completed"
