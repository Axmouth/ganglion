#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATES=(ganglion-core ganglion-openraft)

usage() {
  cat <<'EOF'
Usage:
  scripts/proptest.sh run [--crate <name>] [cargo-test-args...]
  scripts/proptest.sh replay [--crate <name>] [<test-filter>]
  scripts/proptest.sh list

Examples:
  scripts/proptest.sh run
  scripts/proptest.sh run --crate ganglion-openraft
  scripts/proptest.sh replay ganglion-openraft fuzz_control_loop_publishing_and_rejection_matrix
EOF
}

crate_path() {
  local crate_name=$1
  for name in "${CRATES[@]}"; do
    if [[ "$name" == "$crate_name" ]]; then
      echo "$ROOT_DIR/crates/$name"
      return 0
    fi
  done
  return 1
}

run_fuzz_suite() {
  local crate_name=$1
  shift

  local path
  path="$(crate_path "$crate_name")"
  local regression_dir="$path/proptest-regressions"

  mkdir -p "$regression_dir"
  echo "Running proptest suite: $crate_name"
  PROPTEST_REGRESSION_DIR="$regression_dir" \
    cargo test -p "$crate_name" --all-features "$@" -- --test-threads=1
}

run_mode() {
  local mode=$1
  shift

  case "$mode" in
    list)
      printf "%s\n" "${CRATES[@]}"
      return 0
      ;;
    run)
      if [[ $# -gt 0 ]] && [[ "$1" == "--crate" ]]; then
        local crate_name=$2
        shift 2
        run_fuzz_suite "$crate_name" "$@"
        return 0
      fi

      for crate_name in "${CRATES[@]}"; do
        run_fuzz_suite "$crate_name" "$@"
      done
      ;;
    replay)
      if [[ $# -lt 1 ]]; then
        echo "replay requires a crate name." >&2
        usage
        exit 1
      fi

      local crate_name=$1
      shift
      local filter=${1-}

      if ! crate_path "$crate_name" >/dev/null; then
        echo "Unknown crate: $crate_name" >&2
        echo "Known crates: ${CRATES[*]}" >&2
        exit 1
      fi

      if [[ -n "$filter" ]]; then
        echo "Replaying with filter for $crate_name: $filter"
        PROPTEST_REGRESSION_DIR="$ROOT_DIR/crates/$crate_name/proptest-regressions" \
          cargo test -p "$crate_name" "$filter" -- --exact
      else
        echo "Replaying persisted failures for $crate_name"
        PROPTEST_REGRESSION_DIR="$ROOT_DIR/crates/$crate_name/proptest-regressions" \
          cargo test -p "$crate_name" -- --exact
      fi
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

if [[ $# -lt 1 ]]; then
  usage
  exit 0
fi

run_mode "$@"
