#!/bin/bash
# CI entry point: build everything, run the full C++ regression suite plus
# the Python E2E tests, and summarize the result. Suitable for both local
# pre-push checks and unattended CI runners (GitHub Actions uses this exact
# script).
#
# Usage:
#   scripts/ci.sh              # full pipeline
#   scripts/ci.sh --quick      # skip the sanitizer pass (see sanitizer.sh)
#
# Exit code 0 only when every stage passes.

set -u

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SRC_DIR"

QUICK=0
for arg in "$@"; do
    case "$arg" in
        --quick) QUICK=1 ;;
        *) echo "ci.sh: unknown option $arg" >&2; exit 2 ;;
    esac
done

FAILED=0
declare -a STAGE_SUMMARY

run_stage() {
    local name="$1"; shift
    echo
    echo "=================================================================="
    echo "[ci] stage: $name"
    echo "=================================================================="
    local log="/tmp/ci_${name// /_}.log"
    if "$@" > "$log" 2>&1; then
        echo "[ci] $name: OK"
        STAGE_SUMMARY+=("PASS  $name")
    else
        echo "[ci] $name: FAILED (log: $log)"
        tail -30 "$log" >&2 || true
        STAGE_SUMMARY+=("FAIL  $name")
        FAILED=1
    fi
}

echo "[ci] DBMS v0.1.0 continuous integration"
echo "[ci] source: $SRC_DIR"
echo "[ci] start:  $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# 1. Clean production build (dbms_main).
run_stage "build" ./scripts/build.sh

# 2. Full C++ regression + Python E2E suite.
run_stage "regression" ./scripts/build_tests.sh

# 3. Version identity sanity (binary reports the tagged version).
if ./dbms_main --version >/dev/null 2>&1; then
    version_out="$(./dbms_main --version 2>/dev/null | awk '{print $2}')"
    expected="$(grep -oP '(?<=DBMS_VERSION_STRING ")[^"]+' src/common/version.h || true)"
    if [ -n "$version_out" ] && [ "$version_out" == "$expected" ]; then
        echo "[ci] version check: OK (dbms $version_out)"
        STAGE_SUMMARY+=("PASS  version-identity")
    else
        echo "[ci] version check: binary '$version_out' vs header '$expected'" >&2
        STAGE_SUMMARY+=("FAIL  version-identity")
        FAILED=1
    fi
else
    echo "[ci] dbms_main --version failed" >&2
    STAGE_SUMMARY+=("FAIL  version-identity")
    FAILED=1
fi

# 4. Sanitizer pass (skipped with --quick).
if [ "$QUICK" -ne 1 ]; then
    run_stage "sanitizer" ./scripts/sanitizer.sh --core
else
    STAGE_SUMMARY+=("SKIP  sanitizer (--quick)")
fi

echo
echo "=================================================================="
echo "[ci] summary"
echo "=================================================================="
for line in "${STAGE_SUMMARY[@]}"; do
    echo "  $line"
done
echo "[ci] end:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [ "$FAILED" -ne 0 ]; then
    echo "[ci] RESULT: FAILED" >&2
    exit 1
fi
echo "[ci] RESULT: PASSED"
exit 0
