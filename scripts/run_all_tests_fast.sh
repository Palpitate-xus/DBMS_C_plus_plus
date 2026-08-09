#!/usr/bin/env bash
# Quiet facade for the canonical full test runner.
#
# Keep all compilation, linking, stub selection, cache invalidation and E2E
# scheduling in build_tests.sh.  This entry point only changes presentation:
# successful runs get a compact count, while failures print the complete
# diagnostic log.

set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/dbms-tests.XXXXXX")"

cleanup() {
    rm -f "$LOG_FILE"
}
trap cleanup EXIT

if "${SRC_DIR}/scripts/build_tests.sh" >"$LOG_FILE" 2>&1; then
    pass_count="$(grep -cE ' PASSED$' "$LOG_FILE" || true)"
    echo "PASS=${pass_count} FAIL=0"
    exit 0
fi

cat "$LOG_FILE"
echo "PASS=unknown FAIL=1"
exit 1
