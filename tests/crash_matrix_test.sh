#!/bin/bash
# Crash-recovery matrix test for DBMS_C_plus_plus.
#
# Spawns real dbms_main processes, drives them through SQL piped on stdin,
# and SIGKILLs them at defined moments.  Every case then verifies that a
# successor process can open the database, that committed rows are present,
# and that uncommitted work is absent (atomicity) — the crash-recovery
# contract.
#
# Matrix axes:
#   workload : wal-insert           — plain INSERTs inside BEGIN..COMMIT
#              tde-insert           — same under a TDE keyring
#              ddl-mixed            — DDL + DML interleaved
#              connection-pool      — pool-mode server-style session churn
#   kill at  : mid-transaction      — killed with the txn open
#              post-commit          — killed right after COMMIT output
#              post-checkpoint      — killed after CHECKPOINT
#
# Usage: tests/crash_matrix_test.sh [build-dir]
# Exit 0 only when every combination passes.

set -u

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DBMS_MAIN="${1:-${SRC_DIR}/dbms_main}"

if [ ! -x "$DBMS_MAIN" ]; then
    echo "[crash-matrix] dbms_main not found at $DBMS_MAIN" >&2
    exit 2
fi

WORK="$(mktemp -d /tmp/dbms_crash_matrix.XXXXXX)"
trap 'kill -9 ${CHILD_PIDS[@]:-} 2>/dev/null; rm -rf "$WORK"' EXIT

PASS=0
FAIL=0
declare -a RESULTS
CHILD_PIDS=()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

setup_auth() {
    mkdir -p "$1/info/pg_catalog"
    python3 - "$1" <<'PYEOF'
import base64, hashlib, hmac, sys
root = sys.argv[1]
def verifier(password, salt=b"0123456789abcdef", iterations=4096):
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    ck = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    sk = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    return "SCRAM-SHA-256$%d:%s$%s:%s" % (iterations, base64.b64encode(salt).decode(), base64.b64encode(hashlib.sha256(ck).digest()).decode(), base64.b64encode(sk).decode())
import os
os.makedirs(os.path.join(root, "info/pg_catalog"), exist_ok=True)
open(os.path.join(root, "info/pg_catalog/pg_authid.cat"), "w").write(
    '10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n' % verifier("admin"))
open(os.path.join(root, "info/tlist.lst"), "wb").close()
PYEOF
}

# spawn_sql <workdir> <sql-file> — run dbms_main reading SQL from a fifo so
# we control pacing and can kill at a chosen moment.
spawn_sql() {
    local dir="$1" sqlfile="$2"
    mkfifo "$dir/in.fifo" 2>/dev/null || true
    ( cd "$dir" && "$DBMS_MAIN" < "$dir/in.fifo" > "$dir/out.log" 2>&1 ) &
    CHILD_PIDS+=($!)
    # Keep the fifo writer end open so dbms_main doesn't see EOF.
    exec 9>"$dir/in.fifo"
    sleep 0.5
}

feed() { printf '%s\n' "$1" >&9; }

kill_dbms() {
    local pid="$1"
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    exec 9>&- 2>/dev/null || true
}

# verify <workdir> <db> <must-contain-sql-rows> <must-not-contain-id>
# Runs a fresh dbms_main and asserts row visibility.
verify() {
    local dir="$1" db="$2" want="$3" unwant="$4"
    local result
    result="$(cd "$dir" && printf 'admin admin\nUSE DATABASE %s;\nSELECT id, payload FROM crash_t ORDER BY id;\nexit\n' "$db" \
              | timeout 60 "$DBMS_MAIN" 2>&1)"
    local rc=$?
    if [ $rc -ne 0 ]; then
        echo "[crash-matrix] verify: restart process failed rc=$rc"
        echo "$result" | tail -5
        return 1
    fi
    if [ -n "$want" ] && ! echo "$result" | grep -q "$want"; then
        echo "[crash-matrix] verify: committed row missing (wanted '$want')"
        echo "$result" | tail -5
        return 1
    fi
    if [ -n "$unwant" ] && echo "$result" | grep -q "$unwant"; then
        echo "[crash-matrix] verify: uncommitted row leaked ('$unwant')"
        echo "$result" | tail -5
        return 1
    fi
    return 0
}

run_case() {
    local workload="$1" killat="$2"
    local tag="${workload}+${killat}"
    local dir="$WORK/${workload}_${killat}"
    mkdir -p "$dir"
    setup_auth "$dir"

    # TDE workload arms the keyring via dbms.conf.
    if [ "$workload" == "tde-insert" ]; then
        echo "tde_keyring=${dir}/master.key" > "$dir/dbms.conf"
    fi
    # Connection-pool workload runs with pooling armed.
    if [ "$workload" == "connection-pool" ]; then
        printf 'pool_mode=transaction\npool_size=4\n' > "$dir/dbms.conf"
    fi

    local setup_sql='admin admin
CREATE DATABASE crashdb;
USE DATABASE crashdb;
CREATE TABLE crash_t (id INT PRIMARY KEY, payload VARCHAR(64));'
    (cd "$dir" && printf '%s\nexit\n' "$setup_sql" | timeout 60 "$DBMS_MAIN" > /dev/null 2>&1) || {
        RESULTS+=("FAIL $tag (setup)"); FAIL=$((FAIL+1)); return; }

    # Drive the workload with pacing, then kill.
    local pid
    mkfifo "$dir/in.fifo" 2>/dev/null || true
    ( cd "$dir" && "$DBMS_MAIN" < "$dir/in.fifo" > "$dir/out.log" 2>&1 ) &
    pid=$!
    CHILD_PIDS+=($pid)
    exec 9>"$dir/in.fifo"
    sleep 0.6
    printf 'admin admin\nUSE DATABASE crashdb\n' >&9
    sleep 0.4

    case "$workload" in
        wal-insert|tde-insert|connection-pool)
            printf 'BEGIN\n' >&9; sleep 0.2
            printf 'INSERT INTO crash_t VALUES (1, %s)\n' "'committed-early'" >&9; sleep 0.2
            printf 'COMMIT\n' >&9; sleep 0.5
            ;;
        ddl-mixed)
            printf 'CREATE INDEX crash_idx ON crash_t (payload)\n' >&9; sleep 0.4
            printf 'BEGIN\n' >&9; sleep 0.2
            printf 'INSERT INTO crash_t VALUES (1, %s)\n' "'committed-early'" >&9; sleep 0.2
            printf 'COMMIT\n' >&9; sleep 0.5
            ;;
    esac

    case "$killat" in
        mid-transaction)
            printf 'BEGIN\n' >&9; sleep 0.2
            printf 'INSERT INTO crash_t VALUES (2, %s)\n' "'uncommitted-must-vanish'" >&9; sleep 0.3
            ;;
        post-commit)
            printf 'BEGIN\n' >&9; sleep 0.2
            printf 'INSERT INTO crash_t VALUES (2, %s)\n' "'committed-late'" >&9; sleep 0.2
            printf 'COMMIT\n' >&9
            sleep 1.0   # let the commit land + flush
            ;;
        post-checkpoint)
            printf 'BEGIN\n' >&9; sleep 0.2
            printf 'INSERT INTO crash_t VALUES (2, %s)\n' "'committed-late'" >&9; sleep 0.2
            printf 'COMMIT\n' >&9; sleep 0.5
            printf 'CHECKPOINT\n' >&9; sleep 0.8
            ;;
    esac

    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    exec 9>&- 2>/dev/null
    rm -f "$dir/in.fifo"

    # Verify with a successor process.
    local want unwant
    want="committed-early"
    case "$killat" in
        mid-transaction) unwant="uncommitted-must-vanish" ;;
        *)                unwant="" ; want="committed-late" ;;
    esac

    if verify "$dir" crashdb "$want" "$unwant"; then
        RESULTS+=("PASS $tag")
        PASS=$((PASS+1))
    else
        RESULTS+=("FAIL $tag (verify)")
        FAIL=$((FAIL+1))
        tail -8 "$dir/out.log" 2>/dev/null | sed 's/^/    | /'
    fi
}

echo "[crash-matrix] dbms_main: $DBMS_MAIN"
echo "[crash-matrix] workdir:   $WORK"
echo

for workload in wal-insert tde-insert ddl-mixed connection-pool; do
    for killat in mid-transaction post-commit post-checkpoint; do
        echo "[crash-matrix] --- $workload / kill at $killat"
        run_case "$workload" "$killat"
    done
done

echo
echo "=================================================================="
echo "[crash-matrix] summary"
echo "=================================================================="
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo "[crash-matrix] PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
