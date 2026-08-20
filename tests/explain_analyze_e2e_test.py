#!/usr/bin/env python3
"""E2E test for EXPLAIN ANALYZE per-node actuals and BUFFERS.

Verifies through the real SQL shell:
  - EXPLAIN (ANALYZE) prints PG-style "(actual time=... rows=... loops=...)"
    per plan node
  - plain EXPLAIN prints no actuals
  - EXPLAIN (ANALYZE, BUFFERS) adds a "Buffers:" delta line
  - total runtime and actual row count are reported

Runs dbms_main in a temp working directory with a SCRAM admin account.
"""
import os
import base64
import hashlib
import hmac
import subprocess
import shutil
import sys
import tempfile

DBMS_MAIN = os.path.join(os.path.dirname(__file__), "..", "dbms_main")
COMMAND_TIMEOUT = float(os.environ.get("DBMS_EXPLAIN_TEST_TIMEOUT", "60"))


def scram_verifier(password, salt=b"0123456789abcdef", iterations=4096):
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    return (
        "SCRAM-SHA-256$%d:%s$%s:%s"
        % (iterations, base64.b64encode(salt).decode(),
           base64.b64encode(stored_key).decode(),
           base64.b64encode(server_key).decode())
    )


def run_sql(sql):
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    script = "admin admin\n" + "\n".join(statements) + "\nexit\n"
    proc = subprocess.run(
        [DBMS_MAIN],
        input=script,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT,
    )
    return proc.stdout, proc.stderr, proc.returncode


def section(stdout, marker):
    """Split output at lines containing marker; return text after it."""
    lines = stdout.splitlines()
    for i, line in enumerate(lines):
        if marker in line:
            return "\n".join(lines[i:])
    return ""


def main():
    if not os.path.exists(DBMS_MAIN):
        print(f"ERROR: {DBMS_MAIN} not found; run scripts/build.sh first")
        return 1

    work_dir = tempfile.mkdtemp(prefix="dbms-explain-analyze-")
    try:
        os.chdir(work_dir)
        os.makedirs(os.path.join("info", "pg_catalog"), exist_ok=True)
        with open(os.path.join("info", "pg_catalog", "pg_authid.cat"), "w",
                  encoding="utf-8") as auth:
            auth.write('10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n'
                       % scram_verifier("admin"))
        open(os.path.join("info", "tlist.lst"), "wb").close()

        out, err, rc = run_sql(
            "CREATE DATABASE etest; "
            "USE DATABASE etest; "
            "CREATE TABLE t (id INT, v INT); "
            "INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5)"
        )
        if rc != 0:
            print("setup failed:", out, err)
            return 1

        passed = failed = 0

        def check(name, cond, ctx=""):
            nonlocal passed, failed
            if cond:
                passed += 1
                print(f"[EXPLAIN-ANALYZE] {name} OK")
            else:
                failed += 1
                print(f"[EXPLAIN-ANALYZE] {name} FAIL")
                if ctx:
                    print("  ---- got ----")
                    print(ctx)

        # 1. ANALYZE shows per-node actuals
        out, _, rc = run_sql(
            "USE DATABASE etest; EXPLAIN ANALYZE SELECT * FROM t WHERE v < 3")
        ana = section(out, "ANALYZE")
        check("analyze prints actuals", "actual time=" in ana, out)
        check("actuals have loops", "loops=" in ana, ana)
        check("actual rows reported", "Actual rows: 2" in out, out)

        # 2. plain EXPLAIN has no actuals
        out, _, _ = run_sql(
            "USE DATABASE etest; EXPLAIN SELECT * FROM t WHERE v < 3")
        check("plain explain has no actuals", "actual time=" not in out, out)

        # 3. BUFFERS adds a delta line
        out, _, _ = run_sql(
            "USE DATABASE etest; "
            "EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM t WHERE v < 3")
        check("buffers delta line", "Buffers: shared hit=" in out, out)

        # 4. join: multiple nodes report actuals
        out, _, _ = run_sql(
            "USE DATABASE etest; "
            "CREATE TABLE u (id INT, w INT); "
            "INSERT INTO u VALUES (1,9),(2,8),(3,7); "
            "EXPLAIN ANALYZE SELECT * FROM t JOIN u ON t.id = u.id")
        ana = section(out, "ANALYZE")
        n_actual = ana.count("actual time=")
        check("join plan has >=2 instrumented nodes", n_actual >= 2, out)

        print(f"[EXPLAIN-ANALYZE] passed={passed} failed={failed}")
        return 0 if failed == 0 else 1
    finally:
        os.chdir("/")
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
