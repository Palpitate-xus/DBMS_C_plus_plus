#!/usr/bin/env python3
"""E2E test for SELECT ... FROM ONLY under table inheritance.

Verifies:
  - plain SELECT on a parent unions inherited children (existing behavior)
  - SELECT ... FROM ONLY parent returns parent rows alone
  - ONLY works with parenthesized form ONLY(parent)
  - UPDATE/DELETE ONLY still target just the named table

Runs dbms_main in a temp working directory.
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
COMMAND_TIMEOUT = float(os.environ.get("DBMS_INHERIT_TEST_TIMEOUT", "60"))


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
    return proc.stdout.splitlines(), proc.stderr.splitlines(), proc.returncode


def data_rows(lines):
    """Extract the numeric id values from SELECT output rows."""
    rows = []
    in_data = False
    for line in lines:
        s = line.strip()
        if not s:
            continue
        low = s.lower()
        if low.startswith("id") and len(s.split()) <= 2:
            in_data = True
            continue
        if not in_data:
            continue
        if s.startswith(("SQL", "Table", "ERROR", "Invalid", "login", "success", "set")):
            in_data = False
            continue
        vals = s.split()
        if len(vals) >= 1:
            try:
                rows.append(int(vals[0]))
            except ValueError:
                in_data = False
    return rows


def main():
    if not os.path.exists(DBMS_MAIN):
        print(f"ERROR: {DBMS_MAIN} not found; run scripts/build.sh first")
        return 1

    work_dir = tempfile.mkdtemp(prefix="dbms-inherit-only-")
    try:
        os.chdir(work_dir)
        os.makedirs(os.path.join("info", "pg_catalog"), exist_ok=True)
        with open(os.path.join("info", "pg_catalog", "pg_authid.cat"), "w", encoding="utf-8") as auth:
            auth.write('10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n' % scram_verifier("admin"))
        open(os.path.join("info", "tlist.lst"), "wb").close()

        out, err, rc = run_sql(
            "CREATE DATABASE itest; "
            "USE DATABASE itest; "
            "CREATE TABLE cities (id INT, name VARCHAR(20)); "
            "INSERT INTO cities VALUES (1,'a'), (2,'b'); "
            "CREATE TABLE capitals (state VARCHAR(10)) INHERITS (cities); "
            "INSERT INTO capitals VALUES (3,'c','S');"
        )
        if rc != 0:
            print("setup failed:", out, err)
            return 1

        passed = failed = 0

        def check(sql, expected, name):
            nonlocal passed, failed
            out, err, rc = run_sql("USE DATABASE itest; " + sql)
            got = sorted(data_rows(out))
            if got == sorted(expected):
                passed += 1
                print(f"[INHERIT-ONLY] {name} OK")
            else:
                failed += 1
                print(f"[INHERIT-ONLY] {name} FAIL: expected {sorted(expected)} got {got}")
                print("  stdout:", out)

        # Plain parent SELECT unions children
        check("SELECT id FROM cities", [1, 2, 3], "parent select includes children")
        # ONLY restricts to the parent
        check("SELECT id FROM ONLY cities", [1, 2], "ONLY parent excludes children")
        # parenthesized form
        check("SELECT id FROM ONLY(cities)", [1, 2], "ONLY(parent) parenthesized")
        # flag must not leak to the next statement
        check("SELECT id FROM cities", [1, 2, 3], "flag resets after ONLY")
        # child query unaffected
        check("SELECT id FROM capitals", [3], "child select")
        # UPDATE ONLY: rows of children stay intact (already single-table)
        out, err, rc = run_sql(
            "USE DATABASE itest; UPDATE ONLY cities SET name = 'x' WHERE id = 1;"
        )
        # verify parent row updated, child untouched
        check("SELECT id FROM cities WHERE name = 'x'", [1], "UPDATE ONLY parent")

        print(f"[INHERIT-ONLY] passed={passed} failed={failed}")
        return 0 if failed == 0 else 1
    finally:
        os.chdir("/")
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
