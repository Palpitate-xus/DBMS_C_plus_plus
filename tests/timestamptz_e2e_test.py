#!/usr/bin/env python3
"""E2E test for timestamptz rendering under the session TimeZone GUC.

Verifies through the real SQL shell:
  - default (UTC) session renders a stored timestamptz without offset
  - SET TIME ZONE 'Asia/Shanghai' renders +08:00 shifted values
  - SET TIME ZONE 'America/New_York' renders -05:00 shifted values
  - offset GUC syntax and repeatability (no double application)

Runs dbms_main in a temp working directory with a SCRAM admin account.
Each statement batch runs in its own dbms_main process (per-session GUC),
but against one shared data directory prepared once.
"""
import os
import base64
import hashlib
import hmac
import subprocess
import sys
import tempfile

DBMS_MAIN = os.path.join(os.path.dirname(__file__), "..", "dbms_main")
COMMAND_TIMEOUT = float(os.environ.get("DBMS_TZ_TEST_TIMEOUT", "60"))


def scram_verifier(password, salt=b"0123456789abcdef", iterations=4096):
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    return (
        "SCRAM-SHA-256$%d:%s$%s$%s"
        % (iterations, base64.b64encode(salt).decode(),
           base64.b64encode(stored_key).decode(),
           base64.b64encode(server_key).decode())
    )


def run_sql(statements):
    script = "admin admin\n" + "\n".join(statements) + "\nexit\n"
    proc = subprocess.run(
        [DBMS_MAIN],
        input=script,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT,
    )
    return proc.stdout, proc.stderr, proc.returncode


def data_rows(stdout):
    """Return the data rows of the LAST query in stdout.

    Rows follow the final column-header line ("id ts"); insert/update
    notifications are filtered out.
    """
    lines = stdout.splitlines()
    last_header = None
    for i, line in enumerate(lines):
        if line.strip() == "id ts":
            last_header = i
    if last_header is None:
        return []
    rows = []
    for l in lines[last_header + 1:]:
        t = l.strip()
        if not t or not t[0].isdigit():
            continue
        if "row(s)" in t or "inserted" in t or "updated" in t or "deleted" in t:
            continue
        rows.append(t)
    return rows


def main():
    if not os.path.exists(DBMS_MAIN):
        print(f"ERROR: {DBMS_MAIN} not found; run scripts/build.sh first")
        return 1

    work_dir = tempfile.mkdtemp(prefix="dbms-tz-")
    os.chdir(work_dir)
    os.makedirs(os.path.join("info", "pg_catalog"), exist_ok=True)
    with open(os.path.join("info", "pg_catalog", "pg_authid.cat"), "w",
              encoding="utf-8") as auth:
        auth.write('10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n'
                   % scram_verifier("admin"))
    open(os.path.join("info", "tlist.lst"), "wb").close()

    # One-time data setup: every later batch re-uses the same database.
    out, err, rc = run_sql([
        "CREATE DATABASE tztest",
        "USE DATABASE tztest",
        "CREATE TABLE tt (id INT, ts TIMESTAMPTZ)",
        "INSERT INTO tt VALUES (1, '2026-08-17 10:00:00+00')",
    ])
    if rc != 0:
        print("setup failed:", out, err)
        return 1

    use = ["USE DATABASE tztest"]

    passed = failed = 0

    def check(name, cond, ctx=""):
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"[TZ-E2E] {name} OK")
        else:
            failed += 1
            print(f"[TZ-E2E] {name} FAIL {ctx}")

    # Default session (UTC): no offset suffix
    out, _, _ = run_sql(use + ["SELECT * FROM tt"])
    rows = data_rows(out)
    check("default UTC render", rows == ["1 2026-8-17 10:00:00"], str(rows))

    # Asia/Shanghai: +8h
    out, _, _ = run_sql(use + [
        "SET TIME ZONE 'Asia/Shanghai'",
        "SELECT * FROM tt",
    ])
    rows = data_rows(out)
    check("Asia/Shanghai +08:00", rows == ["1 2026-8-17 18:00:00 +08:00"], str(rows))

    # America/New_York: -5h
    out, _, _ = run_sql(use + [
        "SET TIME ZONE 'America/New_York'",
        "SELECT * FROM tt",
    ])
    rows = data_rows(out)
    check("America/New_York -05:00", rows == ["1 2026-8-17 05:00:00 -05:00"], str(rows))

    # Offset GUC syntax + idempotence (same query twice)
    out, _, _ = run_sql(use + [
        "SET TIME ZONE '+08:00'",
        "SELECT * FROM tt",
        "SELECT * FROM tt",
    ])
    rows = data_rows(out)
    check("offset syntax + repeat", rows == ["1 2026-8-17 18:00:00 +08:00"], str(rows))

    print(f"[TZ-E2E] {passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
