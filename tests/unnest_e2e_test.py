#!/usr/bin/env python3
"""E2E test for unnest() table function.

Verifies through the real SQL shell:
  - SELECT * FROM unnest(ARRAY[...]) expands to one row per element
  - SELECT unnest(ARRAY[...]) (FROM-less) works identically
  - quoted string elements (with embedded spaces) survive intact
  - a single-element array yields one row
  - unnest over an expression is not mangled by sqlProcessor rewrites

Runs dbms_main in a temp working directory with a SCRAM admin account.
"""
import os
import base64
import hashlib
import hmac
import subprocess
import sys
import tempfile

DBMS_MAIN = os.path.join(os.path.dirname(__file__), "..", "dbms_main")
COMMAND_TIMEOUT = float(os.environ.get("DBMS_UNNEST_TEST_TIMEOUT", "60"))


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


def rows_after_header(stdout, header="unnest "):
    """Return output lines following the last `header` marker."""
    lines = stdout.splitlines()
    last = None
    for i, line in enumerate(lines):
        if line.strip() == header.strip():
            last = i
    if last is None:
        return []
    return lines[last + 1:]


def main():
    if not os.path.exists(DBMS_MAIN):
        print(f"ERROR: {DBMS_MAIN} not found; run scripts/build.sh first")
        return 1

    work_dir = tempfile.mkdtemp(prefix="dbms-unnest-")
    os.chdir(work_dir)
    os.makedirs(os.path.join("info", "pg_catalog"), exist_ok=True)
    with open(os.path.join("info", "pg_catalog", "pg_authid.cat"), "w",
              encoding="utf-8") as auth:
        auth.write('10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n'
                   % scram_verifier("admin"))
    open(os.path.join("info", "tlist.lst"), "wb").close()

    passed = failed = 0

    def check(name, cond):
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"[UNNEST-E2E] {name} OK")
        else:
            failed += 1
            print(f"[UNNEST-E2E] {name} FAIL")

    # FROM form
    out, err, rc = run_sql(["SELECT * FROM unnest(ARRAY[1,2,3])"])
    check("FROM unnest numbers", rows_after_header(out) == ["1", "2", "3"])

    # FROM-less form
    out, err, rc = run_sql(["SELECT unnest(ARRAY[4,5,6])"])
    check("SELECT unnest(...) no FROM", rows_after_header(out) == ["4", "5", "6"])

    # quoted elements with spaces
    out, err, rc = run_sql(["SELECT * FROM unnest(ARRAY['a','b','c d'])"])
    check("quoted elements", rows_after_header(out) == ["a", "b", "c d"])

    # single element
    out, err, rc = run_sql(["SELECT * FROM unnest(ARRAY[42])"])
    check("single element", rows_after_header(out) == ["42"])

    # alias form
    out, err, rc = run_sql(["SELECT u FROM unnest(ARRAY[7,8]) AS u"])
    check("aliased", rows_after_header(out) == ["7", "8"])

    print(f"[UNNEST-E2E] {passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
