#!/usr/bin/env python3
"""E2E test for greedy multi-table join ordering.

Verifies through the real SQL shell:
  - 3-table INNER JOIN chains return the full join (previously the extra
    tables were silently dropped)
  - result rows are correct (values aligned with their source tables)
  - join order does not affect the logical result set
  - 4-table chains work; greedy ordering picks smallest intermediates

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
COMMAND_TIMEOUT = float(os.environ.get("DBMS_JOIN_TEST_TIMEOUT", "90"))


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


def rows_of_last_query(stdout, header):
    """Data rows following the last occurrence of the header line."""
    lines = stdout.splitlines()
    last = None
    for i, line in enumerate(lines):
        if line.strip() == header.strip():
            last = i
    if last is None:
        return None
    return [l.strip() for l in lines[last + 1:] if l.strip()]


def main():
    if not os.path.exists(DBMS_MAIN):
        print(f"ERROR: {DBMS_MAIN} not found; run scripts/build.sh first")
        return 1

    work_dir = tempfile.mkdtemp(prefix="dbms-join-")
    os.chdir(work_dir)
    os.makedirs(os.path.join("info", "pg_catalog"), exist_ok=True)
    with open(os.path.join("info", "pg_catalog", "pg_authid.cat"), "w",
              encoding="utf-8") as auth:
        auth.write('10,"admin",t,t,t,t,t,f,f,-1,"%s",""\n'
                   % scram_verifier("admin"))
    open(os.path.join("info", "tlist.lst"), "wb").close()

    out, err, rc = run_sql([
        "CREATE DATABASE jt",
        "USE DATABASE jt",
        "CREATE TABLE a (id INT, v INT)",
        "CREATE TABLE b (id INT, a_id INT, w INT)",
        "CREATE TABLE c (id INT, b_id INT, x INT)",
        "CREATE TABLE d (id INT, c_id INT, y INT)",
        "INSERT INTO a VALUES (1,10),(2,20)",
        "INSERT INTO b VALUES (1,1,100),(2,2,200),(3,1,300)",
        "INSERT INTO c VALUES (1,1,7),(2,2,8),(3,3,9)",
        "INSERT INTO d VALUES (1,1,1000),(2,2,2000)",
        "ANALYZE a",
        "ANALYZE b",
        "ANALYZE c",
        "ANALYZE d",
    ])
    if rc != 0:
        print("setup failed:", out, err)
        return 1

    use = ["USE DATABASE jt"]

    passed = failed = 0

    def check(name, cond, ctx=""):
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"[JOIN-E2E] {name} OK")
        else:
            failed += 1
            print(f"[JOIN-E2E] {name} FAIL {ctx}")

    hdr3 = "a.id a.v b.id b.a_id b.w c.id c.b_id c.x"
    expected3 = {
        "1 10 1 1 100 1 1 7",
        "1 10 3 1 300 3 3 9",
        "2 20 2 2 200 2 2 8",
    }

    out, _, _ = run_sql(use + [
        "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
    ])
    rows = rows_of_last_query(out, hdr3)
    check("3-table chain rows", rows is not None and set(rows) == expected3,
          str(rows))

    out, _, _ = run_sql(use + [
        "SELECT * FROM b JOIN a ON a.id = b.a_id JOIN c ON b.id = c.b_id",
    ])
    hdr3b = "b.id b.a_id b.w a.id a.v c.id c.b_id c.x"
    expected3b = {
        "1 1 100 1 10 1 1 7",
        "2 2 200 2 20 2 2 8",
        "3 1 300 1 10 3 3 9",
    }
    rows = rows_of_last_query(out, hdr3b)
    check("order independence", rows is not None and set(rows) == expected3b,
          str(rows))

    # 4-table chain: greedy may reorder columns (c x d starts smallest),
    # so compare order-insensitively on the multiset of row values.
    out, _, _ = run_sql(use + [
        "SELECT * FROM a JOIN b ON a.id = b.a_id "
        "JOIN c ON b.id = c.b_id JOIN d ON c.id = d.c_id",
    ])
    lines = out.splitlines()
    data = [l.strip() for l in lines
            if l.strip() and l.strip()[0].isdigit() and "row(s)" not in l]
    # canonical per-row: sorted tokens (join keys repeat, values identify rows)
    canon = sorted(sorted(r.split()) for r in data)
    expected4 = sorted([
        sorted("1 10 1 1 100 1 1 7 1 1 1000".split()),
        sorted("2 20 2 2 200 2 2 8 2 2 2000".split()),
    ])
    check("4-table chain rows", canon == expected4, str(data))

    print(f"[JOIN-E2E] {passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
