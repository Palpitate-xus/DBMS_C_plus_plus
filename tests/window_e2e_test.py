#!/usr/bin/env python3
"""E2E test for window frame exclusion and named windows (Wave 4.21).

Runs dbms_main with a set of SELECT statements and checks the output.
"""
import os
import subprocess
import shutil
import sys

DBMS_MAIN = os.path.join(os.path.dirname(__file__), "..", "dbms_main")

def run_sql(sql):
    """Send SQL commands (semicolon-separated) to dbms_main and return stdout lines."""
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    script = "admin admin\n" + "\n".join(statements) + "\nexit\n"
    proc = subprocess.run(
        [DBMS_MAIN],
        input=script,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return proc.stdout.splitlines(), proc.stderr.splitlines(), proc.returncode

def setup_db():
    if os.path.exists("wftest"):
        shutil.rmtree("wftest")

def parse_rows(lines):
    """Parse dbms_main output rows; extract id (first token) and sum (last token)."""
    rows = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("login") or s.startswith("successfully") or s.startswith("set Database"):
            continue
        if s.startswith("id "):
            continue
        if s.startswith("SQL") or s.startswith("Table") or s.startswith("ERROR") or s.startswith("Invalid"):
            continue
        vals = s.split()
        if len(vals) < 2:
            continue
        rows.append({"id": vals[0], "sum": vals[-1]})
    return rows

def row_by_id(rows, row_id):
    for r in rows:
        if r.get("id") == str(row_id):
            return r
    return None

def main():
    os.chdir(os.path.dirname(__file__) + "/..")
    if not os.path.exists(DBMS_MAIN):
        print(f"ERROR: {DBMS_MAIN} not found; run scripts/build.sh first")
        return 1

    setup_db()

    # Create table and insert data
    out, err, rc = run_sql(
        "CREATE DATABASE wftest; "
        "USE DATABASE wftest; "
        "CREATE TABLE emp (id INT, dept VARCHAR(20), salary INT); "
        "INSERT INTO emp VALUES (1,'A',100), (2,'A',200), (3,'A',300), "
        "(4,'B',100), (5,'B',200);"
    )
    if rc != 0:
        print("CREATE/INSERT failed:", out, err)
        return 1

    tests_passed = 0
    tests_failed = 0

    def check(sql, expected_by_id, name):
        nonlocal tests_passed, tests_failed
        out, err, rc = run_sql("USE DATABASE wftest; " + sql)
        rows = parse_rows(out)
        ok = True
        for rid, expected in expected_by_id.items():
            r = row_by_id(rows, rid)
            actual = r.get("sum", "") if r else ""
            if actual != expected:
                ok = False
                print(f"[FAIL] {name} id={rid}: got '{actual}', expected '{expected}'")
        if ok:
            tests_passed += 1
            print(f"[PASS] {name}")
        else:
            tests_failed += 1
            print("  stdout:", out)
            print("  stderr:", err)

    # ---- frame exclusion tests ----
    # sum over full partition: A=600, B=300
    check(
        "SELECT id, dept, salary, sum(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM emp ORDER BY id;",
        {1: "600", 2: "600", 3: "600", 4: "300", 5: "300"},
        "EXCLUDE NO OTHERS / default",
    )
    # EXCLUDE CURRENT ROW: exclude current row from the frame
    check(
        "SELECT id, dept, salary, sum(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM emp ORDER BY id;",
        {1: "500", 2: "400", 3: "300", 4: "200", 5: "100"},
        "EXCLUDE CURRENT ROW",
    )
    # EXCLUDE GROUP no ties: same as EXCLUDE CURRENT ROW for a unique value
    check(
        "SELECT id, dept, salary, sum(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM emp ORDER BY id;",
        {1: "500", 2: "400", 3: "300", 4: "200", 5: "100"},
        "EXCLUDE GROUP no ties",
    )
    # EXCLUDE TIES no ties: no peers, same as full sum
    check(
        "SELECT id, dept, salary, sum(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM emp ORDER BY id;",
        {1: "600", 2: "600", 3: "600", 4: "300", 5: "300"},
        "EXCLUDE TIES no ties",
    )

    # Create data with ties: duplicate salary in dept A
    run_sql("USE DATABASE wftest; INSERT INTO emp VALUES (6,'A',200);")
    # partition A total = 100 + 200 + 300 + 200 = 800
    check(
        "SELECT id, dept, salary, sum(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM emp WHERE dept='A' ORDER BY id;",
        {1: "700", 2: "400", 3: "500", 6: "400"},
        "EXCLUDE GROUP with ties",
    )
    check(
        "SELECT id, dept, salary, sum(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM emp WHERE dept='A' ORDER BY id;",
        {1: "800", 2: "600", 3: "800", 6: "600"},
        "EXCLUDE TIES with ties",
    )
    check(
        "SELECT id, dept, salary, sum(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) FROM emp WHERE dept='A' ORDER BY id;",
        {1: "800", 2: "800", 3: "800", 6: "800"},
        "EXCLUDE NO OTHERS with ties",
    )

    # ---- named window tests ----
    # Use standard SQL clause ordering: WHERE ... WINDOW ... ORDER BY
    check(
        "SELECT id, dept, salary, sum(salary) OVER w FROM emp WHERE dept='A' WINDOW w AS (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY id;",
        {1: "800", 2: "800", 3: "800", 6: "800"},
        "named window basic",
    )
    check(
        "SELECT id, dept, salary, sum(salary) OVER w FROM emp WHERE dept='A' WINDOW w AS (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) ORDER BY id;",
        {1: "700", 2: "600", 3: "500", 6: "600"},
        "named window with EXCLUDE CURRENT ROW",
    )

    setup_db()
    print(f"\n=== Window E2E tests: {tests_passed} passed, {tests_failed} failed ===")
    return 0 if tests_failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
