#!/usr/bin/env python3
"""WITH inside a value or subquery must not enter top-level CTE rewriting."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("cte_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for statement in ("CREATE TABLE cte_boundary_rows (id INT);",
                          "INSERT INTO cte_boundary_rows VALUES (1);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], statement)
            assert state is None, (statement, state, message)
        for statement, expected in [
            ("SELECT 'with data' AS value FROM cte_boundary_rows;", [["with data"]]),
            ("SELECT 'start with data' AS value FROM cte_boundary_rows;", [["start with data"]]),
            ("SELECT (SELECT 'with data' FROM cte_boundary_rows) AS value "
             "FROM cte_boundary_rows;", [["with data"]]),
            ("WITH selected AS (SELECT id FROM cte_boundary_rows) SELECT id FROM selected;", [["1"]]),
        ]:
            rows, state, message, _ = runner.ours_query(client, server["sock"], statement)
            assert state is None, (statement, state, message)
            assert rows == expected, (statement, rows, expected)
        print("[CTE CLAUSE BOUNDARY E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
