#!/usr/bin/env python3
"""Execution errors retain their declared SQLSTATE across the wire boundary."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("errors_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ("CREATE TABLE state_outer (id INT);",
                    "CREATE TABLE state_inner (id INT);",
                    "INSERT INTO state_outer VALUES (1);",
                    "INSERT INTO state_inner VALUES (1), (2);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
        cases = [
            ("SELECT id FROM state_inner FETCH FIRST 1 ROW WITH TIES", "42601"),
            ("SELECT id FROM state_inner ORDER BY 2 LIMIT 1", "42P10"),
            ("SELECT id, id FROM state_inner LIMIT 1", "42601"),
            ("SELECT id FROM state_inner", "21000"),
            ("SELECT id FROM state_missing LIMIT 1", "42P01"),
            ("SELECT DISTINCT id FROM state_inner", "0A000"),
        ]
        for inner, expected in cases:
            sql = "SELECT (" + inner + ") AS value FROM state_outer;"
            rows, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state == expected, (sql, state, expected, message)
            assert rows == [], (sql, rows)
            assert "SQLSTATE" not in message, (sql, message)
            # An autocommit error must not poison the connection or leave a lock.
            rows, state, message, _ = runner.ours_query(
                client, server["sock"], "SELECT id FROM state_outer;")
            assert state is None and rows == [["1"]], (state, message, rows)
        print("[SUBQUERY SQLSTATE E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
