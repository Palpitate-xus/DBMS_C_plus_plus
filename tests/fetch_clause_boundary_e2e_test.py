#!/usr/bin/env python3
"""FETCH rewriting must preserve quoted text and inner-query boundaries."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("fetch_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ("CREATE TABLE fetch_boundary_rows (id INT);",
                    "INSERT INTO fetch_boundary_rows VALUES (1), (2), (3);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
        for sql, expected in [
            ("SELECT 'fetch first 1 rows only' AS value FROM fetch_boundary_rows WHERE id = 1;",
             [["fetch first 1 rows only"]]),
            ("SELECT 'prefix fetch first 1 rows only suffix' AS value FROM fetch_boundary_rows WHERE id = 1;",
             [["prefix fetch first 1 rows only suffix"]]),
            ('SELECT id AS "fetch" FROM fetch_boundary_rows ORDER BY id FETCH NEXT 1 ROW ONLY;', [["1"]]),
            ("SELECT (SELECT id FROM fetch_boundary_rows ORDER BY id DESC FETCH FIRST 1 ROWS WITH TIES) "
             "AS picked FROM fetch_boundary_rows WHERE id = 1;", [["3"]]),
            ("SELECT id FROM fetch_boundary_rows ORDER BY id FETCH FIRST ROW ONLY;", [["1"]]),
            ("SELECT id FROM fetch_boundary_rows ORDER BY id FETCH NEXT 2 ROWS ONLY;", [["1"], ["2"]]),
            ("SELECT id FROM fetch_boundary_rows ORDER BY id OFFSET 1 ROWS FETCH NEXT 1 ROW ONLY;", [["2"]]),
        ]:
            rows, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
            assert rows == expected, (sql, rows, expected)
        rows, state, message, _ = runner.ours_query(
            client, server["sock"],
            "SELECT id FROM fetch_boundary_rows ORDER BY id FETCH FIRST 1 ROWS WITH TIES;")
        assert state is None, (state, message)
        assert rows == [["1"]], rows
        rows, state, message, _ = runner.ours_query(
            client, server["sock"],
            "SELECT id FROM fetch_boundary_rows ORDER BY id FETCH FIRST +1 ROWS WITH TIES;")
        assert state is None, (state, message)
        assert rows == [["1"]], rows
        rows, state, message, _ = runner.ours_query(
            client, server["sock"],
            "SELECT id FROM fetch_boundary_rows ORDER BY id FETCH FIRST -1 ROWS WITH TIES;")
        assert state == "2201W", (state, message)
        rows, state, message, _ = runner.ours_query(
            client, server["sock"],
            "SELECT id FROM fetch_boundary_rows ORDER BY id FETCH FIRST -0 ROWS WITH TIES;")
        assert state is None and rows == [], (state, rows, message)
        rows, state, message, _ = runner.ours_query(
            client, server["sock"],
            "SELECT id FROM fetch_boundary_rows ORDER BY id LIMIT +1;")
        assert state is None and rows == [["1"]], (state, rows, message)
        rows, state, message, _ = runner.ours_query(
            client, server["sock"],
            "SELECT id FROM fetch_boundary_rows ORDER BY id OFFSET +1;")
        assert state is None and rows == [["2"], ["3"]], (state, rows, message)
        rows, state, message, _ = runner.ours_query(
            client, server["sock"], "SELECT id FROM fetch_boundary_rows ORDER BY id;")
        assert state is None and rows == [["1"], ["2"], ["3"]], (state, rows, message)
        print("[FETCH CLAUSE BOUNDARY E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
