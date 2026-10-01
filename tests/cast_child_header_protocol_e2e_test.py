#!/usr/bin/env python3
"""Casts retain strong child names, while arithmetic children use the type."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "cast_child_header_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected_rows, expected_headers):
        rows, state, message, headers, tag = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        assert rows == expected_rows, (sql, rows)
        assert headers == expected_headers, (sql, headers)
        assert tag == "SELECT 1", (sql, tag)

    try:
        query("SELECT CAST(abs(-1) AS numeric),abs(-1)::numeric,CAST(COALESCE(NULL,1) AS numeric),coalesce(NULL,1)::numeric,CAST(NULLIF(1,2) AS numeric),CAST(GREATEST(1,2) AS numeric),CAST(LEAST(1,2) AS numeric),CAST(CAST(abs(-1) AS numeric) AS text);",
              [["1", "1", "1", "1", "1", "2", "1", "1"]],
              ["abs", "abs", "coalesce", "coalesce", "nullif", "greatest", "least", "abs"])
        query("SELECT CAST(CAST(1 AS integer) AS text),CAST(1+2 AS text),CAST(abs(-1)+1 AS text),CAST(abs(-1) AS numeric) AS value;",
              [["1", "3", "2", "1"]], ["text", "text", "text", "value"])
        query("SELECT 42;", [["42"]], ["?column?"])
        print("[CAST CHILD HEADER PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
