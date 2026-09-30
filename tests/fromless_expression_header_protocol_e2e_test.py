#!/usr/bin/env python3
"""A cast inside an operator does not name the whole SELECT expression."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "expression_header_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("SELECT -1::numeric,1::numeric+2::numeric,(1+2)::numeric,abs(-1)+1,-abs(-1),'a'::text||'b'::text,'a'::text;",
              [["-1", "3", "3", "2", "-1", "ab", "a"]],
              ["?column?", "?column?", "numeric", "?column?", "?column?", "?column?", "text"])
        query("SELECT abs(-1),CAST(1+2 AS numeric),(1::numeric+2::numeric) AS amount;",
              [["1", "3", "3"]], ["abs", "numeric", "amount"])
        query("SELECT 42;", [["42"]], ["?column?"])
        print("[FROMLESS EXPRESSION HEADER PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
