#!/usr/bin/env python3
"""Catalog histogram boundaries include the final upper bound."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "histogram_extent_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def histogram(table):
        # No projection/WHERE: those legacy catalog bugs remain separate.
        rows = query("SELECT * FROM pg_stats;")
        matches = [row for row in rows if row[1:3] == [table, "v"]]
        assert len(matches) == 1 and len(matches[0]) == 8, rows
        return matches[0][7]

    try:
        query("CREATE TABLE bound_histogram (v DOUBLE PRECISION);")
        query("INSERT INTO bound_histogram VALUES " +
              ",".join(f"({i}.25)" for i in range(20, 0, -1)) + ";")
        query("ANALYZE bound_histogram;")
        expected = ",".join("{" + f"{i}.25" + "}" for i in range(1, 20, 2)) + ",{20.25}"
        assert histogram("bound_histogram") == expected
        query("CREATE TABLE small_histogram (v DOUBLE PRECISION);")
        query("INSERT INTO small_histogram VALUES (0.25);")
        query("ANALYZE small_histogram;")
        assert histogram("small_histogram") == ""
        assert query("SELECT 42;") == [["42"]]
        print("[PG STATS HISTOGRAM EXTENT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
