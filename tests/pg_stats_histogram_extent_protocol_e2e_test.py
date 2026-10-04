#!/usr/bin/env python3
"""ANALYZE remains usable while the incomplete pg_stats view fails closed."""

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

    def expect_catalog_unavailable(sql):
        _, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == "0A000", (sql, state, message)

    try:
        query("CREATE TABLE bound_histogram (v DOUBLE PRECISION);")
        query("INSERT INTO bound_histogram VALUES " +
              ",".join(f"({i}.25)" for i in range(20, 0, -1)) + ";")
        query("ANALYZE bound_histogram;")
        assert query("SELECT COUNT(*) FROM bound_histogram;") == [["20"]]
        expect_catalog_unavailable("SELECT * FROM pg_stats;")
        query("CREATE TABLE small_histogram (v DOUBLE PRECISION);")
        query("INSERT INTO small_histogram VALUES (0.25);")
        query("ANALYZE small_histogram;")
        assert query("SELECT COUNT(*) FROM small_histogram;") == [["1"]]
        expect_catalog_unavailable("SELECT * FROM pg_stats;")
        assert query("SELECT 42;") == [["42"]]
        print("[PG STATS HISTOGRAM EXTENT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
