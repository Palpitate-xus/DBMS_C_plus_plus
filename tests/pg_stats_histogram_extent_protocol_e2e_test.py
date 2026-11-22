#!/usr/bin/env python3
"""ANALYZE exposes the correct FLOAT8 histogram extent in pg_stats."""

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

    def check_histogram(source, table, expected):
        sql = ("SELECT attname, null_frac, avg_width, histogram_bounds FROM " + source
               + " WHERE tablename = '" + table + "'")
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, names, tag, oids = runner.decode_wire_result(
            messages, include_types=True)
        assert state is None and messages[-1] == (b"Z", b"I"), (sql, state, message)
        assert tag == "SELECT 1" and oids == [19, 700, 23, 2277], (tag, oids)
        assert names == ["attname", "null_frac", "avg_width", "histogram_bounds"], names
        assert rows == [["v", "0", "8", expected]], rows

    try:
        query("CREATE TABLE bound_histogram (v DOUBLE PRECISION);")
        query("INSERT INTO bound_histogram VALUES " +
              ",".join(f"({i}.25)" for i in range(20, 0, -1)) + ";")
        query("ANALYZE bound_histogram;")
        assert query("SELECT COUNT(*) FROM bound_histogram;") == [["20"]]
        bounds = "{" + ",".join(f"{i}.25" for i in range(1, 21)) + "}"
        check_histogram("pg_stats", "bound_histogram", bounds)
        query("CREATE TABLE small_histogram (v DOUBLE PRECISION);")
        query("INSERT INTO small_histogram VALUES (0.25);")
        query("ANALYZE small_histogram;")
        assert query("SELECT COUNT(*) FROM small_histogram;") == [["1"]]
        check_histogram("pg_catalog.pg_stats", "small_histogram", None)
        assert query("SELECT 42;") == [["42"]]
        print("[PG STATS HISTOGRAM EXTENT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
