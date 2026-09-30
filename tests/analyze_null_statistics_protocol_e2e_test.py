#!/usr/bin/env python3
"""ANALYZE publishes physical NULL fractions, not empty-string counts."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "analyze_null_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def fraction(table, column):
        # The current legacy catalog frontend ignores projection/WHERE.
        # Keep that separately tracked bug out of this statistics fixture.
        rows = query("SELECT * FROM pg_stats;")
        matches = [row for row in rows if row[1:3] == [table, column]]
        assert len(matches) == 1 and len(matches[0]) == 8, (table, column, rows)
        return matches[0][3]

    try:
        query("CREATE TABLE null_stats_items (id INT PRIMARY KEY,v TEXT,all_null INT);")
        values = ",".join(
            f"({i}," + ("NULL" if i < 5 else "''" if i < 10 else "'NULL'" if i < 15 else "'other'")
            + ",NULL)" for i in range(20))
        query("INSERT INTO null_stats_items VALUES " + values + ";")
        query("ANALYZE null_stats_items;")
        assert fraction("null_stats_items", "v") == "0.250000"
        assert fraction("null_stats_items", "all_null") == "1.000000"
        assert fraction("null_stats_items", "id") == "0.000000"
        assert query("SELECT id FROM null_stats_items WHERE v IS NULL ORDER BY id;") == [
            [str(i)] for i in range(5)]
        assert query("SELECT id FROM null_stats_items WHERE v='' ORDER BY id;") == [
            [str(i)] for i in range(5, 10)]
        query("CREATE TABLE null_stats_empty (v TEXT);")
        query("ANALYZE null_stats_empty;")
        assert fraction("null_stats_empty", "v") == "0.000000"
        assert query("SELECT 42;") == [["42"]]
        print("[ANALYZE NULL STATISTICS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
