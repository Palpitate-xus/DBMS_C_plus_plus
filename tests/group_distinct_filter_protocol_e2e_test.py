#!/usr/bin/env python3
"""FILTER affects its DISTINCT aggregate, not sibling aggregates or groups."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "group_filter_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    try:
        query("CREATE TABLE group_filter (g INT,v NUMERIC,f INT);")
        query("INSERT INTO group_filter VALUES (1,0.50,1),(1,0.500,1),(1,2.00,0),"
              "(1,NULL,1),(2,4.00,0),(2,8.00,NULL);")
        for flag, expected in ((1, [["1", "1", "4"], ["2", "0", "2"]]),
                               (0, [["1", "1", "4"], ["2", "1", "2"]]),
                               (9, [["1", "0", "4"], ["2", "0", "2"]])):
            actual = query(f"SELECT g,count(DISTINCT v) FILTER (WHERE f={flag}),count(*) "
                           "FROM group_filter GROUP BY g ORDER BY g;")
            assert actual == expected, (flag, actual, expected)
        assert query("SELECT g,count(DISTINCT v) FILTER (WHERE f=1),count(*) FROM group_filter "
                     "GROUP BY GROUPING SETS ((g),()) ORDER BY g;") == [["1", "1", "4"], ["2", "0", "2"], [None, "1", "6"]]
        assert query("SELECT 42;") == [["42"]]
        print("[GROUP DISTINCT FILTER PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
