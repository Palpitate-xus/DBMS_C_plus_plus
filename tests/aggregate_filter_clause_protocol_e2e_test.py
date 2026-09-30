#!/usr/bin/env python3
"""SQL FILTER retains null tests and the complete nested function predicate."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "filter_clause_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE filter_clause (v NUMERIC,f INT,t TEXT);")
        query("INSERT INTO filter_clause VALUES (0.50,1,'other'),(0.500,1,'other'),(2.00,0,'a  )  b'),"
              "(NULL,1,'other'),(4.00,0,'other'),(8.00,NULL,'other');")
        actual = query("SELECT count(DISTINCT v) FILTER (WHERE f IS NULL),"
                       "count(DISTINCT v) FILTER (WHERE f IS NOT NULL),count(*) FROM filter_clause;")
        assert actual == [["1", "3", "6"]], actual
        actual = query("SELECT count(DISTINCT v) FILTER (WHERE abs(f)=0) FROM filter_clause;")
        assert actual == [["2"]], actual
        actual = query("SELECT count(*) FILTER (WHERE f IS NULL),"
                       "count(*) FILTER (WHERE f IS NOT NULL),count(*) FROM filter_clause;")
        assert actual == [["1", "5", "6"]], actual
        actual = query("SELECT count(*) FILTER (WHERE t='a  )  b') FROM filter_clause;")
        assert actual == [["1"]], actual
        actual = query("SELECT f,count(DISTINCT v) FILTER (WHERE abs(f)=0),"
                       "count(*) FILTER (WHERE f IS NULL) FROM filter_clause GROUP BY f ORDER BY f;")
        assert actual == [["0", "2", "0"], ["1", "0", "0"], [None, "0", "1"]], actual
        assert query("SELECT 42;") == [["42"]]
        print("[AGGREGATE FILTER CLAUSE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
