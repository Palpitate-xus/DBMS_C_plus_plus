#!/usr/bin/env python3
"""COUNT(DISTINCT column) uses exact type equality and excludes only SQL NULL."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "typed_count_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE typed_count (g INT,v NUMERIC,t TEXT);")
        query("INSERT INTO typed_count VALUES (1,0.50,'0.50'),(1,0.500,'0.500'),"
              "(2,2.00,''),(2,2.000,'NULL'),(1,NULL,NULL),(2,NULL,NULL);")
        assert query("SELECT count(DISTINCT v),count(v),count(*) FROM typed_count;") == [["2", "4", "6"]]
        assert query("SELECT count(DISTINCT t) FROM typed_count;") == [["4"]]
        assert query("SELECT g,count(DISTINCT v) FROM typed_count GROUP BY g ORDER BY g;") == [["1", "1"], ["2", "1"]]
        assert query("SELECT count(DISTINCT v) FROM typed_count WHERE g=1;") == [["1"]]
        assert query("SELECT count(DISTINCT v) FILTER (WHERE g=2) FROM typed_count;") == [["1"]]
        assert query("SELECT count(DISTINCT v) FROM typed_count WHERE g=9;") == [["0"]]
        query("CREATE TABLE typed_count_exact (v NUMERIC);")
        query("INSERT INTO typed_count_exact VALUES (123456789012345678901234567890.1),"
              "(123456789012345678901234567890.10),(123456789012345678901234567890.2);")
        assert query("SELECT count(DISTINCT v) FROM typed_count_exact;") == [["2"]]
        assert query("SELECT 42;") == [["42"]]
        print("[TYPED COUNT DISTINCT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
