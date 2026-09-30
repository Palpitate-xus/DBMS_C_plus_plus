#!/usr/bin/env python3
"""EXPLAIN ANALYZE executes typed native DISTINCT, not frontend deduplication."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "native_distinct_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE native_distinct(v NUMERIC,t TEXT);")
        query("INSERT INTO native_distinct VALUES (0.50,'same'),(0.500,'same'),"
              "(2.00,''),(NULL,'NULL'),(NULL,NULL);")
        for projection, expected in [("v", 3), ("t", 4), ("v,t", 4)]:
            rows = query("EXPLAIN (ANALYZE,FORMAT JSON) SELECT DISTINCT " + projection + " FROM native_distinct;")
            document = json.loads("\n".join(row[0] for row in rows))
            node = document["plan"]
            assert document["actualRows"] == expected, (projection, document)
            assert node["actualRows"] == expected, (projection, node)
            assert node["actualLoops"] > 0, node
        assert query("SELECT DISTINCT v FROM native_distinct ORDER BY v;") == [["0.50"], ["2.00"], [None]]
        assert query("SELECT 42;") == [["42"]]
        print("[NATIVE DISTINCT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
