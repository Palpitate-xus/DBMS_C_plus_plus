#!/usr/bin/env python3
"""EXPLAIN ANALYZE retains BETWEEN bound AND and quoted AND text."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "explain_between_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE explain_between (id BIGINT PRIMARY KEY,v INT,s TEXT);")
        query("INSERT INTO explain_between VALUES (-1,7,'x'),(0,7,'x'),"
              "(1,8,'x and y'),(2,7,'it''s and quoted'),"
              "(9007199254740992,7,'x'),(9007199254740993,8,'x');")
        query("CREATE INDEX explain_between_v ON explain_between(v);")
        cases = (("id BETWEEN 1 AND 2", 2),
                 ("id NOT BETWEEN 1 AND 2", 4),
                 ("id BETWEEN 1 AND 2 AND v=7", 1),
                 ("v=7 AND id NOT BETWEEN 1 AND 2", 3),
                 ("id BETWEEN 1 AND 2 AND v BETWEEN 7 AND 8", 2),
                 ("id BETWEEN 9007199254740992.9 AND 9007199254740993.1", 1),
                 ("s='x and y' AND v=8", 1),
                 ("s='it''s and quoted' AND v=7", 1),
                 ("id=2 AND v=7", 1))
        for predicate, expected in cases:
            sql = "SELECT id FROM explain_between WHERE " + predicate + ";"
            assert len(query(sql)) == expected, sql
            for _ in range(2):
                document = json.loads("\n".join(row[0] for row in query(
                    "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + sql)))
                assert document["actualRows"] == expected, (sql, document)
            text = "\n".join(row[0] for row in query("EXPLAIN ANALYZE " + sql))
            assert "Actual rows: " + str(expected) in text, (sql, text)
        assert query("SELECT 42;") == [["42"]]
        print("[EXPLAIN BETWEEN PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
