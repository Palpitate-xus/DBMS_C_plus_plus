#!/usr/bin/env python3
"""Numeric indexes identify exact SQL values, independently of display scale."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "numeric_key_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected_state=None):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected_state, (sql, state, message)
        if expected_state:
            assert not rows, (sql, rows)
        return rows

    try:
        query("CREATE TABLE numeric_key_primary (v NUMERIC PRIMARY KEY);")
        query("INSERT INTO numeric_key_primary VALUES (0.50);")
        query("INSERT INTO numeric_key_primary VALUES (0.500);", "23505")
        assert query("SELECT v FROM numeric_key_primary;") == [["0.50"]]
        query("INSERT INTO numeric_key_primary VALUES (1.00),(1.000);", "23505")
        assert query("SELECT v FROM numeric_key_primary;") == [["0.50"]]
        query("CREATE TABLE numeric_key_unique (id INT PRIMARY KEY,v NUMERIC UNIQUE);")
        query("INSERT INTO numeric_key_unique VALUES (1,0.50);")
        query("INSERT INTO numeric_key_unique VALUES (2,0.500);", "23505")
        query("INSERT INTO numeric_key_unique VALUES (3,NULL),(4,NULL);")
        assert query("SELECT id FROM numeric_key_unique ORDER BY id;") == [["1"], ["3"], ["4"]]
        query("CREATE TABLE numeric_key_secondary (id INT PRIMARY KEY,v NUMERIC);")
        query("INSERT INTO numeric_key_secondary VALUES (1,0.50),(2,0.500),(3,2.00);")
        query("CREATE INDEX numeric_secondary_idx ON numeric_key_secondary(v);")
        for probe in ("0.5", "0.5000", "5e-1"):
            assert query(f"SELECT id FROM numeric_key_secondary WHERE v={probe} ORDER BY id;") == [["1"], ["2"]]
            document = json.loads("\n".join(row[0] for row in query(
                f"EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) "
                f"SELECT id FROM numeric_key_secondary WHERE v={probe};")))
            assert document["actualRows"] == 2, document
        query("BEGIN;")
        query("INSERT INTO numeric_key_secondary VALUES (4,0.50000);")
        query("ROLLBACK;")
        assert query("SELECT id FROM numeric_key_secondary WHERE v=0.5 ORDER BY id;") == [["1"], ["2"]]
        query("UPDATE numeric_key_secondary SET v=3.00 WHERE id=2;")
        assert query("SELECT id FROM numeric_key_secondary WHERE v=0.5000;") == [["1"]]
        assert query("SELECT id FROM numeric_key_secondary WHERE v=3.000;") == [["2"]]
        assert query("SELECT 42;") == [["42"]]
        print("[NUMERIC INDEX KEY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
