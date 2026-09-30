#!/usr/bin/env python3
"""EXPLAIN executes the same AND/OR grouping as the ordinary query."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "explain_boolean_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE explain_boolean (id INT PRIMARY KEY,v INT,s TEXT);")
        query("INSERT INTO explain_boolean VALUES (1,8,'x and or y'),(2,7,'x'),"
              "(3,7,'it''s or quoted'),(4,NULL,NULL),(5,9,'x');")
        query("CREATE INDEX explain_boolean_v ON explain_boolean(v);")
        cases = (("id=1 OR id=2", 2),
                 ("id=1 OR id=2 AND v=8", 1),
                 ("(id=1 OR id=2) AND v=7", 1),
                 ("(id=1 OR id=2) AND (v=7 OR v=8)", 2),
                 ("id BETWEEN 1 AND 2 OR v IS NULL", 3),
                 ("(id NOT BETWEEN 1 AND 2 AND v=7) OR id=1", 2),
                 ("id IN (1,2) OR v IS NULL", 3),
                 ("s='x and or y' OR s='it''s or quoted'", 2),
                 ("id=2 OR id=2 OR id BETWEEN 2 AND 3", 2),
                 ("(id=1 OR id=2) AND (v=9 OR v IS NULL)", 0))
        for predicate, expected in cases:
            sql = "SELECT id FROM explain_boolean WHERE " + predicate + ";"
            assert len(query(sql)) == expected, (sql, expected)
            for _ in range(2):
                document = json.loads("\n".join(row[0] for row in query(
                    "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + sql)))
                assert document["actualRows"] == expected, (sql, document)
            text = "\n".join(row[0] for row in query("EXPLAIN ANALYZE " + sql))
            assert "Actual rows: " + str(expected) in text, (sql, text)
        huge = " AND ".join("(id=1 OR id=2)" for _ in range(12))
        for predicate, expected_state in ((huge, "54000"),
                                          ("NOT (id=1 OR id=2)", "0A000"),
                                          ("false", "0A000")):
            rows, state, message, _, _ = runner.decode_wire_result(
                client.simple_query(server["sock"],
                    "EXPLAIN (FORMAT JSON,ANALYZE TRUE) SELECT id "
                    "FROM explain_boolean WHERE " + predicate + ";"))
            assert state == expected_state and rows == [], (predicate, rows, state, message)
        assert query("SELECT 42;") == [["42"]]
        print("[EXPLAIN BOOLEAN PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
