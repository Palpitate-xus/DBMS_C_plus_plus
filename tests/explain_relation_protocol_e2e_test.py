#!/usr/bin/env python3
"""EXPLAIN binds quoted/schema/search-path relations outside clause text."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "explain_relation_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def analyze(sql, expected):
        for _ in range(2):
            document = json.loads("\n".join(row[0] for row in query(
                "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + sql)))
            assert document["actualRows"] == expected, (sql, document)

    try:
        query('CREATE TABLE "Plan,Rows" (id INT PRIMARY KEY,v INT);')
        query('INSERT INTO "Plan,Rows" VALUES (1,7),(2,8);')
        query('ANALYZE "Plan,Rows";')
        analyze('SELECT id FROM "Plan,Rows";', 2)
        analyze('SELECT id FROM "Plan,Rows" WHERE id=2;', 1)
        plain = json.loads("\n".join(row[0] for row in query(
            'EXPLAIN (FORMAT JSON) SELECT id FROM "Plan,Rows";')))
        assert plain["totalRows"] == 2, plain
        query('CREATE TABLE "where group by order by limit" (id INT);')
        query('INSERT INTO "where group by order by limit" VALUES (3),(4);')
        analyze('SELECT id FROM "where group by order by limit" WHERE id=3;', 1)
        query('CREATE SCHEMA plan_rel;')
        query('CREATE TABLE plan_rel.items (id INT PRIMARY KEY,v INT);')
        query('INSERT INTO plan_rel.items VALUES (5,7),(6,8);')
        analyze('SELECT id FROM plan_rel.items WHERE id=5;', 1)
        analyze('SELECT id FROM plan_rel . items WHERE id=5;', 1)
        query('SET search_path TO plan_rel,public;')
        analyze('SELECT id FROM items WHERE id=6;', 1)
        query('SET search_path TO public;')
        # WHERE ends before GROUP BY/HAVING, not at ORDER BY alone.
        analyze('SELECT v,count(*) FROM plan_rel.items WHERE id=5 GROUP BY v;', 1)
        for sql, expected in (("EXPLAIN (FORMAT JSON) SELECT id FROM plan_missing;", "42P01"),
                              ("EXPLAIN ANALYZE SELECT id FROM plan_rel.items AS p;", "0A000"),
                              ("EXPLAIN ANALYZE SELECT id FROM plan_rel.items p;", "0A000")):
            rows, state, message, _, _ = runner.decode_wire_result(
                client.simple_query(server["sock"], sql))
            assert state == expected and not rows, (sql, state, rows, message)
        assert query("SELECT 42;") == [["42"]]
        print("[EXPLAIN RELATION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
