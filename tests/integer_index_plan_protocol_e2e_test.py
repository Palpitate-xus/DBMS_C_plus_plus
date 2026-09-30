#!/usr/bin/env python3
"""Integer equality lookups agree between SQL and executed EXPLAIN plans."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "integer_plan_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def analyzed(select):
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + select)))
        assert document["actualRows"] == 1, (select, document)

    try:
        query("CREATE TABLE integer_plan (id BIGINT PRIMARY KEY,value BIGINT);")
        query("INSERT INTO integer_plan VALUES (1,7),(-1,-7),(0,0),(9007199254740993,9);")
        query("CREATE INDEX integer_plan_value ON integer_plan(value);")
        for literal, expected in (("0001", "1"), ("+0001", "1"),
                                  ("-0001", "-1"), ("-0", "0"),
                                  ("+9007199254740993", "9007199254740993")):
            sql = "SELECT id FROM integer_plan WHERE id=" + literal + ";"
            assert query(sql) == [[expected]], sql
            analyzed(sql)
        analyzed("SELECT id FROM integer_plan WHERE id=0001 AND value=+0007;")
        analyzed("SELECT id FROM integer_plan WHERE value=+0007;")
        for family in ("hash", "bloom"):
            table = "integer_plan_" + family
            query("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY,value BIGINT);")
            query("INSERT INTO " + table + " VALUES (1,7),(2,8);")
            query("CREATE INDEX " + table + "_value ON " + table + " USING " + family + "(value);")
            analyzed("SELECT id FROM " + table + " WHERE id=0001 AND value=+0007;")
        query("CREATE TABLE integer_plan_text (id VARCHAR(20) PRIMARY KEY);")
        query("INSERT INTO integer_plan_text VALUES ('0001');")
        analyzed("SELECT id FROM integer_plan_text WHERE id='0001';")
        assert query("SELECT id FROM integer_plan_text WHERE id='1';") == []
        assert query("SELECT 42;") == [["42"]]
        print("[INTEGER INDEX PLAN PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
