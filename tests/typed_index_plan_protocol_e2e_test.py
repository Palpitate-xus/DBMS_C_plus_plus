#!/usr/bin/env python3
"""Executed plans use the same type-specific keys as index maintenance."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "typed_plan_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def check(select, expected):
        assert query(select) == expected, select
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + select)))
        assert document["actualRows"] == len(expected), (select, document)

    try:
        cases = (("DOUBLE PRECISION", "1.5", "1.50"),
                 # Use an exactly representable REAL here. SQL promotion of
                 # REAL versus a numeric literal is a separate analyzer gap.
                 ("REAL", "0.5", "0.5000000000"),
                 ("DOUBLE PRECISION", "'-0'", "0.0"),
                 ("MONEY", "'1.50'", "'1.500'"),
                 ("UUID", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
                  "'{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}'"),
                 ("CHAR(16)", "'a'", "'a   '"))
        for family in ("btree", "hash", "bloom"):
            for i, (data_type, stored, literal) in enumerate(cases):
                table = "typed_plan_" + family + str(i)
                query("CREATE TABLE " + table + " (id INT PRIMARY KEY,value " + data_type + ");")
                query("INSERT INTO " + table + " VALUES (1," + stored + ");")
                query("CREATE INDEX " + table + "_value ON " + table + " USING " + family + "(value);")
                if family == "btree":
                    check("SELECT id FROM " + table + " WHERE value=" + literal + ";", [["1"]])
                check("SELECT id FROM " + table + " WHERE id=1 AND value=" + literal + ";", [["1"]])
        query("CREATE TABLE typed_plan_money_pk (id MONEY PRIMARY KEY);")
        query("INSERT INTO typed_plan_money_pk VALUES ('0.00');")
        check("SELECT id FROM typed_plan_money_pk WHERE id='0.00';", [["$0.00"]])
        query("CREATE TABLE typed_plan_varchar (id INT PRIMARY KEY,value VARCHAR(16));")
        query("INSERT INTO typed_plan_varchar VALUES (1,'a');")
        query("CREATE INDEX typed_plan_varchar_value ON typed_plan_varchar(value);")
        check("SELECT id FROM typed_plan_varchar WHERE value='a   ';", [])
        assert query("SELECT 42;") == [["42"]]
        print("[TYPED INDEX PLAN PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
