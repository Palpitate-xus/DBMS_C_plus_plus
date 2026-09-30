#!/usr/bin/env python3
"""Finite numeric bounds on integer columns retain exact precision."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "integer_numeric_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE integer_numeric (id BIGINT PRIMARY KEY,value INT);")
        query("INSERT INTO integer_numeric VALUES (-1,7),(0,7),(1,7),(2,7),"
              "(9007199254740992,7),(9007199254740993,7);")
        query("CREATE INDEX integer_numeric_value ON integer_numeric(value);")
        for predicate, expected in (("id=1.0", 1), ("id=1e0", 1),
                                    ("id=1.1", 0), ("id<1.1", 3),
                                    ("id>9007199254740992.99999999", 1),
                                    ("id=9007199254740993.0 AND value=7.0", 1)):
            sql = "SELECT id FROM integer_numeric WHERE " + predicate + ";"
            assert len(query(sql)) == expected, sql
            document = json.loads("\n".join(row[0] for row in query(
                "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + sql)))
            assert document["actualRows"] == expected, (sql, document)
        # SQL EXPLAIN's separate WHERE parser currently drops BETWEEN;
        # that reproduced OPT-16 gap is not fixed by numeric comparisons.
        # Keep its real SELECT result here, with raw-plan BETWEEN execution
        # checked in C++ and PostgreSQL precision checked in the diff case.
        assert query("SELECT id FROM integer_numeric WHERE id BETWEEN "
                     "9007199254740992.9 AND 9007199254740993.1;") == [["9007199254740993"]]
        assert query("SELECT 42;") == [["42"]]
        print("[INTEGER NUMERIC PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
