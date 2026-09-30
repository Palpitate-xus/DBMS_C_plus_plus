#!/usr/bin/env python3
"""Function and parenthesized-expression NULL tests retain SQL truth."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "expression_null_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE expression_null_values(id INT,f INT,t TEXT);")
        query("INSERT INTO expression_null_values VALUES (1,0,''),(2,1,'NULL'),(3,NULL,NULL);")
        assert query("SELECT count(*) FILTER (WHERE abs(f) IS NULL) FROM expression_null_values;") == [["1"]]
        assert query("SELECT count(*) FILTER (WHERE abs(f) IS NOT NULL) FROM expression_null_values;") == [["2"]]
        assert query("SELECT count(*) FILTER (WHERE (f=0) IS NULL) FROM expression_null_values;") == [["1"]]
        assert query("SELECT count(*) FILTER (WHERE (f=0) IS NOT NULL) FROM expression_null_values;") == [["2"]]
        assert query("SELECT id,t FROM expression_null_values WHERE abs(f) IS NULL ORDER BY id;") == [["3", None]]
        assert query("SELECT id,t FROM expression_null_values WHERE (f=0) IS NULL ORDER BY id;") == [["3", None]]
        assert query("SELECT id,t FROM expression_null_values WHERE abs(f) IS NOT NULL AND id=1 ORDER BY id;") == [["1", ""]]
        assert query("SELECT count(*) FILTER (WHERE coalesce(f,0) IS NOT NULL) FROM expression_null_values;") == [["3"]]
        assert query("SELECT count(*) FILTER (WHERE length(t) IS NULL) FROM expression_null_values;") == [["1"]]
        query("CREATE TABLE expression_null_virtual(id INT,f INT,g INT GENERATED ALWAYS AS (f+1) VIRTUAL);")
        query("INSERT INTO expression_null_virtual(id,f) VALUES (1,0),(2,1),(3,NULL);")
        assert query("SELECT id,g FROM expression_null_virtual WHERE g IS NULL ORDER BY id;") == [["3", None]]
        assert query("SELECT id,g FROM expression_null_virtual WHERE g IS NOT NULL ORDER BY id;") == [["1", "1"], ["2", "2"]]
        assert query("SELECT 42;") == [["42"]]
        print("[EXPRESSION NULL PREDICATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
