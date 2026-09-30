#!/usr/bin/env python3
"""Native plain projection publishes real cells instead of reparsing display."""

from collections import Counter
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "native_projection_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return Counter(tuple(row) for row in rows)

    try:
        query("CREATE TABLE native_result(v NUMERIC,t TEXT,k INT);")
        query("CREATE TABLE native_result_keys(k INT);")
        query("INSERT INTO native_result_keys VALUES (1);")
        query("INSERT INTO native_result VALUES (0.50,'same',1),(0.500,'same',1),"
              "(2.00,'',1),(NULL,'NULL',1),(NULL,NULL,1);")
        for predicate in ["k IN (SELECT k FROM native_result_keys)",
                          "EXISTS (SELECT k FROM native_result_keys)"]:
            sql = "SELECT t FROM native_result WHERE " + predicate + ";"
            expected = Counter({("same",): 2, ("",): 1, ("NULL",): 1, (None,): 1})
            assert query(sql) == expected, (sql, query(sql), expected)
            sql = "SELECT DISTINCT t FROM native_result WHERE " + predicate + ";"
            expected = Counter({("same",): 1, ("",): 1, ("NULL",): 1, (None,): 1})
            assert query(sql) == expected, (sql, query(sql), expected)
            sql = "SELECT DISTINCT v,t FROM native_result WHERE " + predicate + ";"
            expected = Counter({("0.50", "same"): 1, ("2.00", ""): 1,
                                (None, "NULL"): 1, (None, None): 1})
            assert query(sql) == expected, (sql, query(sql), expected)
        assert query("SELECT 42;") == Counter({("42",): 1})
        print("[NATIVE PROJECTION RESULT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
