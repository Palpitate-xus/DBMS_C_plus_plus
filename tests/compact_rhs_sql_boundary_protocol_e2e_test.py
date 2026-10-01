#!/usr/bin/env python3
"""Legacy API literal disambiguation must not change SQL RHS expressions."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "compact_rhs_sql_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE rhs_sql(id INT PRIMARY KEY,code TEXT,d DATE);")
        query("INSERT INTO rhs_sql VALUES(1,'1+2','2024-01-01'),(2,'2*3','2025-02-02');")
        assert query("SELECT id FROM rhs_sql WHERE code='1+'||'2' ORDER BY id;") == [["1"]]
        assert query("SELECT id FROM rhs_sql WHERE d=CAST('2024-01-01' AS date) ORDER BY id;") == [["1"]]
        assert query("SELECT id FROM rhs_sql WHERE id=1+1 ORDER BY id;") == [["2"]]
        query("UPDATE rhs_sql SET code='updated' WHERE code='1+'||'2';")
        assert query("SELECT id,code FROM rhs_sql ORDER BY id;") == [["1", "updated"], ["2", "2*3"]]
        query("DELETE FROM rhs_sql WHERE d=CAST('2025-02-02' AS date);")
        assert query("SELECT id,code,d FROM rhs_sql ORDER BY id;") == [["1", "updated", "2024-01-01"]]
        query("CREATE TABLE delete_computed(id INT PRIMARY KEY,n NUMERIC,d DATE);")
        predicates = [
            "id=CAST(2 AS integer)",
            "id=1+1",
            "d=CAST('2025-02-02' AS date)",
            "CASE WHEN id=2 THEN true ELSE false END",
            "n/0.5>2",
            "coalesce(1,1/0)=1 AND id=2",
        ]
        for predicate in predicates:
            query("TRUNCATE delete_computed;")
            query("INSERT INTO delete_computed VALUES(1,1,'2024-01-01'),(2,2,'2025-02-02'),(3,NULL,NULL);")
            query("DELETE FROM delete_computed WHERE " + predicate + ";")
            rows = query("SELECT id FROM delete_computed ORDER BY id;")
            assert rows == [["1"], ["3"]], (predicate, rows)
        query("TRUNCATE delete_computed;")
        query("INSERT INTO delete_computed VALUES(1,1,'2024-01-01'),(2,2,'2025-02-02'),(3,NULL,NULL);")
        query("DELETE FROM delete_computed WHERE (id=1+1 OR id=4-1) AND id>1;")
        assert query("SELECT id FROM delete_computed ORDER BY id;") == [["1"]]
        for predicate, expected in [("n/0>1", "22012"),
                                    ("id=CAST('bad' AS integer)", "22P02")]:
            _, state, message, _, _ = runner.decode_wire_result(client.simple_query(
                server["sock"], "DELETE FROM delete_computed WHERE " + predicate + ";"))
            assert state == expected, (predicate, state, message)
            assert query("SELECT id FROM delete_computed ORDER BY id;") == [["1"]]
        assert query("SELECT 42;") == [["42"]]
        print("[COMPACT RHS SQL BOUNDARY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
