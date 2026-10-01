#!/usr/bin/env python3
"""Computed UPDATE predicates must preserve unmatched rows and NULL values."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "update_computed_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, tag = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows, tag

    try:
        query("CREATE TABLE update_computed(id INT PRIMARY KEY,code TEXT,n NUMERIC,d DATE);")
        predicates = ["d=CAST('2025-02-02' AS date)", "id=CAST(2 AS integer)",
                      "id=1+1", "CASE WHEN id=2 THEN true ELSE false END",
                      "n/0.5>2", "coalesce(1,1/0)=1 AND id=2"]
        for predicate in predicates:
            query("TRUNCATE update_computed;")
            query("INSERT INTO update_computed VALUES(1,'original',1,'2024-01-01'),(2,'original',2,'2025-02-02'),(3,NULL,NULL,NULL);")
            _, tag = query("UPDATE update_computed SET code='changed' WHERE " + predicate + ";")
            assert tag == "UPDATE 1", (predicate, tag)
            rows, _ = query("SELECT id,code FROM update_computed ORDER BY id;")
            assert rows == [["1", "original"], ["2", "changed"], ["3", None]], (predicate, rows)
        _, tag = query("UPDATE update_computed SET code='overlap' WHERE id=1+1 OR id>1;")
        assert tag == "UPDATE 2", tag
        rows, _ = query("SELECT id,code FROM update_computed ORDER BY id;")
        assert rows == [["1", "original"], ["2", "overlap"], ["3", "overlap"]]
        for predicate, expected in [("n/0>1", "22012"),
                                    ("id=CAST('bad' AS integer)", "22P02")]:
            _, state, message, _, _ = runner.decode_wire_result(client.simple_query(
                server["sock"], "UPDATE update_computed SET code='bad' WHERE " + predicate + ";"))
            assert state == expected, (predicate, state, message)
            assert query("SELECT id,code FROM update_computed ORDER BY id;")[0] == rows
        print("[UPDATE COMPUTED PREDICATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
