#!/usr/bin/env python3
"""Materialized FROM factors and CTE AS use lexical, not whitespace boundaries."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("factor_boundary_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        cases = (
            ("SELECT d.id FROM(SELECT 1 AS id)d WHERE d.id=1;", [["1"]], ["id"], [23]),
            ("SELECT d.* FROM(SELECT 1 AS id)d;", [["1"]], ["id"], [23]),
            ("SELECT d.id FROM(SELECT 1 AS id)d GROUP BY d.id;", [["1"]], ["id"], [23]),
            ("SELECT d.id,x.n FROM(SELECT 1 AS id)d CROSS JOIN LATERAL(SELECT d.id+1 AS n)x;",
             [["1", "2"]], ["id", "n"], [23, 23]),
            ("SELECT d.id,e.id FROM(SELECT 1 AS id)d JOIN(SELECT 1 AS id)e ON d.id=e.id;",
             [["1", "1"]], ["id", "id"], [23, 23]),
            ("WITH c AS(SELECT 1 AS id) SELECT id FROM c;", [["1"]], ["id"], [23]),
            ("WITH c(id) AS(SELECT 1) SELECT c.id FROM c;", [["1"]], ["id"], [23]),
            ("WITH c AS(SELECT 1 AS id), d AS(SELECT id+1 AS id FROM c) SELECT id FROM d;",
             [["2"]], ["id"], [23]),
            ('WITH "with as name" AS(SELECT 1 AS id) SELECT id FROM "with as name";',
             [["1"]], ["id"], [23]),
            ("WITH RECURSIVE id(id) AS(SELECT 1 UNION ALL SELECT id+1 FROM id WHERE id<3) "
             "SELECT id FROM id ORDER BY id;", [["1"], ["2"], ["3"]], ["id"], [23]),
        )
        for sql, rows, headers, types in cases:
            result = runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)
            assert result[1] is None and result[0] == rows, (sql, result)
            assert result[3] == headers and result[5] == types, (sql, result)
            assert result[4] == f"SELECT {len(rows)}", (sql, result)
        print("[MATERIALIZED FACTOR BOUNDARY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
