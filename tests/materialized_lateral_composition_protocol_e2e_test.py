#!/usr/bin/env python3
"""CTE/derived/LATERAL materializers must not reuse a live temporary relation."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("materialized_lateral_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, names, types):
        result = query(sql)
        assert result[1] is None, (sql, result)
        assert result[0] == rows and result[3] == names and result[5] == types, (sql, result)
        assert result[4] == f"SELECT {len(rows)}", (sql, result)

    try:
        for sql in (
                "CREATE TABLE mat_lat_source(id INT, txt TEXT);",
                "CREATE TABLE mat_lat_empty(id INT, txt TEXT);",
                "INSERT INTO mat_lat_source VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,'');"):
            assert query(sql)[1] is None, sql
        expected = [["1", "a b", "11"], ["2", None, "12"], ["3", "NULL", "13"], ["4", "", "14"]]
        cases = (
            ("WITH c AS (SELECT 1 AS id) SELECT c.id,x.n FROM c "
             "CROSS JOIN LATERAL (SELECT c.id+1 AS n) x;",
             [["1", "2"]], ["id", "n"], [23, 23]),
            ("SELECT c.id,x.n FROM (SELECT 1 AS id) c "
             "CROSS JOIN LATERAL (SELECT c.id+1 AS n) x;",
             [["1", "2"]], ["id", "n"], [23, 23]),
            ("WITH c AS (SELECT id,txt FROM mat_lat_source) "
             "SELECT id,txt,n FROM c CROSS JOIN LATERAL (SELECT id+10 AS n) x ORDER BY id;",
             expected, ["id", "txt", "n"], [23, 25, 23]),
            ("SELECT id,txt,n FROM (SELECT id,txt FROM mat_lat_source) c "
             "CROSS JOIN LATERAL (SELECT id+10 AS n) x ORDER BY id;",
             expected, ["id", "txt", "n"], [23, 25, 23]),
            ("WITH c AS (SELECT id,txt FROM mat_lat_empty) "
             "SELECT id,txt,n FROM c CROSS JOIN LATERAL (SELECT id+10 AS n) x;",
             [], ["id", "txt", "n"], [23, 25, 23]),
            ("WITH a AS (SELECT id,txt FROM mat_lat_source), b AS (SELECT id,txt FROM a) "
             "SELECT b.id,b.txt,x.n FROM b CROSS JOIN LATERAL (SELECT b.id+10 AS n) x ORDER BY b.id;",
             expected, ["id", "txt", "n"], [23, 25, 23]),
            ("WITH c AS (SELECT id,txt FROM mat_lat_source) SELECT id,txt,n,m FROM c "
             "CROSS JOIN LATERAL (SELECT id+10 AS n) x "
             "CROSS JOIN LATERAL (SELECT n+10 AS m) y ORDER BY id;",
             [row + [str(int(row[0])+20)] for row in expected],
             ["id", "txt", "n", "m"], [23, 25, 23, 23]),
            ("SELECT id,txt,n FROM (SELECT id,txt FROM mat_lat_source) c "
             "LEFT JOIN LATERAL (SELECT id+10 AS n WHERE id=1) x ON true ORDER BY id;",
             [["1", "a b", "11"], ["2", None, None], ["3", "NULL", None], ["4", "", None]],
             ["id", "txt", "n"], [23, 25, 23]),
        )
        for case in cases:
            expect(*case)

        # A user's temp relation can have the same logical name as the old
        # first internal candidate. It must neither be overwritten nor dropped.
        for sql in ("CREATE TEMP TABLE __cte_0(keep TEXT);",
                    "INSERT INTO __cte_0 VALUES('keep me');"):
            assert query(sql)[1] is None, sql
        expect("WITH c AS (SELECT 1 AS id) SELECT c.id,x.n FROM c "
               "CROSS JOIN LATERAL (SELECT c.id+1 AS n) x;",
               [["1", "2"]], ["id", "n"], [23, 23])
        expect("SELECT keep FROM __cte_0;", [["keep me"]], ["keep"], [25])
        assert query("DROP TABLE __cte_0;")[1] is None
        # Public relation names must not be shadowed by the internal allocator.
        for sql in ("CREATE TABLE __cte_0(keep TEXT);",
                    "INSERT INTO __cte_0 VALUES('public stays');"):
            assert query(sql)[1] is None, sql
        expect("WITH c AS (SELECT 1 AS id) SELECT c.id,t.keep FROM c CROSS JOIN __cte_0 t;",
               [["1", "public stays"]], ["id", "keep"], [23, 25])
        expect("SELECT keep FROM __cte_0;", [["public stays"]], ["keep"], [25])
        assert query("DROP TABLE __cte_0;")[1] is None
        for sql in (
                "CREATE MATERIALIZED VIEW __cte_0 AS SELECT id,txt FROM mat_lat_source WITH NO DATA;",
                "CREATE MATERIALIZED VIEW __cte_1 AS SELECT id,txt FROM mat_lat_source;"):
            assert query(sql)[1] is None, sql
        expect("WITH c AS (SELECT 1 AS id) SELECT c.id,x.n FROM c "
               "CROSS JOIN LATERAL (SELECT c.id+1 AS n) x;",
               [["1", "2"]], ["id", "n"], [23, 23])
        unpopulated = query("SELECT id FROM __cte_0;")
        assert unpopulated[1] == "55000" and unpopulated[4] is None, unpopulated
        expect("SELECT id,txt FROM __cte_1 ORDER BY id;",
               [row[:2] for row in expected], ["id", "txt"], [23, 25])
        # The same collision check must respect a non-public search path.
        for sql in (
                "CREATE SCHEMA mat_scope;",
                "CREATE MATERIALIZED VIEW mat_scope.__cte_0 AS "
                "SELECT id,txt FROM mat_lat_source WITH NO DATA;",
                "SET search_path TO mat_scope,public;"):
            assert query(sql)[1] is None, sql
        expect("WITH c AS (SELECT 1 AS id) SELECT c.id,x.n FROM c "
               "CROSS JOIN LATERAL (SELECT c.id+1 AS n) x;",
               [["1", "2"]], ["id", "n"], [23, 23])
        unpopulated = query("SELECT id FROM mat_scope.__cte_0;")
        assert unpopulated[1] == "55000" and unpopulated[4] is None, unpopulated
        expect("SELECT id,txt FROM mat_lat_source ORDER BY id;",
               [row[:2] for row in expected], ["id", "txt"], [23, 25])
        print("[MATERIALIZED LATERAL COMPOSITION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
