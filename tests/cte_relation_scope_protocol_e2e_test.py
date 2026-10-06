#!/usr/bin/env python3
"""CTE storage bindings must not rewrite columns, aliases or nested scopes."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("cte_scope_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, headers, types):
        result = query(sql)
        assert result[1] is None, (sql, result)
        assert result[0] == rows and result[3] == headers and result[5] == types, (sql, result)
        assert result[4] == f"SELECT {len(rows)}", (sql, result)

    try:
        for sql in ("CREATE TABLE cte_scope_rows(id INT,txt TEXT);",
                    "INSERT INTO cte_scope_rows VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,'');",
                    "CREATE TABLE c(id INT);", "INSERT INTO c VALUES(99);"):
            assert query(sql)[1] is None, sql
        cases = (
            ("WITH id AS (SELECT 1 AS id) SELECT id FROM id;",
             [["1"]], ["id"], [23]),
            ("WITH c AS (SELECT 1 AS c) SELECT c AS c FROM c;",
             [["1"]], ["c"], [23]),
            ("WITH c AS (SELECT 1 AS id) SELECT t.id AS c FROM cte_scope_rows t ORDER BY t.id;",
             [["1"], ["2"], ["3"], ["4"]], ["c"], [23]),
            ("WITH c AS (SELECT id AS c,txt FROM cte_scope_rows) SELECT c,txt FROM c ORDER BY c;",
             [["1", "a b"], ["2", None], ["3", "NULL"], ["4", ""]], ["c", "txt"], [23, 25]),
            ("WITH coalesce AS (SELECT id FROM cte_scope_rows) "
             "SELECT coalesce(id,9) AS coalesce FROM coalesce ORDER BY id;",
             [["1"], ["2"], ["3"], ["4"]], ["coalesce"], [23]),
            ("WITH c AS (SELECT 1 AS id), id AS (SELECT c.id FROM c) SELECT id.id FROM id;",
             [["1"]], ["id"], [23]),
            ("WITH c AS (SELECT 1 AS id) SELECT a.id,b.id FROM c a CROSS JOIN c b;",
             [["1", "1"]], ["id", "id"], [23, 23]),
            ("WITH c AS (SELECT 1 AS id) SELECT c.id,t.id FROM c CROSS JOIN public.c t;",
             [["1", "99"]], ["id", "id"], [23, 23]),
            ("WITH c AS (SELECT 1 AS id) SELECT c.id+1,t.id+1 FROM c CROSS JOIN public.c t;",
             [["2", "100"]], ["?column?", "?column?"], [23, 23]),
            ("WITH c AS (SELECT 1 AS id) SELECT c.id,t.id FROM c CROSS JOIN public.c t "
             "WHERE c.id=1 AND t.id=99;",
             [["1", "99"]], ["id", "id"], [23, 23]),
            ("WITH c AS (SELECT 1 AS id) SELECT sum(c.id),sum(t.id) FROM c CROSS JOIN public.c t;",
             [["1", "99"]], ["sum", "sum"], [20, 20]),
            ("WITH c AS (SELECT id FROM public.c) SELECT id FROM c;",
             [["99"]], ["id"], [23]),
            ("WITH c AS (SELECT 1 AS id), d AS (WITH c AS (SELECT 2 AS id) SELECT id FROM c) "
             "SELECT d.id,c.id FROM d CROSS JOIN c;",
             [["2", "1"]], ["id", "id"], [23, 23]),
            ("WITH c AS (SELECT 1 AS id), d AS (WITH c AS (SELECT id+1 AS id FROM c) SELECT id FROM c) "
             "SELECT d.id,c.id FROM d CROSS JOIN c;",
             [["2", "1"]], ["id", "id"], [23, 23]),
            ("WITH nested_scope AS (WITH nested_scope AS (SELECT 7 AS id) "
             "SELECT id FROM nested_scope) SELECT id FROM nested_scope;",
             [["7"]], ["id"], [23]),
            ("WITH c AS (SELECT 1 AS id) SELECT id FROM c UNION ALL SELECT id FROM c;",
             [["1"], ["1"]], ["id"], [23]),
            ("WITH RECURSIVE id(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM id WHERE id<3) "
             "SELECT id FROM id ORDER BY id;",
             [["1"], ["2"], ["3"]], ["id"], [23]),
            ("WITH RECURSIVE id AS (SELECT 1 AS id UNION ALL SELECT id FROM cte_scope_rows WHERE id=1) "
             "SELECT id FROM id;",
             [["1"], ["1"]], ["id"], [23]),
            ('WITH "Case" AS (SELECT 1 AS "Case") SELECT "Case" FROM "Case";',
             [["1"]], ["Case"], [23]),
            ('WITH "Mixed CTE" AS (SELECT 1 AS id) SELECT id FROM "Mixed CTE";',
             [["1"]], ["id"], [23]),
            ("WITH c AS (SELECT 1 AS id) SELECT c.*,x.n FROM c CROSS JOIN LATERAL (SELECT c.id+1 AS n)x;",
             [["1", "2"]], ["id", "n"], [23, 23]),
        )
        for case in cases:
            expect(*case)
            # Statement-local CTE bindings cannot hide the real table afterward.
            expect("SELECT id FROM c;", [["99"]], ["id"], [23])
        for sql, state in (
                ("WITH c AS (SELECT 1 AS id) SELECT c.id FROM c AS hidden;", "42P01"),
                ("WITH c AS (SELECT 1 AS id) SELECT missing FROM c;", "42703"),
                ("WITH c AS (SELECT 1 AS id), d AS (SELECT missing FROM c) SELECT id FROM d;", "42703"),
                ("WITH missing_scope AS (SELECT id FROM missing_scope) SELECT id FROM missing_scope;", "42P01")):
            result = query(sql)
            assert result[1] == state and result[0] == [] and result[4] is None, (sql, result)
            expect("SELECT id FROM c;", [["99"]], ["id"], [23])
        print("[CTE RELATION SCOPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
