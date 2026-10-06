#!/usr/bin/env python3
"""Caller CTEs are lexical relations, not namespaces of stored query bodies."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("stored_cte_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, value):
        result = query(sql)
        assert result[1] is None and result[0] == [[str(value)]], (sql, result)
        assert result[3] == ["id"] and result[5] == [23] and result[4] == "SELECT 1", result

    try:
        for sql in ("CREATE TABLE stored_cte_base(id INT);",
                    "INSERT INTO stored_cte_base VALUES(99);",
                    "CREATE VIEW stored_cte_view AS SELECT id FROM stored_cte_base;",
                    "CREATE VIEW stored_cte_chain AS SELECT id FROM stored_cte_view;"):
            assert query(sql)[1] is None, sql
        expect("SELECT id FROM stored_cte_view;", 99)
        expect("WITH stored_cte_base AS (SELECT 1 AS id) SELECT id FROM stored_cte_view;", 99)
        expect("WITH stored_cte_base AS (SELECT 1 AS id) "
               "SELECT id FROM stored_cte_view WHERE id=99;", 99)
        expect("WITH stored_cte_base AS (SELECT 1 AS id) SELECT id FROM stored_cte_chain;", 99)
        expect("SELECT stored_cte_view.id FROM stored_cte_view;", 99)
        expect("SELECT v.id FROM stored_cte_view AS v WHERE v.id=99;", 99)
        expect("SELECT id FROM stored_cte_view WHERE id=1 UNION ALL SELECT 7 AS id;", 7)
        assert query("CREATE TABLE stored_cte_inserted(id INT PRIMARY KEY);")[1] is None
        expect("WITH inserted AS (INSERT INTO stored_cte_inserted VALUES(1) RETURNING id) "
               "SELECT id FROM stored_cte_view;", 99)
        expect("SELECT id FROM stored_cte_inserted;", 1)
        # A CTE can itself shadow a stored view in the caller's lexical scope.
        expect("WITH stored_cte_view AS (SELECT 3 AS id) SELECT id FROM stored_cte_view;", 3)
        expect("SELECT id FROM stored_cte_view;", 99)
        # View execution inside a CTE body must also be isolated, while the
        # ordinary same-query CTE references remain inherited.
        expect("WITH stored_cte_base AS (SELECT 1 AS id), "
               "result AS (SELECT id FROM stored_cte_view) SELECT id FROM result;", 99)
        expect("WITH c AS (SELECT 1 AS id), result AS (SELECT id+1 AS id FROM c) SELECT id FROM result;", 2)
        for sql in (
                "CREATE FUNCTION stored_cte_reader() RETURNS INT LANGUAGE plpgsql AS "
                "$$BEGIN RETURN 99; END;$$;",
                "PREPARE stored_cte_plan AS SELECT id FROM stored_cte_base;"):
            assert query(sql)[1] is None, sql
        expect("WITH stored_cte_base AS (SELECT 1 AS id) SELECT stored_cte_reader() AS id;", 99)
        expect("WITH stored_cte_reader AS (SELECT 1 AS id) SELECT stored_cte_reader() AS id;", 99)
        expect("EXECUTE stored_cte_plan;", 99)
        expect("SELECT id FROM stored_cte_base;", 99)
        for sql in ("CREATE TABLE stored_cte_text(id INT, payload TEXT);",
                    "INSERT INTO stored_cte_text VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,'');",
                    "CREATE VIEW stored_cte_text_view AS SELECT id,payload FROM stored_cte_text;"):
            assert query(sql)[1] is None, sql
        result = query("WITH stored_cte_text AS (SELECT 8 AS id) "
                       "SELECT v.payload,v.id FROM stored_cte_text_view v ORDER BY v.id;")
        assert result == ([["a b", "1"], [None, "2"], ["NULL", "3"], ["", "4"]],
                          None, "", ["payload", "id"], "SELECT 4", [25, 23]), result
        result = query("SELECT payload,id FROM stored_cte_text_view WHERE id=0;")
        assert result == ([], None, "", ["payload", "id"], "SELECT 0", [25, 23]), result
        print("[CTE STORED QUERY NAMESPACE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
