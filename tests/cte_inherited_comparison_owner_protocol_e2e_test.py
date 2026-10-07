#!/usr/bin/env python3
"""Regression gate for catalog preparation stealing a materialized CTE source.

The larger cte_prepared_owner_protocol_e2e_test.py remains an OPEN gate with
its original strong assertions for separate, older CASE/routine consumers.
"""

import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("inherited_cte_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference18" in sys.argv
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server["sock"]

    def query(sql, rows=None, headers=None, types=None, state=None):
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        print("CTE_INHERITED_COMPARISON_OWNER", sql, result, flush=True)
        assert result[1] == state, (sql, result, state)
        if rows is not None:
            assert result[0] == rows and result[4] == f"SELECT {len(rows)}", (sql, result, rows)
        if headers is not None:
            assert result[3] == headers, (sql, result, headers)
        if types is not None:
            assert result[5] == types, (sql, result, types)
        if state is not None:
            assert result[0] == [] and result[4] is None, (sql, result)

    try:
        schema = "inherited_cte_" + uuid.uuid4().hex[:18]
        if reference:
            query("BEGIN")
        query('CREATE SCHEMA "' + schema + '"')
        query(('SET LOCAL' if reference else 'SET') + ' search_path TO "' + schema + '",pg_catalog')
        query("CREATE TABLE cte_scope_rows(id INT,txt TEXT)")
        query("INSERT INTO cte_scope_rows VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,'')")
        cases = [
            ("WITH RECURSIVE id(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM id WHERE id<3) "
             "SELECT id FROM id ORDER BY id;", [["1"], ["2"], ["3"]], ["id"], [23]),
            ("WITH RECURSIVE id AS (SELECT 1 AS id UNION ALL SELECT id FROM cte_scope_rows WHERE id=1) "
             "SELECT id FROM id;", [["1"], ["1"]], ["id"], [23]),
            ('WITH RECURSIVE "Mixed CTE"(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM "Mixed CTE" WHERE id<3) '
             'SELECT id FROM "Mixed CTE" ORDER BY id;', [["1"], ["2"], ["3"]], ["id"], [23]),
            ("WITH RECURSIVE walk(id) AS (SELECT 1 UNION ALL "
             "SELECT CASE WHEN id=1 THEN 2 ELSE 3 END FROM walk WHERE id<3) "
             "SELECT id FROM walk ORDER BY id;", [["1"], ["2"], ["3"]], ["id"], [23]),
            ("WITH RECURSIVE walk(id,txt) AS (SELECT 1,CAST(NULL AS TEXT) UNION ALL "
             "SELECT id+1,CASE WHEN id=1 THEN '' ELSE 'NULL' END FROM walk WHERE id<3) "
             "SELECT id,txt FROM walk ORDER BY id;", [["1", None], ["2", ""], ["3", "NULL"]], ["id", "txt"], [23, 25]),
            ("WITH c AS (SELECT 1 AS id), d AS (WITH c AS (SELECT id+1 AS id FROM c) SELECT id FROM c) "
             "SELECT d.id,c.id FROM d CROSS JOIN c;", [["2", "1"]], ["id", "id"], [23, 23]),
            ("WITH coalesce AS (SELECT 1 AS id) SELECT coalesce(id,9) AS id FROM coalesce;", [["1"]], ["id"], [23]),
        ]
        for case in cases:
            query(*case)
        # A conflicting catalog name must neither be needed to make the first
        # recursive query pass nor replace its actual work-table source.
        query("CREATE TABLE id(id INT)")
        query("INSERT INTO id VALUES(99)")
        query(*cases[0])
        query("WITH id AS (SELECT 1 AS id) SELECT CASE WHEN b.id=99 THEN b.id ELSE 0 END AS id FROM "
              '"' + schema + '".id b;', [["99"]], ["id"], [23])
        query("CREATE SEQUENCE cte_walk_sequence")
        query("WITH RECURSIVE walk(id) AS (SELECT CAST(nextval('cte_walk_sequence') AS INT) UNION ALL "
              "SELECT CAST(nextval('cte_walk_sequence') AS INT) FROM walk WHERE id<3) "
              "SELECT id FROM walk ORDER BY id;", [["1"], ["2"], ["3"]], ["id"], [23])
        query("SELECT currval('cte_walk_sequence');", [["3"]], ["currval"], [20])
        query("CREATE FUNCTION cte_owner_spi_reader() RETURNS INT LANGUAGE plpgsql AS "
              "$$DECLARE n INT; BEGIN SELECT id INTO n FROM id WHERE id=99; RETURN n; END;$$")
        query("WITH id AS (SELECT 1 AS id) SELECT cte_owner_spi_reader() AS id;", [["99"]], ["id"], [23])
        query("WITH cte_owner_spi_reader AS (SELECT 1 AS id) SELECT cte_owner_spi_reader() AS id;", [["99"]], ["id"], [23])
        query("SELECT id FROM id;", [["99"]], ["id"], [23])
        for sql, state in [
            ("WITH c AS (SELECT 1 AS id) SELECT c.id FROM c AS hidden;", "42P01"),
            ("WITH c AS (SELECT 1 AS id) SELECT missing FROM c;", "42703"),
            ("WITH c AS (SELECT 1 AS id), d AS (SELECT missing FROM c) SELECT id FROM d;", "42703"),
            ("WITH missing_scope AS (SELECT id FROM missing_scope) SELECT id FROM missing_scope;", "42P01"),
        ]:
            if reference:
                query("SAVEPOINT inherited_cte_error")
            query(sql, state=state)
            if reference:
                query("ROLLBACK TO inherited_cte_error")
                query("RELEASE inherited_cte_error")
            query("SELECT id FROM id;", [["99"]], ["id"], [23])
            query("SELECT currval('cte_walk_sequence');", [["3"]], ["currval"], [20])
        print("[CTE INHERITED COMPARISON OWNER PROTOCOL E2E] passed", flush=True)
    finally:
        if reference:
            try:
                client.simple_query(sock, "ROLLBACK")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
