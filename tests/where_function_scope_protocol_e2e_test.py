#!/usr/bin/env python3
"""WHERE function analysis must not consume a following query or FROM item."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("where_scope_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in ("CREATE TABLE where_scope_source(id INT, txt TEXT);",
                    "CREATE TABLE where_scope_empty(id INT);",
                    "INSERT INTO where_scope_source VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,'');"):
            assert query(sql)[1] is None, sql
        for sql in (
                "WITH c AS (SELECT id,txt FROM where_scope_source WHERE id<0) "
                "SELECT id,txt,n FROM c CROSS JOIN LATERAL (SELECT id+10 AS n) x;",
                "SELECT id,txt,n FROM (SELECT id,txt FROM where_scope_source WHERE id<0) c "
                "CROSS JOIN LATERAL (SELECT id+10 AS n) x;"):
            result = query(sql)
            assert result[1] is None and result[0] == [], (sql, result)
            assert result[3] == ["id", "txt", "n"] and result[5] == [23, 25, 23], (sql, result)
            assert result[4] == "SELECT 0", (sql, result)
        for sql in (
                "WITH c AS (SELECT id,txt FROM where_scope_source WHERE id=1) "
                "SELECT id,txt,n FROM c CROSS JOIN LATERAL (SELECT id+10 AS n) x;",
                "SELECT id,txt,n FROM (SELECT id,txt FROM where_scope_source WHERE id=1) c "
                "CROSS JOIN LATERAL (SELECT id+10 AS n) x;"):
            result = query(sql)
            assert result[1] is None and result[0] == [["1", "a b", "11"]], (sql, result)
            assert result[3] == ["id", "txt", "n"] and result[5] == [23, 25, 23], (sql, result)
        # An apparent call inside a literal is not a function reference.
        for literal in ("' where nosuchfn(1)'", "$$ where nosuchfn(1)$$"):
            result = query(f"SELECT {literal} AS marker;")
            assert result[1] is None and result[0] == [[" where nosuchfn(1)"]], result
        # Function analysis is required even on empty inputs and in an
        # unreachable CASE arm; mutation errors must leave all rows unchanged.
        for sql in (
                "SELECT id FROM where_scope_source WHERE nosuchfn(id)>0;",
                "SELECT id FROM where_scope_empty WHERE nosuchfn(id)>0;",
                "SELECT id FROM where_scope_source WHERE coalesce(nosuchfn(id),1)>0;",
                "SELECT id FROM where_scope_source WHERE CASE WHEN false THEN nosuchfn(id)>0 ELSE true END;",
                "SELECT id FROM where_scope_source WHERE nosuchfn /* comment */ (id)>0;",
                "WITH c AS (SELECT id FROM where_scope_empty WHERE nosuchfn(id)>0) SELECT id FROM c;",
                "DELETE FROM where_scope_source WHERE nosuchfn(id)>0;",
                "UPDATE where_scope_source SET id=99 WHERE nosuchfn(id)>0;"):
            result = query(sql)
            assert result[1] == "42883" and result[0] == [] and result[4] is None, (sql, result)
            assert "nosuchfn(integer)" in result[2], (sql, result)
            unchanged = query("SELECT id,txt FROM where_scope_source ORDER BY id;")
            assert unchanged[1] is None and unchanged[0] == [
                ["1", "a b"], ["2", None], ["3", "NULL"], ["4", ""]], unchanged
        for sql in (
                "CREATE MATERIALIZED VIEW where_scope_mv AS SELECT id FROM where_scope_source WITH NO DATA;",
                "CREATE SCHEMA where_scope_ns;",
                "CREATE MATERIALIZED VIEW where_scope_ns.mv AS SELECT id FROM where_scope_source WITH NO DATA;"):
            assert query(sql)[1] is None, sql
        for relation in ("where_scope_mv", "public.where_scope_mv", "where_scope_ns.mv"):
            result = query(f"SELECT id FROM {relation} WHERE nosuchfn(id)>0;")
            assert result[1] == "42883" and result[4] is None, (relation, result)
            assert "nosuchfn(integer)" in result[2], (relation, result)
            # The real unpopulated-view access gate remains in effect, and
            # neither error may disconnect or poison an autocommit session.
            result = query(f"SELECT id FROM {relation};")
            assert result[1] == "55000" and result[4] is None, (relation, result)
            alive = query("SELECT 1 AS alive;")
            assert alive[1] is None and alive[0] == [["1"]], alive
        print("[WHERE FUNCTION SCOPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
