#!/usr/bin/env python3
"""Materializing a CTE/derived source must retain qualified columns and stars."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("mat_alias_pgdiff", root / "tests/compat/pg_diff_runner.py")
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
        for sql in ("CREATE TABLE mat_alias_source(id INT,txt TEXT);",
                    "INSERT INTO mat_alias_source VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,'');"):
            assert query(sql)[1] is None, sql
        for prefix, source in (
                ("", "(SELECT 1 AS id) d"),
                ("WITH d AS (SELECT 1 AS id) ", "d")):
            expect(prefix + "SELECT d.id,x.id FROM " + source +
                   " CROSS JOIN LATERAL (SELECT d.id+1 AS id) x;",
                   [["1", "2"]], ["id", "id"], [23, 23])
            expect(prefix + "SELECT d.*,x.n FROM " + source +
                   " CROSS JOIN LATERAL (SELECT d.id+1 AS n) x;",
                   [["1", "2"]], ["id", "n"], [23, 23])
            expect(prefix + "SELECT x.*,d.* FROM " + source +
                   " CROSS JOIN LATERAL (SELECT d.id+1 AS n) x;",
                   [["2", "1"]], ["n", "id"], [23, 23])
            expect(prefix + "SELECT d.*,x.*,d.id FROM " + source +
                   " CROSS JOIN LATERAL (SELECT d.id+1 AS id) x;",
                   [["1", "2", "1"]], ["id", "id", "id"], [23, 23, 23])
            result = query(prefix + "SELECT id FROM " + source +
                           " CROSS JOIN LATERAL (SELECT d.id+1 AS id) x;")
            assert result[1] == "42702" and result[0] == [] and result[4] is None, result

        rows = [["1", "a b", "11"], ["2", None, "12"],
                ["3", "NULL", "13"], ["4", "", "14"]]
        for prefix, source in (
                ("", "(SELECT id,txt FROM mat_alias_source) AS d"),
                ("WITH d AS (SELECT id,txt FROM mat_alias_source) ", "d")):
            expect(prefix + "SELECT d.*,x.n FROM " + source +
                   " CROSS JOIN LATERAL (SELECT d.id+10 AS n) x ORDER BY d.id;",
                   rows, ["id", "txt", "n"], [23, 25, 23])
            expect(prefix + "SELECT d.id,d.txt FROM " + source + " ORDER BY d.id;",
                   [row[:2] for row in rows], ["id", "txt"], [23, 25])
            expect(prefix + "SELECT d.* FROM " + source + " ORDER BY d.id;",
                   [row[:2] for row in rows], ["id", "txt"], [23, 25])
            expect(prefix + "SELECT d.id FROM " + source + " WHERE d.id=1;",
                   [["1"]], ["id"], [23])
            expect(prefix + "SELECT d.id FROM " + source + " GROUP BY d.id ORDER BY d.id;",
                   [["1"], ["2"], ["3"], ["4"]], ["id"], [23])
            expect(prefix + "SELECT d.id,t.id FROM " + source +
                   " JOIN mat_alias_source t ON d.id=t.id ORDER BY d.id;",
                   [["1", "1"], ["2", "2"], ["3", "3"], ["4", "4"]],
                   ["id", "id"], [23, 23])
        for prefix, source in (
                ("", '(SELECT 1 AS "MixedId") d'),
                ('WITH d AS (SELECT 1 AS "MixedId") ', "d")):
            expect(prefix + 'SELECT d."MixedId",x.n FROM ' + source +
                   ' CROSS JOIN LATERAL (SELECT d."MixedId"+1 AS n) x;',
                   [["1", "2"]], ["MixedId", "n"], [23, 23])
        expect("SELECT d.*,x.n FROM (SELECT id,txt FROM mat_alias_source WHERE id<0) d "
               "CROSS JOIN LATERAL (SELECT d.id+10 AS n) x;",
               [], ["id", "txt", "n"], [23, 25, 23])
        print("[MATERIALIZED ALIAS SCOPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
