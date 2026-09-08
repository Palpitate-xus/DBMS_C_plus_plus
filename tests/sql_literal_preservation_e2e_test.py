#!/usr/bin/env python3
"""SQL preprocessing and projection preserve literal values exactly."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("literal_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ("CREATE TABLE literal_rows (id INT);", "INSERT INTO literal_rows VALUES (1);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
        for literal, expected in [("'a'' fetch first 1 rows only'", "a' fetch first 1 rows only"),
                                  ("''''", "'"),
                                  ("'x'' y'' z'", "x' y' z"),
                                  ("'-123'", "-123"), ("-123", "-123")]:
            sql = "SELECT " + literal + " AS value FROM literal_rows;"
            rows, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
            assert rows == [[expected]], (sql, rows, expected)
        for sql in (
                "CREATE TABLE literal_list_rows (id INT, value TEXT);",
                ("INSERT INTO literal_list_rows VALUES "
                 "(1, 'a,b'), (2, 'x'), (3, 'O''Brien, Jr.');"),
                "CREATE TABLE rewrite_literal_rows (id INT, value TEXT);",
                ("INSERT INTO rewrite_literal_rows VALUES "
                 "(1, 'x in (1)'), (2, 'exists(select 1)'), "
                 "(3, '(select 1)'), (4, 'x = any(select 1)'), "
                 "(5, 'x = all(select 1)'), (6, 'plain like word'), "
                 "(7, 'plain ilike word'), (8, 'plain regexp word'), "
                 "(9, 'plain contains word'), (10, 'plain overlaps word'), "
                 "(11, 'x / 0'), (12, 'x is null'), "
                 "(13, 'x is not null'), (14, 'x similar to y'), "
                 "(15, 'x[1]'), (16, 'x = any (array[1])'), "
                 "(17, 'substring(x from y)'), (18, 'position(x in y)'), "
                 "(19, 'x is distinct from y'), "
                 "(20, 'x is not distinct from y'), "
                 "(21, 'case when x then y else z end'), "
                 "(22, 'for update'), (23, 'for share'), "
                 "(24, 'nowait'), (25, 'skip locked'), "
                 "(26, 'before (select 1) from (select 2) after'), "
                 "(27, 'lateral (select 1) as x'), "
                 "(28, 'into outfile');")):
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)
        list_cases = [
            ("WHERE value IN ('a,b') ORDER BY id", [["1"]]),
            ("WHERE value NOT IN ('a,b') ORDER BY id", [["2"], ["3"]]),
            ("WHERE value IN ('O''Brien, Jr.') ORDER BY id", [["3"]]),
            ("WHERE value IN('x') ORDER BY id", [["2"]]),
            ("WHERE value NOT IN('a,b') ORDER BY id", [["2"], ["3"]]),
        ]
        for clause, expected in list_cases:
            sql = "SELECT id FROM literal_list_rows " + clause + ";"
            rows, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)
            assert rows == expected, (sql, rows, expected)
        rewrite_literals = [
            (1, "x in (1)"),
            (2, "exists(select 1)"),
            (3, "(select 1)"),
            (4, "x = any(select 1)"),
            (5, "x = all(select 1)"),
            (6, "plain like word"),
            (7, "plain ilike word"),
            (8, "plain regexp word"),
            (9, "plain contains word"),
            (10, "plain overlaps word"),
            (11, "x / 0"),
            (12, "x is null"),
            (13, "x is not null"),
            (14, "x similar to y"),
            (15, "x[1]"),
            (16, "x = any (array[1])"),
            (17, "substring(x from y)"),
            (18, "position(x in y)"),
            (19, "x is distinct from y"),
            (20, "x is not distinct from y"),
            (21, "case when x then y else z end"),
            (22, "for update"),
            (23, "for share"),
            (24, "nowait"),
            (25, "skip locked"),
            (26, "before (select 1) from (select 2) after"),
            (27, "lateral (select 1) as x"),
            (28, "into outfile"),
        ]
        for row_id, value in rewrite_literals:
            sql = ("SELECT id FROM rewrite_literal_rows WHERE value = '" +
                   value + "' ORDER BY id;")
            rows, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)
            assert rows == [[str(row_id)]], (sql, rows, row_id)
        qualifier_literals = [
            ("SELECT 'rewrite_literal_rows.id' AS value "
             "FROM rewrite_literal_rows WHERE id = 1;",
             [["rewrite_literal_rows.id"]]),
            ("SELECT 'r.id' AS value FROM rewrite_literal_rows AS r "
             "WHERE r.id = 1;",
             [["r.id"]]),
            ("SELECT 'd.id' AS value FROM (SELECT 1 AS id) AS d;",
             [["d.id"]]),
            ("SELECT 'd.id' AS value FROM (SELECT 1 AS id) d;",
             [["d.id"]]),
        ]
        for sql, expected in qualifier_literals:
            rows, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)
            assert rows == expected, (sql, rows, expected)
        _, state, message, _ = runner.ours_query(
            client, server["sock"], "BEGIN;")
        assert state is None, (state, message)
        for clause in ("FOR UPDATE NOWAIT", "FOR UPDATE SKIP LOCKED",
                       "FOR SHARE"):
            sql = "SELECT id FROM literal_rows WHERE id = 1 " + clause + ";"
            rows, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)
            assert rows == [["1"]], (sql, rows)
        _, state, message, _ = runner.ours_query(
            client, server["sock"], "COMMIT;")
        assert state is None, (state, message)
        print("[SQL LITERAL PRESERVATION E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
