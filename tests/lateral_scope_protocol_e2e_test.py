#!/usr/bin/env python3
"""LATERAL materialization preserves its SQL-visible column namespace."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "lateral_scope_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in (
                "CREATE TABLE lat_scope_left (id INT, payload TEXT);",
                "CREATE TABLE lat_scope_right (id INT, label TEXT);",
                "CREATE TABLE lat_scope_empty (id INT, payload TEXT);",
                "INSERT INTO lat_scope_left VALUES (1, 'left one'), (2, NULL);",
                "INSERT INTO lat_scope_right VALUES (1, 'right one'), (3, 'right three');",
                'CREATE TABLE lat_scope_quoted ("MixedId" INT, txt TEXT);',
                "INSERT INTO lat_scope_quoted VALUES (1, 'id l.id'), (2, '');"):
            result = query(sql)
            assert result[1] is None, (sql, result)

        cases = [
            ("SELECT id, payload, x.next_id FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT l.id + 10 AS next_id) x ORDER BY id;",
             [["1", "left one", "11"], ["2", None, "12"]],
             ["id", "payload", "next_id"], [23, 25, 23]),
            ("SELECT * FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x ORDER BY id;",
             [["1", "left one", "11"], ["2", None, "12"]],
             ["id", "payload", "next_id"], [23, 25, 23]),
            ("SELECT upper(payload) FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x ORDER BY upper;",
             [["LEFT ONE"], [None]], ["upper"], [25]),
            ("SELECT l.*, x.* FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x ORDER BY l.id;",
             [["1", "left one", "11"], ["2", None, "12"]],
             ["id", "payload", "next_id"], [23, 25, 23]),
            ("SELECT id + next_id AS total, payload FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x "
             "WHERE id = 1 ORDER BY total;",
             [["12", "left one"]], ["total", "payload"], [23, 25]),
            ("SELECT next_id AS id FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x "
             "WHERE id = 1 ORDER BY id DESC;",
             [["11"]], ["id"], [23]),
            ("SELECT * FROM lat_scope_left l JOIN lat_scope_right r USING (id) "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x;",
             [["1", "left one", "right one", "11"]],
             ["id", "payload", "label", "next_id"], [23, 25, 25, 23]),
            ("SELECT id, l.id, r.id, next_id FROM lat_scope_left l "
             "FULL JOIN lat_scope_right r USING (id) "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x ORDER BY 1;",
             [["1", "1", "1", "11"], ["2", "2", None, "12"],
              ["3", None, "3", "13"]],
             ["id", "id", "id", "next_id"], [23, 23, 23, 23]),
            ("SELECT * FROM lat_scope_left l FULL JOIN lat_scope_right r USING (id) "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x ORDER BY 1;",
             [["1", "left one", "right one", "11"], ["2", None, None, "12"],
              ["3", None, "right three", "13"]],
             ["id", "payload", "label", "next_id"], [23, 25, 25, 23]),
            ("SELECT r.*, l.*, x.* FROM lat_scope_left l "
             "FULL JOIN lat_scope_right r USING (id) "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x "
             "WHERE id = 3;",
             [["3", "right three", None, None, "13"]],
             ["id", "label", "id", "payload", "next_id"], [23, 25, 23, 25, 23]),
            ("SELECT * FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS first) x "
             "CROSS JOIN LATERAL (SELECT first + 10 AS second) y ORDER BY id;",
             [["1", "left one", "11", "21"], ["2", None, "12", "22"]],
             ["id", "payload", "first", "second"], [23, 25, 23, 23]),
            ("SELECT l.*, x.*, y.* FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT l.id + 10 AS first) x "
             "CROSS JOIN LATERAL (SELECT x.first + 10 AS second) y ORDER BY l.id;",
             [["1", "left one", "11", "21"], ["2", None, "12", "22"]],
             ["id", "payload", "first", "second"], [23, 25, 23, 23]),
            ("SELECT * FROM lat_scope_empty l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x;",
             [], ["id", "payload", "next_id"], [23, 25, 23]),
            ('SELECT "q".*, x.* FROM lat_scope_quoted "q" '
             'CROSS JOIN LATERAL (SELECT "q"."MixedId" + 1 AS next_id) x '
             'ORDER BY "q"."MixedId";',
             [["1", "id l.id", "2"], ["2", "", "3"]],
             ["MixedId", "txt", "next_id"], [23, 25, 23]),
            ('SELECT "MixedId", txt, x.next_id FROM lat_scope_quoted q '
             'CROSS JOIN LATERAL (SELECT q."MixedId" + 1 AS next_id) x '
             'WHERE("MixedId" = 1) ORDER BY "MixedId";',
             [["1", "id l.id", "2"]],
             ["MixedId", "txt", "next_id"], [23, 25, 23]),
            ("SELECT id, sum(next_id) AS total FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x "
             "GROUP BY id HAVING sum(next_id) > 11 ORDER BY id;",
             [["2", "12"]], ["id", "total"], [23, 20]),
            ("SELECT id, CASE WHEN payload IS NULL THEN 'empty' "
             "ELSE upper(payload) END AS value FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS next_id) x ORDER BY id;",
             [["1", "LEFT ONE"], ["2", "empty"]],
             ["id", "value"], [23, 25]),
            ("SELECT x.* FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT l.id AS dup, l.id + 10 AS dup) x "
             "ORDER BY l.id;",
             [["1", "11"], ["2", "12"]], ["dup", "dup"], [23, 23]),
            ("SELECT * FROM lat_scope_left l NATURAL FULL JOIN lat_scope_right r "
             "CROSS JOIN LATERAL (SELECT id + 10 AS first) x "
             "CROSS JOIN LATERAL (SELECT id + first AS second) y ORDER BY id;",
             [["1", "left one", "right one", "11", "12"],
              ["2", None, None, "12", "14"],
              ["3", None, "right three", "13", "16"]],
             ["id", "payload", "label", "first", "second"],
             [23, 25, 25, 23, 23]),
            ("SELECT l.id, x.first, y.second FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT id + 10 AS first) x "
             "CROSS JOIN LATERAL (SELECT id AS second FROM lat_scope_right "
             "WHERE id = l.id) y ORDER BY l.id;",
             [["1", "11", "1"]], ["id", "first", "second"], [23, 23, 23]),
            ("SELECT id /* id l.id */, $$id l.id next_id$$ AS literal "
             "FROM lat_scope_left l CROSS JOIN LATERAL "
             "(SELECT id + 10 AS next_id) x WHERE(id=1) ORDER BY id;",
             [["1", "id l.id next_id"]], ["id", "literal"], [23, 25]),
            ("SELECT l.id, next_id, r.label FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT l.id + 10 AS next_id) x "
             "LEFT JOIN lat_scope_right r ON l.id = r.id ORDER BY l.id;",
             [["1", "11", "right one"], ["2", "12", None]],
             ["id", "next_id", "label"], [23, 23, 25]),
            ("SELECT l.id, x.first, r.label, y.second FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT l.id + 10 AS first) x "
             "LEFT JOIN lat_scope_right r ON l.id = r.id "
             "CROSS JOIN LATERAL (SELECT x.first + 10 AS second) y ORDER BY l.id;",
             [["1", "11", "right one", "21"], ["2", "12", None, "22"]],
             ["id", "first", "label", "second"], [23, 23, 25, 23]),
            ("SELECT * FROM lat_scope_left l "
             "CROSS JOIN LATERAL (SELECT l.id + 10 AS next_id) x "
             "FULL JOIN lat_scope_right r USING(id) ORDER BY id;",
             [["1", "left one", "11", "right one"],
              ["2", None, "12", None], ["3", None, None, "right three"]],
             ["id", "payload", "next_id", "label"], [23, 25, 23, 25]),
        ]
        for sql, expected_rows, expected_headers, expected_types in cases:
            rows, state, message, headers, tag, types = query(sql)
            assert state is None, (sql, state, message)
            assert rows == expected_rows, (sql, rows, expected_rows)
            assert headers == expected_headers, (sql, headers, expected_headers)
            assert types == expected_types, (sql, types, expected_types)
            assert tag == f"SELECT {len(expected_rows)}", (sql, tag)

        for sql, expected_state in (
                ("SELECT id FROM lat_scope_left l "
                 "CROSS JOIN LATERAL (SELECT l.id AS id) x;", "42702"),
                ("SELECT payload FROM lat_scope_left l "
                 "CROSS JOIN LATERAL (SELECT 1 AS payload) x;", "42702"),
                ("SELECT lat_scope_left.id FROM lat_scope_left l "
                 "CROSS JOIN LATERAL (SELECT l.id + 1 AS next_id) x;", "42P01"),
                ("SELECT x.dup FROM lat_scope_left l "
                 "CROSS JOIN LATERAL (SELECT l.id AS dup, 10 AS dup) x;", "42702"),
                ("SELECT l.id AS dup, x.next_id AS dup FROM lat_scope_left l "
                 "CROSS JOIN LATERAL (SELECT l.id + 1 AS next_id) x "
                 "ORDER BY dup;", "42702"),
                ("SELECT y.next_id FROM lat_scope_left l "
                 "CROSS JOIN LATERAL (SELECT l.id AS id) x "
                 "CROSS JOIN LATERAL (SELECT id + 1 AS next_id) y;", "42702")):
            rows, state, message, headers, tag, types = query(sql)
            assert state == expected_state, (sql, state, message)
            assert rows == [] and tag is None, (sql, rows, tag)
        print("[LATERAL SCOPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
