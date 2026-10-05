#!/usr/bin/env python3
"""Derived tables, CTEs, and views retain inner PostgreSQL result types."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "derived_type_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE typed_src (grp VARCHAR(4), v INT);",
            "INSERT INTO typed_src VALUES ('a', 1), ('a', 2), ('b', 4);",
            "CREATE TABLE typed_multiplier (grp VARCHAR(4), mult NUMERIC);",
            "INSERT INTO typed_multiplier VALUES ('a', 10.5), ('b', 2.0);",
            "CREATE TABLE typed_dates (id INT, d DATE);",
            ("INSERT INTO typed_dates VALUES "
             "(1, DATE '2024-01-10'), (2, NULL);"),
            "CREATE TABLE typed_lateral_left (id INT, payload TEXT);",
            ("INSERT INTO typed_lateral_left VALUES "
             "(1, 'left one'), (2, ''), (4, 'NULL'), (5, NULL), "
             "(NULL, 'null id'), (6, 'quote '' value');"),
            "CREATE TABLE typed_lateral_empty (id INT);",
            "CREATE TABLE typed_lateral_right (id INT, label TEXT);",
            ("INSERT INTO typed_lateral_right VALUES "
             "(1, 'right one'), (1, 'right uno'), (2, ''), (3, 'unused'), "
             "(4, 'NULL'), (5, NULL);"),
            "CREATE TABLE typed_text_edges (id INT, txt TEXT);",
            ("INSERT INTO typed_text_edges VALUES "
             "(1, 'a b'), (2, ''), (3, 'NULL'), (4, NULL), "
             "(5, '  edge  '), (6, 'line1\nline2');"),
            "CREATE TABLE typed_recursive_edges (parent INT, child INT);",
            ("INSERT INTO typed_recursive_edges VALUES "
             "(1, 2), (1, 3), (2, 4), (3, 4);"),
            ("CREATE VIEW typed_view AS SELECT grp, sum(v) AS total "
             "FROM typed_src GROUP BY grp;"),
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)

        cases = [
            (("SELECT grp, total FROM "
              "(SELECT grp, sum(v) AS total FROM typed_src GROUP BY grp) AS d "
              "ORDER BY grp;"),
             [["a", "3"], ["b", "4"]], [1043, 20]),
            (("WITH totals AS "
              "(SELECT grp, sum(v) AS total FROM typed_src GROUP BY grp) "
              "SELECT grp, total FROM totals ORDER BY grp;"),
             [["a", "3"], ["b", "4"]], [1043, 20]),
            ("SELECT grp, total FROM typed_view ORDER BY grp;",
             [["a", "3"], ["b", "4"]], [1043, 20]),
            ("SELECT grp FROM typed_view WHERE total > 3 ORDER BY grp;",
             [["b"]], [1043]),
            (("SELECT i, n, t FROM "
              "(SELECT 1 AS i, 1.5 AS n, 'x'::text AS t) AS d;"),
             [["1", "1.5", "x"]], [23, 1700, 25]),
            ("SELECT d + 1 FROM typed_dates ORDER BY id;",
             [["2024-01-11"], [None]], [1082]),
            ("SELECT d - DATE '2024-01-01' FROM typed_dates ORDER BY id;",
             [["9"], [None]], [23]),
            ("SELECT extract(year FROM d) FROM typed_dates ORDER BY id;",
             [["2024"], [None]], [1700]),
            ("SELECT d + interval '1 day' FROM typed_dates ORDER BY id;",
             [["2024-01-11 00:00:00"], [None]], [1114]),
            (("SELECT l.id, l.payload, x.label FROM typed_lateral_left AS l "
              "CROSS JOIN LATERAL "
              "(SELECT label FROM typed_lateral_right WHERE id = l.id) AS x "
              "ORDER BY l.id, x.label;"),
             [["1", "left one", "right one"],
              ["1", "left one", "right uno"], ["2", "", ""],
              ["4", "NULL", "NULL"], ["5", None, None]],
             [23, 25, 25]),
            (("SELECT l.id, x.label FROM typed_lateral_left l "
              "CROSS JOIN LATERAL "
              "(SELECT r.label || payload AS label "
              "FROM typed_lateral_right r "
              "WHERE r.id = l.id AND payload <> 'skip') x "
              "ORDER BY l.id, x.label;"),
             [["1", "right oneleft one"], ["1", "right unoleft one"],
              ["2", ""], ["4", "NULLNULL"]], [23, 25]),
            (("SELECT l.id, x.label FROM typed_lateral_left l "
              "LEFT JOIN LATERAL "
              "(SELECT label FROM typed_lateral_right WHERE id = l.id) x "
              "ON true ORDER BY l.id NULLS LAST, x.label NULLS FIRST;"),
             [["1", "right one"], ["1", "right uno"], ["2", ""],
              ["4", "NULL"], ["5", None], ["6", None], [None, None]],
             [23, 25]),
            (("SELECT l.id, x.label FROM typed_lateral_left l "
              "LEFT JOIN LATERAL "
              "(SELECT label FROM typed_lateral_right WHERE id = l.id) x "
              "ON x.label = 'right one' "
              "ORDER BY l.id NULLS LAST, x.label NULLS FIRST;"),
             [["1", "right one"], ["2", None], ["4", None], ["5", None],
              ["6", None], [None, None]],
             [23, 25]),
            (("SELECT l.id, x.label FROM typed_lateral_left l JOIN LATERAL "
              "(SELECT label FROM typed_lateral_right WHERE id = l.id) x "
              "ON x.label = 'right one' ORDER BY l.id;"),
             [["1", "right one"]], [23, 25]),
            (("SELECT l.id, x.label FROM typed_lateral_left l, LATERAL "
              "(SELECT label FROM typed_lateral_right WHERE id = l.id) x "
              "ORDER BY l.id, x.label;"),
             [["1", "right one"], ["1", "right uno"], ["2", ""],
              ["4", "NULL"],
              ["5", None]],
             [23, 25]),
            (("SELECT l.id, x.outer_id FROM typed_lateral_left l "
              "CROSS JOIN LATERAL (SELECT id AS outer_id) x "
              "ORDER BY l.id NULLS LAST;"),
             [["1", "1"], ["2", "2"], ["4", "4"], ["5", "5"],
              ["6", "6"], [None, None]],
             [23, 23]),
            (("SELECT l.id, x.outer_id FROM typed_lateral_left l "
              "CROSS JOIN LATERAL (SELECT id + 10 AS outer_id) x "
              "ORDER BY l.id NULLS LAST;"),
             [["1", "11"], ["2", "12"], ["4", "14"], ["5", "15"],
              ["6", "16"], [None, None]],
             [23, 23]),
            (("SELECT l.id, x.out FROM typed_lateral_left l "
              "CROSS JOIN LATERAL "
              "(SELECT CAST(payload AS text) || '!' AS out) x "
              "ORDER BY l.id NULLS LAST;"),
             [["1", "left one!"], ["2", "!"], ["4", "NULL!"],
              ["5", None], ["6", "quote ' value!"], [None, "null id!"]],
             [23, 25]),
            (("SELECT l.id, x.ok FROM typed_lateral_left l "
              "CROSS JOIN LATERAL "
              "(SELECT 'matched'::text AS ok WHERE id < 3) x "
              "ORDER BY l.id;"),
             [["1", "matched"], ["2", "matched"]], [23, 25]),
            (("SELECT l.id, x.ok FROM typed_lateral_left l "
              "CROSS JOIN LATERAL "
              "(SELECT 'matched'::text AS ok WHERE id < 0) x;"),
             [], [23, 25]),
            (("SELECT l.id, x.outer_payload FROM typed_lateral_left l "
              "CROSS JOIN LATERAL (SELECT payload AS outer_payload) x "
              "ORDER BY l.id NULLS LAST;"),
             [["1", "left one"], ["2", ""], ["4", "NULL"], ["5", None],
              ["6", "quote ' value"], [None, "null id"]],
             [23, 25]),
            (("SELECT e.id, x.outer_id FROM typed_lateral_empty e "
              "CROSS JOIN LATERAL (SELECT id AS outer_id) x;"),
             [], [23, 23]),
            (("SELECT e.id, x.label FROM typed_lateral_empty e "
              "CROSS JOIN LATERAL "
              "(SELECT label FROM typed_lateral_right WHERE id = e.id) x "
              "ORDER BY e.id;"),
             [], [23, 25]),
            (("SELECT e.id, x.label FROM typed_lateral_empty AS e "
              "CROSS JOIN LATERAL "
              "(SELECT label FROM typed_lateral_right WHERE id = e.id) AS x;"),
             [], [23, 25]),
            (("SELECT id, txt FROM "
              "(SELECT id, txt FROM typed_text_edges) AS d ORDER BY id;"),
             [["1", "a b"], ["2", ""], ["3", "NULL"], ["4", None],
              ["5", "  edge  "], ["6", "line1\nline2"]],
             [23, 25]),
            (("WITH d AS (SELECT id, txt FROM typed_text_edges) "
              "SELECT id, txt FROM d ORDER BY id;"),
             [["1", "a b"], ["2", ""], ["3", "NULL"], ["4", None],
              ["5", "  edge  "], ["6", "line1\nline2"]],
             [23, 25]),
            (("WITH source_rows AS "
              "(SELECT id, txt FROM typed_text_edges), "
              "first_reader AS (SELECT id, txt FROM source_rows), "
              "second_reader AS (SELECT id, txt FROM source_rows) "
              "SELECT id, txt FROM second_reader ORDER BY id;"),
             [["1", "a b"], ["2", ""], ["3", "NULL"], ["4", None],
              ["5", "  edge  "], ["6", "line1\nline2"]],
             [23, 25]),
            (("WITH renamed(row_id, body) AS "
              "(SELECT id, txt FROM typed_text_edges) "
              "SELECT row_id, body FROM renamed ORDER BY row_id;"),
             [["1", "a b"], ["2", ""], ["3", "NULL"], ["4", None],
              ["5", "  edge  "], ["6", "line1\nline2"]],
             [23, 25]),
            (("WITH RECURSIVE edges("
              "n, spaced, empty_text, literal_null, sql_null) AS ("
              "SELECT 1, 'a b'::text, ''::text, 'NULL'::text, NULL::text "
              "UNION ALL "
              "SELECT n + 1, spaced, empty_text, literal_null, sql_null "
              "FROM edges WHERE n < 3) "
              "SELECT n, spaced, empty_text, literal_null, sql_null "
              "FROM edges ORDER BY n;"),
             [["1", "a b", "", "NULL", None],
              ["2", "a b", "", "NULL", None],
              ["3", "a b", "", "NULL", None]],
             [23, 25, 25, 25, 25]),
            (("WITH RECURSIVE walk(n) AS ("
              "SELECT 1 UNION ALL "
              "SELECT e.child FROM walk w "
              "JOIN typed_recursive_edges e ON w.n = e.parent) "
              "SELECT n FROM walk ORDER BY n;"),
             [["1"], ["2"], ["3"], ["4"], ["4"]], [23]),
            (("WITH RECURSIVE walk(n) AS ("
              "SELECT 1 UNION "
              "SELECT e.child FROM walk w "
              "JOIN typed_recursive_edges e ON w.n = e.parent) "
              "SELECT n FROM walk ORDER BY n;"),
             [["1"], ["2"], ["3"], ["4"]], [23]),
            (("WITH RECURSIVE constants(n) AS ("
              "SELECT 1 UNION ALL SELECT 2) "
              "SELECT n FROM constants ORDER BY n;"),
             [["1"], ["2"]], [23]),
        ]
        for sql, expected_rows, expected_types in cases:
            decoded = runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert rows == expected_rows, (sql, rows, expected_rows)
            assert len(headers) == len(expected_types), (sql, headers)
            assert type_oids == expected_types, (sql, type_oids, expected_types)
            if "lateral" in sql.lower():
                if "outer_id" in sql:
                    expected_headers = ["id", "outer_id"]
                elif "outer_payload" in sql:
                    expected_headers = ["id", "outer_payload"]
                elif "x.out" in sql:
                    expected_headers = ["id", "out"]
                elif "x.ok" in sql:
                    expected_headers = ["id", "ok"]
                elif "l.payload" in sql:
                    expected_headers = ["id", "payload", "label"]
                elif "x.label" in sql:
                    expected_headers = ["id", "label"]
                else:
                    expected_headers = ["id", "label"]
                assert headers == expected_headers, (sql, headers)
            assert command_tag == "SELECT %d" % len(expected_rows), (
                sql, command_tag)

        scalar_cases = [
            ("SELECT (SELECT count(*) FROM typed_src) AS c;", [20], 1),
            (("SELECT grp, count(*), "
              "(SELECT mult FROM typed_multiplier "
              "WHERE typed_multiplier.grp = typed_src.grp) "
              "FROM typed_src GROUP BY grp ORDER BY grp;"),
             [1043, 20, 1700], 2),
            (("SELECT grp, sum(v) + "
              "(SELECT mult FROM typed_multiplier "
              "WHERE typed_multiplier.grp = typed_src.grp) "
              "FROM typed_src GROUP BY grp ORDER BY grp;"),
             [1043, 1700], 2),
        ]
        for sql, expected_types, expected_row_count in scalar_cases:
            decoded = runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert len(rows) == expected_row_count, (sql, rows)
            assert len(headers) == len(expected_types), (sql, headers)
            assert type_oids == expected_types, (sql, type_oids, expected_types)
            assert command_tag == "SELECT %d" % expected_row_count, (
                sql, command_tag)
        print("[DERIVED TYPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
