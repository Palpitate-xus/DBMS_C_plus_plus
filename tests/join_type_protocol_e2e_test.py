#!/usr/bin/env python3
"""JOIN projections retain both relation schemas in RowDescription."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "join_type_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE join_type_left (id INT, name TEXT);",
            "CREATE TABLE join_type_right (id INT, val NUMERIC);",
            "INSERT INTO join_type_left VALUES (1, 'one'), (2, 'two');",
            "INSERT INTO join_type_right VALUES (1, 1.5), (3, 3.5);",
            "CREATE TABLE join_exact_left (id INT, txt TEXT);",
            "CREATE TABLE join_exact_right (id INT, txt TEXT);",
            ("INSERT INTO join_exact_left VALUES "
             "(1, ''), (2, NULL), (3, 'NULL'), (4, 'a b'), "
             "(5, 'left only');"),
            ("INSERT INTO join_exact_right VALUES "
             "(1, 'r one'), (2, ''), (3, NULL), (4, 'NULL'), "
             "(6, 'right only');"),
            "CREATE TABLE join_collision_left (id INT, txt TEXT);",
            "CREATE TABLE join_collision_right (id INT, txt TEXT);",
            "INSERT INTO join_collision_left VALUES (1, 'NULL');",
            ("INSERT INTO join_collision_right VALUES "
             "(2, 'b c'), (1, 'b c');"),
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)

        cases = [
            (("SELECT * FROM join_type_left INNER JOIN join_type_right "
              "ON join_type_left.id = join_type_right.id ORDER BY join_type_left.id;"),
             [23, 25, 23, 1700]),
            (("SELECT * FROM join_type_left LEFT JOIN join_type_right "
              "ON join_type_left.id = join_type_right.id ORDER BY join_type_left.id;"),
             [23, 25, 23, 1700]),
            (("SELECT * FROM join_type_left RIGHT JOIN join_type_right "
              "ON join_type_left.id = join_type_right.id ORDER BY join_type_right.id;"),
             [23, 25, 23, 1700]),
            (("SELECT name, val FROM join_type_left l JOIN join_type_right r "
              "ON l.id = r.id ORDER BY name;"),
             [25, 1700]),
            ("SELECT count(*) FROM join_type_left CROSS JOIN join_type_right;",
             [20]),
        ]
        for sql, expected_types in cases:
            decoded = runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert rows, (sql, rows)
            assert len(headers) == len(expected_types), (sql, headers)
            assert type_oids == expected_types, (sql, type_oids, expected_types)
            assert command_tag == "SELECT %d" % len(rows), (sql, command_tag)

        exact_sql = (
            "SELECT l.id, l.txt, r.txt FROM join_exact_left l "
            "JOIN join_exact_right r ON l.id = r.id ORDER BY l.id;")
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], exact_sql), include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [
            ["1", "", "r one"],
            ["2", None, ""],
            ["3", "NULL", None],
            ["4", "a b", "NULL"],
        ], rows
        assert headers == ["id", "txt", "txt"], headers
        assert type_oids == [23, 25, 25], type_oids
        assert command_tag == "SELECT 4", command_tag

        left_sql = (
            "SELECT l.id, l.txt, r.txt FROM join_exact_left l "
            "LEFT JOIN join_exact_right r ON l.id = r.id ORDER BY l.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], left_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "", "r one"], ["2", None, ""],
            ["3", "NULL", None], ["4", "a b", "NULL"],
            ["5", "left only", None],
        ], rows
        assert type_oids == [23, 25, 25], type_oids
        assert command_tag == "SELECT 5", command_tag

        right_sql = (
            "SELECT l.txt, r.id, r.txt FROM join_exact_left l "
            "RIGHT JOIN join_exact_right r ON l.id = r.id ORDER BY r.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], right_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["", "1", "r one"], [None, "2", ""],
            ["NULL", "3", None], ["a b", "4", "NULL"],
            [None, "6", "right only"],
        ], rows
        assert type_oids == [25, 23, 25], type_oids
        assert command_tag == "SELECT 5", command_tag

        full_sql = (
            "SELECT l.id, l.txt, r.id, r.txt FROM join_exact_left l "
            "FULL OUTER JOIN join_exact_right r ON l.id = r.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], full_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [
            ["1", "", "1", "r one"], ["2", None, "2", ""],
            ["3", "NULL", "3", None], ["4", "a b", "4", "NULL"],
            ["5", "left only", None, None],
            [None, None, "6", "right only"],
        ], rows
        assert type_oids == [23, 25, 23, 25], type_oids
        assert command_tag == "SELECT 6", command_tag

        collision_sql = (
            "SELECT l.txt, r.txt FROM join_collision_left l "
            "FULL OUTER JOIN join_collision_right r ON l.id = r.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], collision_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["NULL", "b c"], [None, "b c"]], rows
        assert type_oids == [25, 25], type_oids
        assert command_tag == "SELECT 2", command_tag

        distinct_sql = (
            "SELECT DISTINCT l.txt FROM join_exact_left l "
            "CROSS JOIN join_exact_right r;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], distinct_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [[""], [None], ["NULL"], ["a b"], ["left only"]], rows
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 5", command_tag

        cross_limit_sql = (
            "SELECT l.txt AS left_text, r.txt AS right_text "
            "FROM join_exact_left l CROSS JOIN join_exact_right r "
            "LIMIT 3 OFFSET 1;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], cross_limit_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["", ""], ["", None], ["", "NULL"]], rows
        assert headers == ["left_text", "right_text"], headers
        assert type_oids == [25, 25], type_oids
        assert command_tag == "SELECT 3", command_tag

        aggregate_sql = (
            "SELECT count(l.txt) AS left_count, "
            "count(r.txt) AS right_count, min(l.txt) AS minimum, "
            "max(r.txt) AS maximum "
            "FROM join_exact_left l JOIN join_exact_right r ON l.id = r.id;")
        rows, state, message, headers, command_tag, type_oids = (
            runner.decode_wire_result(
                client.simple_query(server["sock"], aggregate_sql),
                include_types=True))
        assert state is None, (state, message)
        assert rows == [["3", "3", "", "r one"]], rows
        assert headers == [
            "left_count", "right_count", "minimum", "maximum"
        ], headers
        assert type_oids == [20, 20, 25, 25], type_oids
        assert command_tag == "SELECT 1", command_tag
        print("[JOIN TYPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
