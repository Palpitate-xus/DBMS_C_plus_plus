#!/usr/bin/env python3
"""Plain table SELECT preserves cells and SQL NULL without text framing."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "table_structured_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE exact_table_rows (id INT, v TEXT);",
            ("INSERT INTO exact_table_rows VALUES "
             "(1, 'hello world'), (2, ''), (3, NULL), (4, 'NULL'), "
             "(5, 'line1\nline2'), (6, 'say \"hi\"'), (7, ' lead ');"),
            "CREATE TABLE exact_null_order (id INT, v INT);",
            "INSERT INTO exact_null_order VALUES (1, NULL), (2, 1), (3, 2);",
            "CREATE TABLE exact_scalar_text_order (v TEXT);",
            "INSERT INTO exact_scalar_text_order VALUES ('2'), ('10'), (NULL);",
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)

        sql = "SELECT id, v FROM exact_table_rows ORDER BY id;"
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        expected = [
            ["1", "hello world"],
            ["2", ""],
            ["3", None],
            ["4", "NULL"],
            ["5", "line1\nline2"],
            ["6", 'say "hi"'],
            ["7", " lead "],
        ]
        assert state is None, (state, message)
        assert rows == expected, (rows, expected)
        assert headers == ["id", "v"], headers
        assert type_oids == [23, 25], type_oids
        assert command_tag == "SELECT 7", command_tag

        reversed_sql = "SELECT v, id FROM exact_table_rows ORDER BY id;"
        reversed_rows, reversed_state, reversed_message, reversed_headers = (
            runner.ours_query(client, server["sock"], reversed_sql))
        assert reversed_state is None, (reversed_state, reversed_message)
        assert reversed_rows == [[row[1], row[0]] for row in expected], \
            reversed_rows
        assert reversed_headers == ["v", "id"], reversed_headers

        one_column_sql = "SELECT v FROM exact_table_rows ORDER BY id;"
        one_column_rows, one_column_state, one_column_message, _ = (
            runner.ours_query(client, server["sock"], one_column_sql))
        assert one_column_state is None, (
            one_column_state, one_column_message)
        assert one_column_rows == [[row[1]] for row in expected], \
            one_column_rows

        scalar_cases = [
            (
                "SELECT upper(v) AS rendered FROM exact_table_rows "
                "ORDER BY id;",
                [[None if row[1] is None else row[1].upper()]
                 for row in expected],
            ),
            (
                "SELECT coalesce(v, 'fallback') AS rendered "
                "FROM exact_table_rows ORDER BY id;",
                [["fallback" if row[1] is None else row[1]]
                 for row in expected],
            ),
            (
                "SELECT v || '!' AS rendered FROM exact_table_rows "
                "ORDER BY id;",
                [[None if row[1] is None else row[1] + "!"]
                 for row in expected],
            ),
        ]
        for scalar_sql, scalar_expected in scalar_cases:
            scalar_rows, scalar_state, scalar_message, scalar_headers = (
                runner.ours_query(client, server["sock"], scalar_sql))
            assert scalar_state is None, (
                scalar_sql, scalar_state, scalar_message)
            assert scalar_rows == scalar_expected, (
                scalar_sql, scalar_rows, scalar_expected)
            assert scalar_headers == ["rendered"], scalar_headers

        alias_values = [
            None if row[1] is None else row[1].upper()
            for row in expected
        ]
        alias_expected = [[value] for value in sorted(
            (value for value in alias_values if value is not None),
            key=str.casefold)] + [[None]]
        alias_sql = (
            "SELECT upper(v) AS rendered FROM exact_table_rows "
            "ORDER BY rendered;")
        alias_rows, alias_state, alias_message, _ = runner.ours_query(
            client, server["sock"], alias_sql)
        assert alias_state is None, (alias_state, alias_message)
        assert alias_rows == alias_expected, (alias_rows, alias_expected)

        ordinal_values = [
            None if row[1] is None else row[1] + "!"
            for row in expected
        ]
        ordinal_expected = [[value] for value in sorted(
            (value for value in ordinal_values if value is not None),
            key=str.casefold, reverse=True)] + [[None]]
        ordinal_sql = (
            "SELECT v || '!' AS rendered FROM exact_table_rows "
            "ORDER BY 1 DESC NULLS LAST;")
        ordinal_rows, ordinal_state, ordinal_message, _ = runner.ours_query(
            client, server["sock"], ordinal_sql)
        assert ordinal_state is None, (ordinal_state, ordinal_message)
        assert ordinal_rows == ordinal_expected, (
            ordinal_rows, ordinal_expected)

        typed_order_sql = (
            "SELECT coalesce(v, 'z') AS rendered "
            "FROM exact_scalar_text_order ORDER BY rendered;")
        typed_order_rows, typed_order_state, typed_order_message, _ = (
            runner.ours_query(client, server["sock"], typed_order_sql))
        assert typed_order_state is None, (
            typed_order_state, typed_order_message)
        assert typed_order_rows == [["10"], ["2"], ["z"]], \
            typed_order_rows

        order_cases = [
            ("SELECT id FROM exact_null_order ORDER BY v;",
             [["2"], ["3"], ["1"]]),
            ("SELECT id FROM exact_null_order ORDER BY v DESC;",
             [["1"], ["3"], ["2"]]),
            ("SELECT id FROM exact_null_order ORDER BY v DESC NULLS LAST;",
             [["3"], ["2"], ["1"]]),
            ("SELECT id FROM exact_null_order ORDER BY v ASC NULLS FIRST;",
             [["1"], ["2"], ["3"]]),
        ]
        for order_sql, expected_rows in order_cases:
            order_rows, order_state, order_message, _ = runner.ours_query(
                client, server["sock"], order_sql)
            assert order_state is None, (order_sql, order_state, order_message)
            assert order_rows == expected_rows, (
                order_sql, order_rows, expected_rows)
        print("[TABLE STRUCTURED PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
