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
