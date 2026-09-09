#!/usr/bin/env python3
"""UPDATE and DELETE publish the actual affected-row count."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "dml_tag_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE dml_tag_rows (id INT, value VARCHAR(8));",
            "INSERT INTO dml_tag_rows VALUES (1, 'a'), (2, 'b'), (3, 'c');",
            "CREATE TABLE dml_return_rows (id INT, value TEXT);",
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)

        cases = [
            ("UPDATE dml_tag_rows SET value = 'x' WHERE id = 2;",
             "UPDATE 1"),
            ("UPDATE dml_tag_rows SET value = 'y' WHERE id = 99;",
             "UPDATE 0"),
            ("DELETE FROM dml_tag_rows WHERE id = 3;", "DELETE 1"),
            ("DELETE FROM dml_tag_rows WHERE id = 99;", "DELETE 0"),
            ("UPDATE dml_tag_rows SET value = 'z';", "UPDATE 2"),
            ("DELETE FROM dml_tag_rows;", "DELETE 2"),
        ]
        for sql, expected_tag in cases:
            messages = client.simple_query(server["sock"], sql)
            rows, state, message, headers, command_tag = (
                runner.decode_wire_result(messages))
            assert state is None, (sql, state, message)
            assert rows == [], (sql, rows)
            assert headers == [], (sql, headers)
            assert command_tag == expected_tag, (
                sql, command_tag, expected_tag)

        returning_cases = [
            ("INSERT INTO dml_return_rows VALUES (1, 'a'), (2, 'b') "
             "RETURNING id, value;",
             [["1", "a"], ["2", "b"]], "INSERT 0 2"),
            ("UPDATE dml_return_rows SET value = 'c' WHERE id = 1 "
             "RETURNING id, value;",
             [["1", "c"]], "UPDATE 1"),
            ("DELETE FROM dml_return_rows WHERE id = 2 "
             "RETURNING id, value;",
             [["2", "b"]], "DELETE 1"),
        ]
        for sql, expected_rows, expected_tag in returning_cases:
            decoded = runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert rows == expected_rows, (sql, rows, expected_rows)
            assert headers == ["id", "value"], (sql, headers)
            assert command_tag == expected_tag, (sql, command_tag, expected_tag)
            assert type_oids == [23, 25], (sql, type_oids)
        print("[DML COMMAND TAG E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
