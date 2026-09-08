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
        print("[DML COMMAND TAG E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
