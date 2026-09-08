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
                 "(1, 'a,b'), (2, 'x'), (3, 'O''Brien, Jr.');")):
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)
        list_cases = [
            ("WHERE value IN ('a,b') ORDER BY id", [["1"]]),
            ("WHERE value NOT IN ('a,b') ORDER BY id", [["2"], ["3"]]),
            ("WHERE value IN ('O''Brien, Jr.') ORDER BY id", [["3"]]),
        ]
        for clause, expected in list_cases:
            sql = "SELECT id FROM literal_list_rows " + clause + ";"
            rows, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)
            assert rows == expected, (sql, rows, expected)
        print("[SQL LITERAL PRESERVATION E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
