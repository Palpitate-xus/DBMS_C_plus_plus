#!/usr/bin/env python3
"""Quoted SELECT aliases are one exact protocol column name."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("alias_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ("CREATE TABLE alias_rows (id INT);",
                    "INSERT INTO alias_rows VALUES (1), (2);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
        cases = [
            ('SELECT id AS "fetch first 1 rows only" FROM alias_rows WHERE id = 1;',
             ["fetch first 1 rows only"], [["1"]]),
            ('SELECT id + 1 AS "sum value" FROM alias_rows WHERE id = 1;',
             ["sum value"], [["2"]]),
            ('SELECT count(*) AS "row count" FROM alias_rows;',
             ["row count"], [["2"]]),
            ('SELECT id AS "group key", count(*) AS "row count" FROM alias_rows GROUP BY id ORDER BY id;',
             ["group key", "row count"], [["1", "1"], ["2", "1"]]),
            ('SELECT row_number() OVER (ORDER BY id) AS "row number" FROM alias_rows LIMIT 1;',
             ["row number"], [["1"]]),
            ('SELECT id AS "a""b" FROM alias_rows WHERE id = 1;',
             ['a"b'], [["1"]]),
            ('SELECT 1 AS "constant value";', ["constant value"], [["1"]]),
        ]
        for sql, expected_headers, expected_rows in cases:
            rows, state, message, headers = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
            assert headers == expected_headers, (sql, headers, expected_headers)
            assert rows == expected_rows, (sql, rows, expected_rows)

        decoded = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                'SELECT id AS "typed alias" FROM alias_rows WHERE id = 1;'),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert headers == ["typed alias"], headers
        assert rows == [["1"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        for sql, expected_rows in [
                ("SELECT i FROM generate_series(1, 3) AS t(i);",
                 [["1"], ["2"], ["3"]]),
                ("SELECT s FROM generate_series(2, 6, 2) AS t(s);",
                 [["2"], ["4"], ["6"]])]:
            decoded = runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert rows == expected_rows, (sql, rows)
            assert type_oids == [23], (sql, type_oids)
            assert command_tag == "SELECT 3", (sql, command_tag)

        aggregate_cases = [
            ("SELECT sum(i) AS total FROM generate_series(1, 4) AS t(i);",
             [["10"]], [20]),
            ("SELECT count(*) AS n FROM generate_series(1, 100, 10) AS t(i);",
             [["10"]], [20]),
        ]
        for sql, expected_rows, expected_types in aggregate_cases:
            decoded = runner.decode_wire_result(
                client.simple_query(server["sock"], sql), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (sql, state, message)
            assert rows == expected_rows, (sql, rows)
            assert type_oids == expected_types, (sql, type_oids)
            assert command_tag == "SELECT 1", (sql, command_tag)
        print("[QUOTED ALIAS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
