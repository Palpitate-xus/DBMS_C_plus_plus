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
        print("[JOIN TYPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
