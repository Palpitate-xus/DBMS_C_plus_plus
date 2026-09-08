#!/usr/bin/env python3
"""Window projections publish PostgreSQL-compatible result type OIDs."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "window_type_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE window_types (g VARCHAR(4), v INT, n NUMERIC);",
            ("INSERT INTO window_types VALUES "
             "('a', 1, 1.5), ('a', 2, 2.5), ('b', 4, 4.5);")
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)

        cases = [
            (("SELECT g, v, row_number() OVER (PARTITION BY g ORDER BY v) "
              "FROM window_types ORDER BY g, v;"),
             [1043, 23, 20]),
            (("SELECT rank() OVER (ORDER BY v), "
              "dense_rank() OVER (ORDER BY v), ntile(2) OVER (ORDER BY v) "
              "FROM window_types ORDER BY v;"),
             [20, 20, 23]),
            (("SELECT percent_rank() OVER (ORDER BY v), "
              "cume_dist() OVER (ORDER BY v) FROM window_types ORDER BY v;"),
             [701, 701]),
            (("SELECT lag(v) OVER (ORDER BY v), lead(n) OVER (ORDER BY v), "
              "nth_value(v, 2) OVER (ORDER BY v) "
              "FROM window_types ORDER BY v;"),
             [23, 1700, 23]),
            (("SELECT sum(v) OVER (PARTITION BY g), "
              "avg(v) OVER (PARTITION BY g), min(n) OVER (PARTITION BY g), "
              "count(*) OVER (PARTITION BY g) "
              "FROM window_types ORDER BY g, v;"),
             [20, 1700, 1700, 20]),
            (("SELECT array_agg(v) OVER (ORDER BY v) "
              "FROM window_types ORDER BY v;"),
             [1007]),
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
        print("[WINDOW TYPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
