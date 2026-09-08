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
             "('a', 1, 1.5), ('a', 2, 2.5), ('b', 4, 4.5);"),
            "CREATE TABLE window_null_text (id INT, p TEXT, v TEXT);",
            ("INSERT INTO window_null_text VALUES "
             "(1, '', ''), (2, '', NULL), "
             "(3, NULL, 'x'), (4, NULL, '');"),
            "CREATE TABLE window_exact_text (id INT, v TEXT);",
            ("INSERT INTO window_exact_text VALUES "
             "(1, ''), (2, NULL), (3, 'NULL'), (4, 'a b');"),
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

        null_partition_sql = (
            "SELECT id, count(v) OVER (PARTITION BY p) AS counted, "
            "min(v) OVER (PARTITION BY p) AS minimum "
            "FROM window_null_text ORDER BY id;")
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], null_partition_sql),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [
            ["1", "1", ""], ["2", "1", ""],
            ["3", "2", ""], ["4", "2", ""],
        ], rows
        assert headers == ["id", "counted", "minimum"], headers
        assert type_oids == [23, 20, 25], type_oids
        assert command_tag == "SELECT 4", command_tag

        offset_and_array_sql = (
            "SELECT id, lag(v) OVER (ORDER BY id) AS previous, "
            "array_agg(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED "
            "PRECEDING AND CURRENT ROW) AS collected "
            "FROM window_null_text ORDER BY id;")
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], offset_and_array_sql),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [
            ["1", None, '{""}'],
            ["2", "", '{"",NULL}'],
            ["3", None, '{"",NULL,x}'],
            ["4", "x", '{"",NULL,x,""}'],
        ], rows
        assert headers == ["id", "previous", "collected"], headers
        assert type_oids == [23, 25, 1009], type_oids
        assert command_tag == "SELECT 4", command_tag

        exact_text_sql = (
            "SELECT id, v, lag(v) OVER (ORDER BY id) AS previous "
            "FROM window_exact_text ORDER BY id;")
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], exact_text_sql),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [
            ["1", "", None],
            ["2", None, ""],
            ["3", "NULL", None],
            ["4", "a b", "NULL"],
        ], rows
        assert headers == ["id", "v", "previous"], headers
        assert type_oids == [23, 25, 25], type_oids
        assert command_tag == "SELECT 4", command_tag

        default_sql = (
            "SELECT id, lag(v, 10, '') OVER (ORDER BY id) AS empty_default, "
            "lag(v, 10, NULL) OVER (ORDER BY id) AS null_default, "
            "lag(v, 10, 'NULL') OVER (ORDER BY id) AS text_default "
            "FROM window_exact_text ORDER BY id;")
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], default_sql),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [[str(i), "", None, "NULL"] for i in range(1, 5)], rows
        assert headers == [
            "id", "empty_default", "null_default", "text_default"
        ], headers
        assert type_oids == [23, 25, 25, 25], type_oids
        assert command_tag == "SELECT 4", command_tag
        print("[WINDOW TYPE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
