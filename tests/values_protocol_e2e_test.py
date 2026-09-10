#!/usr/bin/env python3
"""Bare VALUES publishes exact PostgreSQL protocol rows and type metadata."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "values_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        sql = (
            "VALUES ('hello world', '', 'NULL', NULL, 'line1\nline2', 1 + 2), "
            "('a,b', 'x', '', 'value', 'plain', 4);"
        )
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], sql), include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert headers == [
            "column1", "column2", "column3", "column4", "column5",
            "column6",
        ], headers
        assert rows == [
            ["hello world", "", "NULL", None, "line1\nline2", "3"],
            ["a,b", "x", "", "value", "plain", "4"],
        ], rows
        assert type_oids == [25, 25, 25, 25, 25, 23], type_oids
        assert command_tag == "SELECT 2", command_tag

        coerced = runner.decode_wire_result(
            client.simple_query(
                server["sock"], "VALUES (1), ('2'), (NULL);"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = coerced
        assert state is None, (state, message)
        assert rows == [["1"], ["2"], [None]], rows
        assert headers == ["column1"], headers
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 3", command_tag

        preserved = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "VALUES ('  padded  '), ('has space');"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = preserved
        assert state is None, (state, message)
        assert rows == [["  padded  "], ["has space"]], rows
        assert type_oids == [25], type_oids

        identity = runner.decode_wire_result(
            client.simple_query(server["sock"], "VALUES (current_user);"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = identity
        assert state is None, (state, message)
        assert rows == [["alice"]], rows
        assert type_oids == [19], type_oids

        promoted = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "VALUES (1, DATE '2024-03-15'), "
                "(2.5, TIMESTAMP '2024-03-16 10:30:00');"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = promoted
        assert state is None, (state, message)
        assert rows == [
            ["1", "2024-03-15 00:00:00"],
            ["2.5", "2024-03-16 10:30:00"],
        ], rows
        assert type_oids == [1700, 1114], type_oids
        assert command_tag == "SELECT 2", command_tag

        integer_ranges = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "VALUES (-2147483648, 2147483648, "
                "9223372036854775808);"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = integer_ranges
        assert state is None, (state, message)
        assert rows == [[
            "-2147483648", "2147483648", "9223372036854775808"
        ]], rows
        assert type_oids == [23, 20, 1700], type_oids

        _, state, _, _ = runner.ours_query(
            client, server["sock"], "VALUES (1), (2, 3);")
        assert state == "42601", state
        _, state, _, _ = runner.ours_query(
            client, server["sock"], "VALUES (1), (DATE '2024-03-15');")
        assert state == "42804", state
        _, state, _, _ = runner.ours_query(
            client, server["sock"], "VALUES ('1' || '2'), (3);")
        assert state == "42804", state

        error_cases = [
            ("VALUES (1 AS x);", "42601"),
            ("VALUES (1 bogus);", "42601"),
            ("VALUES (missing_name);", "42703"),
            ("VALUES (DEFAULT);", "42601"),
            ("VALUES (1,);", "42601"),
            ("VALUES (,1);", "42601"),
            ("VALUES (1),;", "42601"),
            ("VALUES (1), (true);", "42804"),
            ("VALUES (1), ('bad');", "22P02"),
            ("VALUES (ARRAY[1, 2]);", "0A000"),
        ]
        for query, expected_state in error_cases:
            _, state, _, _ = runner.ours_query(
                client, server["sock"], query)
            assert state == expected_state, (query, state)
            recovered = runner.decode_wire_result(
                client.simple_query(server["sock"], "VALUES (9);"),
                include_types=True)
            rows, state, message, headers, command_tag, type_oids = recovered
            assert state is None, (query, state, message)
            assert rows == [["9"]], (query, rows)
            assert type_oids == [23], (query, type_oids)
            assert command_tag == "SELECT 1", (query, command_tag)
        print("[VALUES STRUCTURED PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
