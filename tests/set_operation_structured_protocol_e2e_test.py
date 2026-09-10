#!/usr/bin/env python3
"""Set operations retain exact cells, NULL metadata, and common types."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "setop_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE setop_edges (id INT, v TEXT);",
            ("INSERT INTO setop_edges VALUES "
             "(1, 'a b'), (2, ''), (3, 'NULL'), (4, NULL), "
             "(5, 'line1\nline2'), (6, '  edge  ');")
        ]
        for sql in setup:
            _, state, message, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message)

        sql = ("SELECT upper(v) AS value FROM setop_edges "
               "UNION ALL SELECT 'tail'::text;")
        decoded = runner.decode_wire_result(
            client.simple_query(server["sock"], sql),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [
            ["A B"], [""], ["NULL"], [None], ["LINE1\nLINE2"],
            ["  EDGE  "], ["tail"]
        ], rows
        assert headers == ["value"], headers
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 7", command_tag

        distinct = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT v FROM setop_edges UNION SELECT v FROM setop_edges;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = distinct
        assert state is None, (state, message)
        assert rows == [
            ["a b"], [""], ["NULL"], [None], ["line1\nline2"],
            ["  edge  "]
        ], rows
        assert headers == ["v"], headers
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 6", command_tag

        promoted = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 1 AS n UNION SELECT count(*) FROM setop_edges;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = promoted
        assert state is None, (state, message)
        assert rows == [["1"], ["6"]], rows
        assert headers == ["n"], headers
        assert type_oids == [20], type_oids
        assert command_tag == "SELECT 2", command_tag

        ordered = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 10 AS n UNION ALL SELECT 2 ORDER BY n LIMIT 1;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = ordered
        assert state is None, (state, message)
        assert rows == [["2"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 1", command_tag

        null_ordered = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                ("SELECT NULL::text AS v UNION ALL SELECT ''::text "
                 "ORDER BY v NULLS FIRST;")),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = null_ordered
        assert state is None, (state, message)
        assert rows == [[None], [""]], rows
        assert type_oids == [25], type_oids
        assert command_tag == "SELECT 2", command_tag

        error_cases = [
            ("SELECT 1 UNION SELECT 1, 2;", "42601"),
            ("SELECT 1 UNION SELECT DATE '2024-01-01';", "42804"),
            ("SELECT 1 AS n UNION SELECT 2 ORDER BY missing;", "42P10"),
        ]
        for sql, expected_state in error_cases:
            _, state, _, _ = runner.ours_query(
                client, server["sock"], sql)
            assert state == expected_state, (sql, state)
            rows, state, message, _ = runner.ours_query(
                client, server["sock"], "SELECT 9;")
            assert state is None and rows == [["9"]], (
                sql, state, message, rows)

        print("[STRUCTURED SET OPERATION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
