#!/usr/bin/env python3
"""FROM-less SELECT values cross the PostgreSQL protocol without text parsing."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "fromless_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        sql = (
            "SELECT 'left right' AS spaced, '' AS empty, "
            "'NULL' AS text_null, NULL AS sql_null, "
            "'line1\nline2' AS multiline, '  padded  ' AS padded, "
            "'a''b' AS quoted;"
        )
        rows, state, message, headers = runner.ours_query(
            client, server["sock"], sql)
        assert state is None, (state, message)
        assert headers == [
            "spaced", "empty", "text_null", "sql_null",
            "multiline", "padded", "quoted",
        ], headers
        assert rows == [[
            "left right", "", "NULL", None,
            "line1\nline2", "  padded  ", "a'b",
        ]], rows

        decoded = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 1 AS i, 1.5 AS n, true AS b, "
                "DATE '2024-03-15' AS d, "
                "TIMESTAMP '2024-03-15 10:30:00' AS ts, "
                "current_user AS u, pg_typeof(1) AS pt;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert headers == [
            "i", "n", "b", "d", "ts", "u", "pt",
        ], headers
        assert type_oids == [23, 1700, 16, 1082, 1114, 19, 2206], type_oids
        assert command_tag == "SELECT 1", command_tag

        integers = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 2147483647 AS i4, 2147483648 AS i8, "
                "9223372036854775808 AS n, -2147483648 AS neg_i4, "
                "-9223372036854775808 AS neg_i8, "
                "-9223372036854775809 AS neg_n;"),
            include_types=True)
        values, state, message, headers, tag, type_oids = integers
        assert state is None, (state, message)
        assert values == [[
            "2147483647", "2147483648", "9223372036854775808",
            "-2147483648", "-9223372036854775808",
            "-9223372036854775809",
        ]], values
        assert type_oids == [23, 20, 1700, 23, 20, 1700], type_oids
        assert tag == "SELECT 1", tag

        decoded = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT 'abc' AS plain, 'don''t' AS escaped, "
                "CASE WHEN true THEN 'yes' ELSE 'no' END AS choice, "
                "'kept'::varchar AS explicit_varchar;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [["abc", "don't", "yes", "kept"]], rows
        assert headers == ["plain", "escaped", "choice", "explicit_varchar"], headers
        assert type_oids == [25, 25, 25, 1043], type_oids
        assert command_tag == "SELECT 1", command_tag

        decoded = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT ARRAY[1,2] AS ints, "
                "string_to_array('a,b', ',') AS texts, exp(1) AS e, "
                "date_part('month', DATE '2026-05-06') AS part, "
                "to_timestamp('2026-08-15 14:30:05', "
                "'YYYY-MM-DD HH24:MI:SS') AS ts, "
                "regexp_matches('abc', '(a)(b)') AS matches;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert len(rows) == 1 and len(rows[0]) == 6, rows
        assert headers == ["ints", "texts", "e", "part", "ts", "matches"], headers
        assert type_oids == [1007, 1009, 701, 701, 1184, 1009], type_oids
        assert command_tag == "SELECT 1", command_tag

        rows, state, message, headers = runner.ours_query(
            client, server["sock"],
            'SELECT current_user AS "who am I", session_user AS su, '
            'user AS usr, current_database() AS db, current_schema() AS sch, '
            'pg_typeof(1) AS type_name, version() AS banner;')
        assert state is None, (state, message)
        assert headers == [
            "who am I", "su", "usr", "db", "sch", "type_name", "banner",
        ], headers
        assert len(rows) == 1 and len(rows[0]) == 7, rows

        decoded = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                'SELECT generate_series(1, 2) AS "series value";'),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert headers == ["series value"], headers
        assert rows == [["1"], ["2"]], rows
        assert type_oids == [23], type_oids
        assert command_tag == "SELECT 2", command_tag

        rows, state, message, headers = runner.ours_query(
            client, server["sock"],
            "SELECT 'still described' AS value WHERE false;")
        assert state is None, (state, message)
        assert headers == ["value"], headers
        assert rows == [], rows
        print("[FROMLESS STRUCTURED PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
