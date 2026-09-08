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
            "i", "n", "b", "d", "ts", "current_user", "pg_typeof",
        ], headers
        assert type_oids == [23, 1700, 16, 1082, 1114, 19, 2206], type_oids
        assert command_tag == "SELECT 1", command_tag

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
