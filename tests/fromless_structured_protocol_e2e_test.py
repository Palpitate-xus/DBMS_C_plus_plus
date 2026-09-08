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
