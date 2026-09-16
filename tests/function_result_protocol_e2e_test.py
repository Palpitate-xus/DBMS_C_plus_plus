#!/usr/bin/env python3
"""Scalar functions retain declared result types and SQL NULL on the wire."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "function_result_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            ("CREATE FUNCTION fn_twice(x INT) RETURNS INT LANGUAGE plpgsql "
             "AS $$ BEGIN RETURN x * 2; END $$"),
            ("CREATE FUNCTION fn_add(a INT, b INT) RETURNS BIGINT "
             "LANGUAGE plpgsql AS $$ BEGIN RETURN a + b; END $$"),
            ("CREATE FUNCTION fn_null() RETURNS INT LANGUAGE plpgsql "
             "AS $$ BEGIN RETURN NULL; END $$"),
            ("CREATE FUNCTION fn_text_null() RETURNS TEXT LANGUAGE plpgsql "
             "AS $$ BEGIN RETURN 'null'; END $$"),
            ("CREATE FUNCTION fn_flag() RETURNS BOOLEAN LANGUAGE plpgsql "
             "AS $$ BEGIN RETURN true; END $$"),
        ]
        for sql in setup:
            rows, state, message, headers = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message, rows, headers)

        result = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT fn_twice(4) AS doubled, fn_add(20, 22) AS total, "
                "fn_null() AS missing, fn_text_null() AS literal, "
                "fn_flag() AS flag;"),
            include_types=True)
        rows, state, message, headers, tag, type_oids = result
        assert state is None, (state, message)
        assert headers == ["doubled", "total", "missing", "literal", "flag"], headers
        assert rows == [["8", "42", None, "null", "t"]], rows
        assert type_oids == [23, 20, 23, 25, 16], type_oids
        assert tag == "SELECT 1", tag

        # An error after function execution must not poison the connection.
        rows, state, message, headers = runner.ours_query(
            client, server["sock"], "SELECT 1 AS alive;")
        assert state is None and rows == [["1"]], (state, message, rows)
        print("[FUNCTION RESULT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
