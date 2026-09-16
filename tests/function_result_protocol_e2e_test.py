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
            ("CREATE FUNCTION fn_sql_add(x INT) RETURNS INT LANGUAGE sql "
             "AS $$ SELECT x + 1 $$"),
            ("CREATE FUNCTION fn_sql_label(x TEXT) RETURNS TEXT LANGUAGE sql "
             "AS $$ SELECT x || '-x-' $$"),
            ("CREATE FUNCTION fn_sql_echo(x INT) RETURNS INT LANGUAGE sql "
             "AS $$ SELECT x $$"),
            ("CREATE FUNCTION fn_sql_text_null() RETURNS TEXT LANGUAGE sql "
             "AS $$ SELECT 'null' $$"),
            ("CREATE FUNCTION fn_sql_from() RETURNS INT LANGUAGE sql "
             "AS $$ SELECT id FROM fn_inputs $$"),
            ("CREATE FUNCTION fn_strict(x INT) RETURNS INT STRICT "
             "LANGUAGE sql AS $$ SELECT 7 $$"),
            ("CREATE FUNCTION fn_strict_long(x INT) RETURNS INT "
             "RETURNS NULL ON NULL INPUT "
             "LANGUAGE plpgsql AS $$ BEGIN RETURN 7; END $$"),
            ("CREATE FUNCTION fn_called(x INT) RETURNS INT CALLED ON NULL INPUT "
             "LANGUAGE sql AS $$ SELECT 7 $$"),
            "CREATE TABLE fn_inputs (id INT, missing INT)",
            "INSERT INTO fn_inputs VALUES (5, NULL)",
        ]
        for sql in setup:
            rows, state, message, headers = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message, rows, headers)

        rejected = [
            ("CREATE FUNCTION bad_type() RETURNS no_such_type LANGUAGE sql "
             "AS $$ SELECT 1 $$"),
            ("CREATE FUNCTION bad_parameter(x no_such_type) RETURNS INT "
             "LANGUAGE sql AS $$ SELECT 1 $$"),
            ("CREATE FUNCTION bad_language() RETURNS INT LANGUAGE python "
             "AS $$ SELECT 1 $$"),
        ]
        for sql in rejected:
            rows, state, message, headers = runner.ours_query(
                client, server["sock"], sql)
            assert state is not None, (sql, rows, headers)

        rows, state, message, headers = runner.ours_query(
            client, server["sock"],
            "CREATE FUNCTION bad_result() RETURNS INT LANGUAGE sql "
            "AS $$ SELECT 'abc' $$")
        assert state is None, (state, message)
        rows, state, message, headers = runner.ours_query(
            client, server["sock"], "SELECT bad_result();")
        assert state is not None, (rows, headers)

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

        result = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT fn_strict(NULL) AS short_form, "
                "fn_strict_long(NULL) AS long_form, "
                "fn_called(NULL) AS called;"),
            include_types=True)
        rows, state, message, headers, tag, type_oids = result
        assert state is None, (state, message)
        assert rows == [[None, None, "7"]], rows
        assert type_oids == [23, 23, 23], type_oids
        assert tag == "SELECT 1", tag

        result = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT fn_sql_add(1 + 2) AS computed, "
                "fn_sql_label('ok') AS label, fn_sql_echo(NULL) AS missing, "
                "fn_sql_text_null() AS literal;"),
            include_types=True)
        rows, state, message, headers, tag, type_oids = result
        assert state is None, (state, message)
        assert rows == [["4", "ok-x-", None, "null"]], rows
        assert headers == ["computed", "label", "missing", "literal"], headers
        assert type_oids == [23, 25, 23, 25], type_oids
        assert tag == "SELECT 1", tag

        result = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "SELECT fn_twice(id) AS doubled, fn_sql_add(id) AS added, "
                "fn_sql_echo(missing) AS absent FROM fn_inputs;"),
            include_types=True)
        rows, state, message, headers, tag, type_oids = result
        assert state is None, (state, message)
        assert rows == [["10", "6", None]], rows
        assert headers == ["doubled", "added", "absent"], headers
        assert type_oids == [23, 23, 23], type_oids
        assert tag == "SELECT 1", tag

        for sql in ["SELECT fn_sql_add();", "SELECT fn_sql_from();"]:
            rows, state, message, headers = runner.ours_query(
                client, server["sock"], sql)
            assert state is not None, (sql, rows, headers)

        # An error after function execution must not poison the connection.
        rows, state, message, headers = runner.ours_query(
            client, server["sock"], "SELECT 1 AS alive;")
        assert state is None and rows == [["1"]], (state, message, rows)
        print("[FUNCTION RESULT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
