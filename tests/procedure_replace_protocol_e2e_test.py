#!/usr/bin/env python3
"""CREATE OR REPLACE PROCEDURE preserves identity and transactionality."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "procedure_replace_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            "CREATE TABLE procedure_values (value INT)",
            ("CREATE PROCEDURE replace_proc(x INT) LANGUAGE sql "
             "AS $$ INSERT INTO procedure_values VALUES (?x) $$"),
            "CALL replace_proc(1)",
            ("CREATE OR REPLACE PROCEDURE replace_proc(x INTEGER) "
             "AS $$ INSERT INTO procedure_values VALUES (20) $$ "
             "LANGUAGE sql"),
            "CALL replace_proc(2)",
            ("CREATE PROCEDURE typmod_proc(x NUMERIC(10,2), y INT) "
             "LANGUAGE sql AS $$ INSERT INTO procedure_values VALUES (40) $$"),
            ("CREATE OR REPLACE PROCEDURE typmod_proc(x NUMERIC(10,2), "
             "y INTEGER) LANGUAGE sql "
             "AS $$ INSERT INTO procedure_values VALUES (40) $$"),
            "CALL typmod_proc(1.25, 2)",
        ]
        for sql in setup:
            rows, state, message, headers = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message, rows, headers)

        rejected = [
            ("CREATE OR REPLACE PROCEDURE replace_proc(x BIGINT) "
             "LANGUAGE sql AS $$ SELECT 1 $$"),
            ("CREATE OR REPLACE PROCEDURE replace_proc(x INTEGER) "
             "LANGUAGE plpgsql AS $$ BEGIN NULL; END $$"),
            ("CREATE PROCEDURE bad_proc(x no_such_type) LANGUAGE sql "
             "AS $$ SELECT 1 $$"),
        ]
        for sql in rejected:
            rows, state, message, headers = runner.ours_query(
                client, server["sock"], sql)
            assert state is not None, (sql, rows, headers)

        rows, state, message, headers = runner.ours_query(
            client, server["sock"], "CALL replace_proc(3)")
        assert state is None, (state, message)

        transaction = [
            "BEGIN",
            ("CREATE OR REPLACE PROCEDURE replace_proc(x INT) LANGUAGE sql "
             "AS $$ INSERT INTO procedure_values VALUES (30) $$"),
            "ROLLBACK",
            "CALL replace_proc(4)",
        ]
        for sql in transaction:
            rows, state, message, headers = runner.ours_query(
                client, server["sock"], sql)
            assert state is None, (sql, state, message, rows, headers)

        rows, state, message, headers = runner.ours_query(
            client, server["sock"],
            "SELECT value FROM procedure_values ORDER BY value")
        assert state is None, (state, message)
        assert rows == [["1"], ["20"], ["20"], ["20"], ["40"]], rows
        print("[PROCEDURE REPLACE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
