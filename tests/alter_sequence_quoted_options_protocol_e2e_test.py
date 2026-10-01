#!/usr/bin/env python3
"""ALTER SEQUENCE preserves quoted option identifiers and owned-by binding."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "alter_sequence_quoted_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None):
        rows, state, message, _, _ = runner.decode_wire_result(client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    try:
        query('CREATE SCHEMA "alter space";')
        query('CREATE TABLE "alter space"."table.dot"("id value" BIGINT,"id""quote" BIGINT);')
        query('CREATE SEQUENCE "alter space"."seq.dot" START 19 INCREMENT 4;')
        query('ALTER SEQUENCE "alter space"."seq.dot" OWNED BY "alter space"."table.dot"."id value";')
        query('SELECT nextval(\'"alter space"."seq.dot"\');', [["19"]])
        query('ALTER SEQUENCE "alter space"."seq.dot" RENAME TO "renamed seq";')
        query('SELECT currval(\'"alter space"."renamed seq"\');', [["19"]])
        query('ALTER SEQUENCE "alter space"."renamed seq" OWNED BY "alter space"."table.dot"."id""quote" RESTART WITH 23;')
        query('SELECT nextval(\'"alter space"."renamed seq"\');', [["23"]])
        query('BEGIN;')
        query('ALTER SEQUENCE "alter space"."renamed seq" OWNED BY NONE;')
        query('ROLLBACK;')
        query('DROP TABLE "alter space"."table.dot";')
        _, state, message, _, _ = runner.decode_wire_result(client.simple_query(
            server["sock"], 'SELECT nextval(\'"alter space"."renamed seq"\');'))
        assert state == "42P01", (state, message)
        query('DROP SCHEMA "alter space";')
        print("[ALTER SEQUENCE QUOTED OPTIONS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
