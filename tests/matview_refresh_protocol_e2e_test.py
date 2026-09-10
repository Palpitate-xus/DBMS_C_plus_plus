#!/usr/bin/env python3
"""REFRESH MATERIALIZED VIEW preserves typed values and atomic old data."""

import importlib.util
from pathlib import Path


def query(runner, client, server, sql):
    return runner.decode_wire_result(
        client.simple_query(server["sock"], sql), include_types=True)


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "matview_refresh_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        setup = [
            ("CREATE TABLE refresh_values ("
             "id INT, note VARCHAR(100), marker VARCHAR(100));"),
            ("CREATE MATERIALIZED VIEW refresh_values_mv AS "
             "SELECT id, note, marker FROM refresh_values;"),
            ("INSERT INTO refresh_values VALUES "
             "(1, 'hello world', ''), "
             "(2, '', NULL), "
             "(3, '  padded text  ', 'NULL'), "
             "(4, 'line one\nline two', 'two words');"),
        ]
        for sql in setup:
            _, state, message, _, _, _ = query(
                runner, client, server, sql)
            assert state is None, (sql, state, message)

        rows, state, message, headers, command_tag, type_oids = query(
            runner, client, server,
            "REFRESH MATERIALIZED VIEW refresh_values_mv;")
        assert state is None, (state, message)
        assert rows == [] and headers == [], (rows, headers)
        assert type_oids == [], type_oids
        assert command_tag == "REFRESH MATERIALIZED VIEW", command_tag

        expected = [
            ["1", "hello world", ""],
            ["2", "", None],
            ["3", "  padded text  ", "NULL"],
            ["4", "line one\nline two", "two words"],
        ]
        rows, state, message, headers, command_tag, type_oids = query(
            runner, client, server,
            "SELECT id, note, marker FROM refresh_values_mv ORDER BY id;")
        assert state is None, (state, message)
        assert rows == expected, rows
        assert headers == ["id", "note", "marker"], headers
        assert type_oids == [23, 1043, 1043], type_oids
        assert command_tag == "SELECT 4", command_tag

        # A WITH NO DATA materialized view exists but is not scannable until
        # it has been populated.  Both qualified and search_path resolution
        # must report PostgreSQL's object-not-in-prerequisite-state code.
        population_setup = [
            ("CREATE MATERIALIZED VIEW unpopulated_mv AS "
             "SELECT id, note FROM refresh_values WITH NO DATA;"),
            "CREATE SCHEMA reporting;",
            ("CREATE MATERIALIZED VIEW reporting.unpopulated_mv AS "
             "SELECT id, note FROM refresh_values WITH NO DATA;"),
        ]
        for sql in population_setup:
            _, state, message, _, _, _ = query(
                runner, client, server, sql)
            assert state is None, (sql, state, message)

        for sql in [
                "SELECT * FROM unpopulated_mv;",
                "SELECT * FROM public.unpopulated_mv;",
                "SELECT * FROM reporting.unpopulated_mv;"]:
            _, state, _, _, _, _ = query(runner, client, server, sql)
            assert state == "55000", (sql, state)

        _, state, message, _, _, _ = query(
            runner, client, server,
            "SET search_path TO reporting, public;")
        assert state is None, (state, message)
        _, state, _, _, _, _ = query(
            runner, client, server, "SELECT * FROM unpopulated_mv;")
        assert state == "55000", state

        # REFRESH uses the same namespace rules and publishes the new state
        # only after its existing atomic replacement commits.
        _, state, message, _, command_tag, _ = query(
            runner, client, server,
            "REFRESH MATERIALIZED VIEW unpopulated_mv;")
        assert state is None, (state, message)
        assert command_tag == "REFRESH MATERIALIZED VIEW", command_tag
        rows, state, message, headers, command_tag, _ = query(
            runner, client, server,
            "SELECT id, note FROM unpopulated_mv ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [row[:2] for row in expected], rows
        assert headers == ["id", "note"], headers
        assert command_tag == "SELECT 4", command_tag

        _, state, message, _, _, _ = query(
            runner, client, server,
            "SET search_path TO public;")
        assert state is None, (state, message)
        _, state, message, _, _, _ = query(
            runner, client, server,
            "REFRESH MATERIALIZED VIEW unpopulated_mv;")
        assert state is None, (state, message)
        rows, state, message, _, _, _ = query(
            runner, client, server,
            "SELECT id FROM public.unpopulated_mv ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1"], ["2"], ["3"], ["4"]], rows

        _, state, message, _, command_tag, _ = query(
            runner, client, server,
            "REFRESH MATERIALIZED VIEW public.unpopulated_mv WITH NO DATA;")
        assert state is None, (state, message)
        assert command_tag == "REFRESH MATERIALIZED VIEW", command_tag
        _, state, _, _, _, _ = query(
            runner, client, server,
            "SELECT * FROM public.unpopulated_mv;")
        assert state == "55000", state
        _, state, message, _, _, _ = query(
            runner, client, server,
            "REFRESH MATERIALIZED VIEW public.unpopulated_mv WITH DATA;")
        assert state is None, (state, message)

        _, state, message, _, _, _ = query(
            runner, client, server,
            "INSERT INTO refresh_values VALUES (5, 'new', 'row');")
        assert state is None, (state, message)
        _, state, _, _, _, _ = query(
            runner, client, server,
            "REFRESH MATERIALIZED VIEW CONCURRENTLY refresh_values_mv;")
        assert state == "0A000", state
        rows, state, message, _, _, _ = query(
            runner, client, server,
            "SELECT id, note, marker FROM refresh_values_mv ORDER BY id;")
        assert state is None, (state, message)
        assert rows == expected, rows

        failure_setup = [
            "CREATE TABLE refresh_failure_source (id INT, note TEXT);",
            "INSERT INTO refresh_failure_source VALUES (1, 'old value');",
            ("CREATE MATERIALIZED VIEW refresh_failure_mv AS "
             "SELECT id, note FROM refresh_failure_source;"),
            ("ALTER TABLE refresh_failure_source "
             "ALTER COLUMN id TYPE VARCHAR(20);"),
            ("INSERT INTO refresh_failure_source VALUES "
             "('not-an-integer', 'invalid replacement');"),
        ]
        for sql in failure_setup:
            _, state, message, _, _, _ = query(
                runner, client, server, sql)
            assert state is None, (sql, state, message)
        _, state, _, _, _, _ = query(
            runner, client, server,
            "REFRESH MATERIALIZED VIEW refresh_failure_mv;")
        assert state == "22023", state
        rows, state, message, _, command_tag, _ = query(
            runner, client, server,
            "SELECT id, note FROM refresh_failure_mv;")
        assert state is None, (state, message)
        assert rows == [["1", "old value"]], rows
        assert command_tag == "SELECT 1", command_tag

        print("[MATVIEW REFRESH PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
