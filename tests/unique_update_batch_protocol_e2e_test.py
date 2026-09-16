#!/usr/bin/env python3
"""Immediate UNIQUE keys produced by one UPDATE are statement-visible."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "unique_batch_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    sock = server["sock"]

    def query(sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    def ok(sql):
        result = query(sql)
        assert result[1] is None, (sql, result[1], result[2])
        return result

    try:
        for sql in [
            "CREATE TABLE batch_unique_wire "
            "(id INT PRIMARY KEY, tag TEXT UNIQUE);",
            "INSERT INTO batch_unique_wire VALUES (1, 'a'), (2, 'b');",
        ]:
            ok(sql)

        _, state, _, _, _, _ = query(
            "UPDATE batch_unique_wire SET tag = 'same';")
        assert state == "23505", state
        rows, state, message, _, _, _ = query(
            "SELECT id, tag FROM batch_unique_wire ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "a"], ["2", "b"]], rows

        # Row-dependent UPDATE ... FROM uses the same storage constraint
        # path; two distinct source rows cannot create the same final key.
        for sql in [
            "CREATE TABLE batch_source_wire (id INT, tag TEXT);",
            "INSERT INTO batch_source_wire VALUES (1, 'from'), (2, 'from');",
        ]:
            ok(sql)
        _, state, _, _, _, _ = query(
            "UPDATE batch_unique_wire AS dst SET tag = src.tag "
            "FROM batch_source_wire AS src WHERE dst.id = src.id;")
        assert state == "23505", state
        rows, state, message, _, _, _ = query(
            "SELECT id, tag FROM batch_unique_wire ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "a"], ["2", "b"]], rows

        for sql in [
            "CREATE TABLE batch_composite_wire "
            "(id INT PRIMARY KEY, a INT, b INT, UNIQUE (a, b));",
            "INSERT INTO batch_composite_wire VALUES (1, 7, 10), (2, 7, 20);",
        ]:
            ok(sql)
        _, state, _, _, _, _ = query(
            "UPDATE batch_composite_wire SET b = 30;")
        assert state == "23505", state
        rows, state, message, _, _, _ = query(
            "SELECT id, b FROM batch_composite_wire ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "10"], ["2", "20"]], rows

        for sql in [
            "CREATE TABLE batch_index_wire (id INT PRIMARY KEY, tag TEXT);",
            "CREATE UNIQUE INDEX batch_index_wire_tag_key "
            "ON batch_index_wire (tag);",
            "INSERT INTO batch_index_wire VALUES (1, 'x'), (2, 'y');",
        ]:
            ok(sql)
        _, state, _, _, _, _ = query(
            "UPDATE batch_index_wire SET tag = 'z';")
        assert state == "23505", state
        rows, state, message, _, _, _ = query(
            "SELECT id, tag FROM batch_index_wire ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "x"], ["2", "y"]], rows

        # Ordinary UNIQUE keeps NULLS DISTINCT behavior.
        _, state, message, _, command_tag, _ = query(
            "UPDATE batch_unique_wire SET tag = NULL;")
        assert state is None, (state, message)
        assert command_tag == "UPDATE 2", command_tag
        rows, state, message, _, _, _ = query(
            "SELECT id, tag FROM batch_unique_wire ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", None], ["2", None]], rows

        # An error inside a user transaction rolls back the UPDATE's partial
        # work. Rolling back to the user's savepoint preserves earlier work.
        ok("BEGIN;")
        ok("INSERT INTO batch_index_wire VALUES (9, 'prior');")
        ok("SAVEPOINT keep_prior;")
        _, state, _, _, _, _ = query(
            "UPDATE batch_index_wire SET tag = 'collision';")
        assert state == "23505", state
        ok("ROLLBACK TO SAVEPOINT keep_prior;")
        ok("COMMIT;")
        rows, state, message, _, _, _ = query(
            "SELECT id, tag FROM batch_index_wire ORDER BY id;")
        assert state is None, (state, message)
        assert rows == [["1", "x"], ["2", "y"], ["9", "prior"]], rows

        print("[UNIQUE UPDATE BATCH PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
