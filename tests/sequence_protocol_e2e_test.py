#!/usr/bin/env python3
"""Sequence functions preserve PostgreSQL session state and wire metadata."""

import importlib.util
import socket
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "sequence_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    peer = socket.create_connection(("127.0.0.1", server["port"]), timeout=15)
    client.startup(peer, "alice", "info")

    def execute(sock, sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    def expect_error(sock, sql, expected):
        rows, state, message, _, _, _ = execute(sock, sql)
        assert rows == [], (sql, rows)
        assert state == expected, (sql, state, message)
        return message

    try:
        primary = server["sock"]
        _, state, message, _, _, _ = execute(
            primary, "CREATE SEQUENCE protocol_seq START 10;")
        assert state is None, message

        expect_error(primary, "SELECT currval('protocol_seq');", "55000")
        expect_error(primary, "SELECT lastval();", "55000")
        expect_error(peer, "SELECT currval('protocol_seq');", "55000")

        result = execute(
            primary,
            "SELECT nextval('protocol_seq') AS allocated, "
            "currval('protocol_seq') AS current_value, "
            "lastval() AS last_value;")
        rows, state, message, headers, tag, type_oids = result
        assert state is None, message
        assert rows == [["10", "10", "10"]], rows
        assert headers == ["allocated", "current_value", "last_value"], headers
        assert type_oids == [20, 20, 20], type_oids
        assert tag == "SELECT 1", tag

        # NULL arguments propagate without touching durable or session state.
        rows, state, message, _, tag, type_oids = execute(
            primary,
            "SELECT nextval(NULL), currval(NULL), setval(NULL, 99);")
        assert state is None, message
        assert rows == [[None, None, None]], rows
        assert type_oids == [20, 20, 20], type_oids
        assert tag == "SELECT 1", tag

        # setval(..., false) leaves currval unchanged and makes the supplied
        # value the next allocation. The true form updates currval but does
        # not change which sequence lastval refers to.
        rows, state, message, _, _, _ = execute(
            primary, "SELECT setval('protocol_seq', 30, false);")
        assert state is None and rows == [["30"]], (state, message, rows)
        rows, state, message, _, _, _ = execute(
            primary, "SELECT currval('protocol_seq'), lastval();")
        assert state is None and rows == [["10", "10"]], (state, message, rows)
        rows, state, message, _, _, _ = execute(
            primary, "SELECT nextval('protocol_seq');")
        assert state is None and rows == [["30"]], (state, message, rows)
        rows, state, message, _, _, _ = execute(
            primary, "SELECT setval('protocol_seq', 40);")
        assert state is None and rows == [["40"]], (state, message, rows)

        _, state, message, _, _, _ = execute(primary, "CREATE SEQUENCE other_seq;")
        assert state is None, message
        rows, state, message, _, _, _ = execute(
            primary, "SELECT setval('other_seq', 7), lastval();")
        assert state is None and rows == [["7", "40"]], (state, message, rows)

        # Sequence state is backend-local even though allocations are durable
        # and immediately visible to all backends.
        expect_error(peer, "SELECT currval('protocol_seq');", "55000")
        expect_error(peer, "SELECT lastval();", "55000")
        rows, state, message, _, _, _ = execute(
            peer, "SELECT nextval('protocol_seq');")
        assert state is None and rows == [["41"]], (state, message, rows)
        rows, state, message, _, _, _ = execute(
            primary, "SELECT currval('protocol_seq'), lastval();")
        assert state is None and rows == [["40", "40"]], (state, message, rows)

        for query in (
                "SELECT nextval();",
                "SELECT currval('protocol_seq', 'other_seq');",
                "SELECT lastval(1);",
                "SELECT setval('protocol_seq');"):
            expect_error(primary, query, "42883")
        expect_error(primary, "SELECT nextval('missing_sequence');", "42P01")
        _, state, message, _, _, _ = execute(
            primary, "CREATE TABLE not_a_sequence (id INT);")
        assert state is None, message
        expect_error(primary, "SELECT nextval('not_a_sequence');", "42809")

        # regclass lookup follows search_path and uses catalog OID identity,
        # so aliases and rename preserve currval while drop/recreate does not.
        for sql in (
                "CREATE SCHEMA sequence_app;",
                "CREATE SEQUENCE sequence_app.search_seq START 70;",
                "SET search_path TO sequence_app, public;"):
            _, state, message, _, _, _ = execute(primary, sql)
            assert state is None, (sql, message)
        rows, state, message, _, _, _ = execute(primary, "SELECT nextval('search_seq');")
        assert state is None and rows == [["70"]], (state, message, rows)
        rows, state, message, _, _, _ = execute(
            primary, "SELECT currval('sequence_app.search_seq');")
        assert state is None and rows == [["70"]], (state, message, rows)
        _, state, message, _, _, _ = execute(
            primary, "ALTER SEQUENCE sequence_app.search_seq RENAME TO renamed_seq;")
        assert state is None, message
        rows, state, message, _, _, _ = execute(primary, "SELECT currval('renamed_seq');")
        assert state is None and rows == [["70"]], (state, message, rows)
        _, state, message, _, _, _ = execute(primary, "DROP SEQUENCE renamed_seq;")
        assert state is None, message
        _, state, message, _, _, _ = execute(primary, "CREATE SEQUENCE renamed_seq;")
        assert state is None, message
        expect_error(primary, "SELECT currval('renamed_seq');", "55000")

        _, state, message, _, _, _ = execute(
            primary,
            "CREATE SEQUENCE bounded_seq START 2 MINVALUE 1 MAXVALUE 2;")
        assert state is None, message
        rows, state, message, _, _, _ = execute(primary, "SELECT nextval('bounded_seq');")
        assert state is None and rows == [["2"]], (state, message, rows)
        expect_error(primary, "SELECT nextval('bounded_seq');", "2200H")
        expect_error(primary, "SELECT setval('bounded_seq', 3);", "22003")

        # Sequence allocations and backend-local currval survive transaction
        # and savepoint rollback, matching PostgreSQL's non-MVCC behavior.
        _, state, message, _, _, _ = execute(primary, "BEGIN;")
        assert state is None, message
        _, state, message, _, _, _ = execute(primary, "SAVEPOINT seq_point;")
        assert state is None, message
        rows, state, message, _, _, _ = execute(primary, "SELECT nextval('protocol_seq');")
        assert state is None and rows == [["42"]], (state, message, rows)
        _, state, message, _, _, _ = execute(primary, "ROLLBACK TO seq_point;")
        assert state is None, message
        _, state, message, _, _, _ = execute(primary, "ROLLBACK;")
        assert state is None, message
        rows, state, message, _, _, _ = execute(
            primary, "SELECT currval('protocol_seq'), nextval('protocol_seq');")
        assert state is None and rows == [["42", "43"]], (state, message, rows)

        _, state, message, _, _, _ = execute(primary, "DISCARD SEQUENCES;")
        assert state is None, message
        expect_error(primary, "SELECT currval('protocol_seq');", "55000")
        expect_error(primary, "SELECT lastval();", "55000")
        rows, state, message, _, _, _ = execute(primary, "SELECT nextval('protocol_seq');")
        assert state is None and rows == [["44"]], (state, message, rows)
        _, state, message, _, _, _ = execute(primary, "DISCARD ALL;")
        assert state is None, message
        expect_error(primary, "SELECT lastval();", "55000")
        _, state, message, _, _, _ = execute(primary, "BEGIN;")
        assert state is None, message
        expect_error(primary, "DISCARD ALL;", "25001")
        _, state, message, _, _, _ = execute(primary, "ROLLBACK;")
        assert state is None, message
        print("[SEQUENCE PROTOCOL E2E] passed")
    finally:
        peer.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
