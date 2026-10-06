#!/usr/bin/env python3
"""A SELECT blocked by an ALTER TABLE must report lock timeout, not no rows."""

import importlib.util
from pathlib import Path
import socket
import struct


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "table_lock_timeout_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    second = None

    def wire_query(sock, sql):
        return runner.decode_wire_result(
            client.simple_query(sock, sql), include_types=True)

    def query(sock, sql):
        rows, state, message, _, _, _ = wire_query(sock, sql)
        assert state is None, (sql, state, message)
        return rows

    def timeout(sql, extended=False):
        if extended:
            # Exercise the separate extended-query owner, preserving Sync
            # recovery after a failed BEGIN rather than weakening its state.
            parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
            bind = b"\0\0" + struct.pack("!HHH", 0, 0, 0)
            second.sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
            messages = client.read_until_ready(second)
        else:
            messages = client.simple_query(second, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == "55P03", (sql, extended, result)
        assert result[0] == [] and result[4] is None, (sql, extended, result)
        assert not any(kind in (b"D", b"C") for kind, _ in messages), (sql, messages)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        assert query(second, "SELECT 1;") == [["1"]]
        # The failed reader must not roll back the actual lock owner's txn.
        assert query(first, "SELECT id FROM table_lock_timeout;") == [["1"]]
        print("[TABLE LOCK TIMEOUT]", "extended" if extended else "simple", sql, "55P03 / recovered")

    try:
        second = socket.create_connection(("127.0.0.1", server["port"]), timeout=15)
        second.settimeout(15)
        client.startup(second, "alice", "info")
        first = server["sock"]
        query(first, "CREATE TABLE table_lock_timeout (id INT PRIMARY KEY);")
        query(first, "INSERT INTO table_lock_timeout VALUES (1);")
        query(first, "CREATE FUNCTION table_lock_timeout_write(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO table_lock_timeout(id) VALUES(arg); RETURN arg; END; $$;")
        query(second, "PREPARE table_lock_timeout_prepared AS SELECT id FROM table_lock_timeout;")
        query(second, "SET lock_timeout = 50;")
        query(first, "BEGIN;")
        query(first, "ALTER TABLE table_lock_timeout ADD COLUMN v INT;")
        assert query(first, "SELECT id FROM table_lock_timeout;") == [["1"]]
        rows, state, message, _, _, _ = wire_query(
            second, "SELECT id FROM table_lock_timeout;")
        assert state == "55P03", (rows, state, message)
        rows, state, message, _, _, _ = wire_query(
            second, "SELECT id FROM table_lock_timeout WHERE id = 1;")
        assert state == "55P03", (rows, state, message)
        assert query(second, "SELECT 1;") == [["1"]]
        for statement, expected in (
            ("SELECT 1;", [["1"]]),
            ("SELECT NULL;", [[None]]),
            ("SELECT 1+2*3;", [["7"]]),
            ("SELECT 'writer() FROM table_lock_timeout';", [["writer() FROM table_lock_timeout"]]),
        ):
            messages = client.simple_query(second, statement)
            result = runner.decode_wire_result(messages, include_types=True)
            assert result[1] is None and result[0] == expected, (statement, result)
            assert messages[-1] == (b"Z", b"I"), (statement, messages)
        # A primitive constant error is still the expression's exact error,
        # not a database-lock failure; there is no engine owner to leak.
        messages = client.simple_query(second, "SELECT 1/0;")
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == "22012" and result[0] == [], result
        assert messages[-1] == (b"Z", b"I"), messages
        assert query(second, "SELECT 1;") == [["1"]]
        for statement in (
            "SELECT id FROM table_lock_timeout;",
            "SELECT id FROM table_lock_timeout WHERE id=1;",
            "SELECT table_lock_timeout_write(90);",
            "INSERT INTO table_lock_timeout(id) VALUES(91);",
            "EXPLAIN SELECT id FROM table_lock_timeout;",
            "EXPLAIN ANALYZE SELECT table_lock_timeout_write(92);",
            "EXECUTE table_lock_timeout_prepared;",
        ):
            timeout(statement)
        timeout("SELECT id FROM table_lock_timeout;", extended=True)
        query(first, "ROLLBACK;")
        assert query(second, "SELECT id FROM table_lock_timeout;") == [["1"]]
        assert query(first, "SELECT id FROM table_lock_timeout;") == [["1"]]
        # Repeated failed starts must restore executeDepth so a following
        # writing function gets a real calling-statement owner and commits.
        assert query(second, "SELECT table_lock_timeout_write(2);") == [["2"]]
        assert query(first, "SELECT id FROM table_lock_timeout ORDER BY id;") == [["1"], ["2"]]
        print("[TABLE LOCK TIMEOUT PROTOCOL E2E] passed")
    finally:
        if second is not None:
            second.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
