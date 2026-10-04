#!/usr/bin/env python3
"""statement_timeout must interrupt lock waits in Simple and Extended Query."""

import importlib.util
from pathlib import Path
import socket
import struct


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "statement_timeout_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    waiter = server["sock"]
    waiter.settimeout(5)
    blocker = None

    def query(sock, sql, expected_state=None, expected_ready=None):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == expected_state, (sql, result)
        assert messages[-1][0] == b"Z", (sql, messages[-1])
        if expected_ready is not None:
            assert messages[-1][1] == expected_ready, (sql, messages[-1])
        return result

    def extended_query(sock, sql):
        parse = client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0")
        bind = client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
        describe = client.typed(b"D", b"P\0")
        execute = client.typed(b"E", b"\0" + struct.pack("!I", 0))
        sync = client.typed(b"S")
        sock.sendall(parse + bind + describe + execute + sync)
        messages = client.read_until_ready(sock)
        return runner.decode_wire_result(messages, include_types=True), messages

    try:
        for sql in (
                "CREATE TABLE statement_timeout_lock (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO statement_timeout_lock VALUES (1, 0)",
                "SET statement_timeout = 250"):
            result = query(waiter, sql)
            assert result[1] is None, (sql, result)
        timeout_setting = query(
            waiter,
            "SELECT setting FROM pg_catalog.pg_settings "
            "WHERE name = 'statement_timeout'")
        assert timeout_setting[0] == [["250"]], timeout_setting

        blocker = socket.create_connection(("127.0.0.1", server["port"]), timeout=5)
        blocker.settimeout(5)
        client.startup(blocker, "alice", "info")

        # An actual row-lock wait is bounded by the test socket timeout. Before
        # the fix, this waits for four seconds and raises socket.timeout.
        query(blocker, "BEGIN")
        query(blocker, "UPDATE statement_timeout_lock SET v = 1 WHERE id = 1")
        simple_result = query(
            waiter,
            "UPDATE statement_timeout_lock SET v = 2 WHERE id = 1",
            expected_state="57014")
        assert "statement timeout" in simple_result[2].lower(), simple_result
        assert simple_result[0] == [], simple_result
        query(blocker, "ROLLBACK")
        result = query(waiter, "SELECT v FROM statement_timeout_lock WHERE id = 1")
        assert result[0] == [["0"]], result

        # Inside an explicit transaction the timeout must leave PostgreSQL's
        # aborted-transaction ReadyForQuery state until ROLLBACK.
        query(waiter, "BEGIN", expected_ready=b"T")
        query(blocker, "BEGIN")
        query(blocker, "UPDATE statement_timeout_lock SET v = 5 WHERE id = 1")
        query(waiter,
              "UPDATE statement_timeout_lock SET v = 6 WHERE id = 1",
              expected_state="57014", expected_ready=b"E")
        query(waiter, "SELECT 1", expected_state="25P02", expected_ready=b"E")
        query(blocker, "ROLLBACK")
        query(waiter, "ROLLBACK", expected_ready=b"I")

        # Repeat through Parse/Bind/Execute, which has a separate wire path.
        query(blocker, "BEGIN")
        query(blocker, "UPDATE statement_timeout_lock SET v = 3 WHERE id = 1")
        extended, messages = extended_query(
            waiter,
            "UPDATE statement_timeout_lock SET v = 4 WHERE id = 1")
        assert extended[1] == "57014", extended
        assert "statement timeout" in extended[2].lower(), extended
        assert messages[-1] == (b"Z", b"I"), messages[-1]
        query(blocker, "ROLLBACK")
        result = query(waiter, "SELECT v FROM statement_timeout_lock WHERE id = 1")
        assert result[0] == [["0"]], result
        print("[STATEMENT TIMEOUT PROTOCOL E2E] Simple/Extended lock waits passed")
    finally:
        if blocker is not None:
            try:
                client.simple_query(blocker, "ROLLBACK")
            except Exception:
                pass
            blocker.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
