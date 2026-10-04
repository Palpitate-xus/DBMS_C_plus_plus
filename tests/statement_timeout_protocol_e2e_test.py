#!/usr/bin/env python3
"""statement_timeout must interrupt lock waits in Simple and Extended Query."""

import importlib.util
from pathlib import Path
import socket
import struct
import time


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
    partial_copy = None

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

        # COPY FROM STDIN blocks in the protocol input loop rather than the
        # executor's lock-wait loop. Its deadline must still abort and restore
        # an autocommit connection without waiting for CopyDone.
        waiter.sendall(client.typed(
            b"Q", b"COPY statement_timeout_lock (id, v) FROM STDIN\0"))
        kind, body = client.read_message(waiter)
        assert kind == b"G", (kind, body)
        time.sleep(0.4)
        copy_messages = client.read_until_ready(waiter)
        assert any(kind == b"E" and b"C57014\0" in body
                   for kind, body in copy_messages), copy_messages
        assert copy_messages[-1] == (b"Z", b"I"), copy_messages[-1]
        result = query(waiter, "SELECT v FROM statement_timeout_lock WHERE id = 1")
        assert result[0] == [["0"]], result

        # The same idle COPY wait through Parse/Bind/Describe/Execute must
        # retain its first-message deadline and recover only after Sync.
        copy_sql = b"COPY statement_timeout_lock (id, v) FROM STDIN"
        parse = client.typed(b"P", b"\0" + copy_sql + b"\0\0\0")
        bind = client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
        describe = client.typed(b"D", b"P\0")
        execute = client.typed(b"E", b"\0" + struct.pack("!I", 0))
        waiter.sendall(parse + bind + describe + execute)
        assert client.read_message(waiter) == (b"1", b"")
        assert client.read_message(waiter) == (b"2", b"")
        assert client.read_message(waiter)[0] == b"n"
        kind, body = client.read_message(waiter)
        assert kind == b"G", (kind, body)
        time.sleep(0.4)
        kind, body = client.read_message(waiter)
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        waiter.sendall(client.typed(b"S"))
        sync_messages = client.read_until_ready(waiter)
        assert sync_messages[-1] == (b"Z", b"I"), sync_messages[-1]

        # A client that begins a CopyData frame and then stalls must not block
        # the server past the statement deadline. Since part of a protocol
        # frame was consumed, the server reports the timeout and closes that
        # connection rather than misparsing the remaining frame bytes.
        partial_copy = socket.create_connection(
            ("127.0.0.1", server["port"]), timeout=5)
        partial_copy.settimeout(5)
        client.startup(partial_copy, "alice", "info")
        query(partial_copy, "SET statement_timeout = 250")
        partial_copy.sendall(client.typed(
            b"Q", b"COPY statement_timeout_lock (id, v) FROM STDIN\0"))
        kind, body = client.read_message(partial_copy)
        assert kind == b"G", (kind, body)
        partial_copy.sendall(client.typed(b"d", b"2\t8\n"))
        time.sleep(0.05)
        partial_copy.sendall(b"d" + struct.pack("!I", 100))
        time.sleep(0.4)
        kind, body = client.read_message(partial_copy)
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        assert partial_copy.recv(1) == b"", "partial protocol frame must close"
        result = query(
            waiter,
            "SELECT v FROM statement_timeout_lock WHERE id = 2")
        assert result[0] == [], result

        # PostgreSQL starts an extended-protocol timer at the first
        # query-related message, not only when Execute begins. Parse opens
        # the implicit snapshot here; with no later Bind/Execute, the server
        # must time out the pending cycle and recover on Sync.
        parse = client.typed(b"P", b"\0SELECT 1\0\0\0")
        waiter.sendall(parse)
        kind, body = client.read_message(waiter)
        assert (kind, body) == (b"1", b""), (kind, body)
        time.sleep(0.4)
        kind, body = client.read_message(waiter)
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        waiter.sendall(client.typed(b"S"))
        sync_messages = client.read_until_ready(waiter)
        assert sync_messages[-1] == (b"Z", b"I"), sync_messages[-1]

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
        print(
            "[STATEMENT TIMEOUT PROTOCOL E2E] Simple/Extended execution and "
            "COPY waits passed")
    finally:
        if blocker is not None:
            try:
                client.simple_query(blocker, "ROLLBACK")
            except Exception:
                pass
            blocker.close()
        if partial_copy is not None:
            partial_copy.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
