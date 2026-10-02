#!/usr/bin/env python3
"""A Simple Query finishes a pending implicit Parse/Bind transaction."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "mixed_query_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = "--reference" in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
        server = {"sock": sock}
    else:
        server = runner.start_ours(client)
        sock = server["sock"]

    def pending():
        sock.sendall(client.typed(b"P", b"mixed\0SELECT 1\0\0\0") +
                     client.typed(b"B", b"portal\0mixed\0" + struct.pack("!HHH", 0, 0, 0)) +
                     client.typed(b"H", b""))
        assert client.read_message(sock) == (b"1", b"")
        assert client.read_message(sock) == (b"2", b"")

    def query(sql, state=None, ready=b"I", rows=None):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)
        return messages

    def expired_portal():
        sock.sendall(client.typed(b"D", b"Pportal\0") + client.typed(b"S", b""))
        messages = client.read_until_ready(sock)
        assert runner.decode_wire_result(messages)[1] == "34000", messages
        assert messages[-1] == (b"Z", b"I"), messages
        # Prepared statements are session-scoped even when their portal ends.
        sock.sendall(client.typed(b"D", b"Smixed\0") + client.typed(b"S", b""))
        messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages, include_types=True)
        assert (result[1], result[3], result[5]) == (None, ["?column?"], [23]), result
        assert messages[-1] == (b"Z", b"I"), messages
        sock.sendall(client.typed(b"C", b"Smixed\0") + client.typed(b"S", b""))
        assert client.read_until_ready(sock) == [(b"3", b""), (b"Z", b"I")]

    try:
        for sql, state, rows in (("SELECT 42;", None, [["42"]]),
                                 ("SELECT 1 /* open", "42601", None),
                                 ("", None, []),
                                 ("COMMIT AND CHAIN;", "25P01", None)):
            pending()
            query(sql, state, rows=rows)
            expired_portal()
        pending()
        messages = query("BEGIN;", ready=b"T")
        assert not any(kind == b"N" for kind, _ in messages), messages
        # BEGIN promotes the existing transaction, retaining the snapshot
        # taken while Parse/Bind planned SELECT 1; it must not restart it.
        query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;", "25001", b"E")
        query("ROLLBACK;")
        expired_portal()
        print("[MIXED QUERY IMPLICIT LIFECYCLE " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
