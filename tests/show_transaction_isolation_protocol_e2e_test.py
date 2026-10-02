#!/usr/bin/env python3
"""SHOW returns the live isolation level without acquiring a query snapshot."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "show_isolation_pgdiff", root / "tests/compat/pg_diff_runner.py")
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

    def execute(sql, extended=False):
        if not extended:
            return client.simple_query(sock, sql)
        sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                     client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                     client.typed(b"D", b"P\0") +
                     client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                     client.typed(b"S", b""))
        return client.read_until_ready(sock)

    def query(sql, extended=False, state=None, ready=b"I", value=None, tag=None):
        messages = execute(sql, extended)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, extended, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if value is not None:
            assert (result[0], result[3], result[4], result[5]) == \
                ([[value]], ["transaction_isolation"], "SHOW", [25]), (sql, result)
        if tag is not None:
            assert result[4] == tag, (sql, result)

    def describe(sql, ready):
        sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                     client.typed(b"D", b"S\0") + client.typed(b"H", b""))
        messages = [client.read_message(sock) for _ in range(3)]
        assert [kind for kind, _ in messages] == [b"1", b"t", b"T"], messages
        result = runner.decode_wire_result(messages, include_types=True)
        assert (result[1], result[3], result[5]) == \
            (None, ["transaction_isolation"], [25]), result
        sock.sendall(client.typed(b"S", b""))
        assert client.read_until_ready(sock) == [(b"Z", ready)]

    try:
        aliases = ("SHOW transaction_isolation;",
                   "SHOW TRANSACTION ISOLATION LEVEL;",
                   'SHOW "TRANSACTION_ISOLATION";')
        for extended in (False, True):
            for sql in aliases:
                query(sql, extended, value="read committed")
                describe(sql, b"I")
            for level in ("READ UNCOMMITTED", "READ COMMITTED",
                          "REPEATABLE READ", "SERIALIZABLE"):
                query("BEGIN ISOLATION LEVEL " + level + ";", ready=b"T")
                for sql in aliases:
                    query(sql, extended, ready=b"T", value=level.lower())
                describe(aliases[0], b"T")
                # SHOW and Describe are utilities, not a first query snapshot.
                target = "SERIALIZABLE" if level != "SERIALIZABLE" else "READ COMMITTED"
                query("SET TRANSACTION ISOLATION LEVEL " + target + ";", ready=b"T")
                query(aliases[0], extended, ready=b"T", value=target.lower())
                query("SAVEPOINT shown;", ready=b"T")
                query(aliases[1], extended, ready=b"T", value=target.lower())
                query("ROLLBACK TO shown;", ready=b"T")
                query(aliases[0], extended, ready=b"T", value=target.lower())
                query("RELEASE shown;", ready=b"T")
                query("COMMIT AND CHAIN;", ready=b"T")
                query(aliases[0], extended, ready=b"T", value=target.lower())
                query("ROLLBACK;", tag="ROLLBACK")
                query(aliases[0], extended, value="read committed")
            for sql in ("SHOW transaction_isolation junk;", "SHOW TRANSACTION ISOLATION;",
                        "SHOW TRANSACTION ISOLATION LEVEL junk;"):
                query(sql, extended, state="42601")
            query("BEGIN;", ready=b"T")
            query("SELECT 1 /* unfinished", state="42601", ready=b"E")
            query(aliases[0], extended, state="25P02", ready=b"E")
            query("ROLLBACK;")
        print("[SHOW TRANSACTION ISOLATION " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
