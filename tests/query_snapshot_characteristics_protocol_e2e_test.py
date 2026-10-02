#!/usr/bin/env python3
"""Even table-free queries take a first snapshot; subabort does not undo that fact."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "query_snapshot_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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

    def query(sql, extended=False, state=None, ready=b"T", rows=None):
        if extended:
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S", b""))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)

    try:
        for extended in (False, True):
            # Leading-comment routing has its own strong regression in
            # leading_query_comments_protocol_e2e_test.py (issue 928).
            for sql, rows in (("SELECT 1;", [["1"]]), ("SELECT NULL;", [[None]]),
                              ("VALUES(1);", [["1"]])):
                for change in ("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
                               "BEGIN ISOLATION LEVEL SERIALIZABLE;", "BEGIN READ WRITE;"):
                    query("BEGIN READ ONLY;", extended)
                    query(sql, extended, rows=rows)
                    query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;", extended)
                    query(change, extended, "25001", b"E")
                    query("ROLLBACK;", extended, ready=b"I")
            for ending in ("RELEASE child;", "ROLLBACK TO child;"):
                query("BEGIN;", extended)
                query("SAVEPOINT child;", extended)
                query("SELECT 1;", extended, rows=[["1"]])
                query(ending, extended)
                if ending.startswith("ROLLBACK"):
                    query("RELEASE child;", extended)
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;", extended, "25001", b"E")
                query("ROLLBACK;", extended, ready=b"I")
            # Parse analysis itself takes a snapshot for a query; binding
            # an already prepared query in a fresh transaction does too.
            for phase in ("parse", "bind"):
                if phase == "bind":
                    sock.sendall(client.typed(b"P", b"snapshot_parse\0SELECT 1;\0\0\0") +
                                 client.typed(b"S", b""))
                    assert client.read_until_ready(sock) == [(b"1", b""), (b"Z", b"I")]
                query("BEGIN;", extended)
                packet = (client.typed(b"P", b"snapshot_parse\0SELECT 1;\0\0\0")
                          if phase == "parse" else
                          client.typed(b"B", b"snapshot_portal\0snapshot_parse\0" +
                                       struct.pack("!HHH", 0, 0, 0)))
                sock.sendall(packet + client.typed(b"S", b""))
                assert client.read_until_ready(sock) == [
                    (b"1" if phase == "parse" else b"2", b""), (b"Z", b"T")]
                query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;", extended, "25001", b"E")
                query("ROLLBACK;", extended, ready=b"I")
                sock.sendall(client.typed(b"C", b"Ssnapshot_parse\0") + client.typed(b"S", b""))
                assert client.read_until_ready(sock) == [(b"3", b""), (b"Z", b"I")]
            # A utility SHOW does not need a query snapshot, including Parse.
            query("BEGIN;", extended)
            sock.sendall(client.typed(b"P", b"\0SHOW transaction_isolation;\0\0\0") +
                         client.typed(b"S", b""))
            assert client.read_until_ready(sock) == [(b"1", b""), (b"Z", b"T")]
            query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;", extended)
            query("ROLLBACK;", extended, ready=b"I")
            # Transaction boundaries clear the first-snapshot marker.
            query("BEGIN;", extended)
            query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;", extended)
            query("SELECT 1;", extended, rows=[["1"]])
            query("COMMIT;", extended, ready=b"I")
            query("BEGIN READ ONLY;", extended)
            query("BEGIN READ WRITE;", extended)
            query("ROLLBACK;", extended, ready=b"I")
        print("[QUERY SNAPSHOT CHARACTERISTICS " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            try:
                client.simple_query(sock, "ROLLBACK;")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
