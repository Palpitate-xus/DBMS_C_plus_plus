#!/usr/bin/env python3
"""Repeated BEGIN checks each mode in order instead of folding to a final value."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "ordered_begin_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "ordered_begin_" + uuid.uuid4().hex
    created = False

    def query(sql, extended=False, state=None, ready=b"T", rows=None, repeated=False):
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
        if repeated:
            warnings = [client.diagnostic_fields(body) for kind, body in messages if kind == b"N"]
            assert len(warnings) == 1 and warnings[0][b"C"] == b"25001", warnings
            assert warnings[0][b"S"] == b"WARNING", warnings
            if state is not None:
                assert next(i for i, pair in enumerate(messages) if pair[0] == b"N") < next(
                    i for i, pair in enumerate(messages) if pair[0] == b"E"), messages

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);", ready=b"I")
        created = True
        for extended in (False, True):
            for prefix in ("BEGIN", "START TRANSACTION"):
                for separator in (" ", ", "):
                    for options in (
                        "ISOLATION LEVEL SERIALIZABLE" + separator + "ISOLATION LEVEL READ COMMITTED",
                        "ISOLATION LEVEL READ UNCOMMITTED" + separator + "ISOLATION LEVEL READ COMMITTED",
                        "READ ONLY" + separator + "READ WRITE",
                    ):
                        query("BEGIN;", extended)
                        query(f"SELECT id FROM {table};", extended, rows=[])
                        query(prefix + " " + options + ";", extended, "25001", b"E", repeated=True)
                        query("SELECT 1;", extended, "25P02", b"E")
                        query("ROLLBACK;", extended, ready=b"I")
                    # The same final modes are legal before snapshot use.
                    query("BEGIN;", extended)
                    query(prefix + " ISOLATION LEVEL SERIALIZABLE" + separator +
                          "ISOLATION LEVEL READ COMMITTED READ ONLY" + separator + "READ WRITE;",
                          extended, repeated=True)
                    query(f"INSERT INTO {table} VALUES(10);", extended)
                    query("ROLLBACK;", extended, ready=b"I")
                    # READ ONLY -> READ WRITE in a child is illegal even
                    # without a query; rolling it back restores its parent.
                    query("BEGIN;", extended)
                    query("SAVEPOINT child;", extended)
                    query(prefix + " READ ONLY" + separator + "READ WRITE;",
                          extended, "25001", b"E", repeated=True)
                    query("ROLLBACK TO child;", extended)
                    query(f"INSERT INTO {table} VALUES(20);", extended)
                    query("ROLLBACK;", extended, ready=b"I")
                    # A same-level option does not acquire a new snapshot.
                    query("BEGIN;", extended)
                    query(f"SELECT id FROM {table};", extended, rows=[])
                    query(prefix + " ISOLATION LEVEL READ COMMITTED" + separator +
                          "ISOLATION LEVEL READ COMMITTED;", extended, repeated=True)
                    query("ROLLBACK;", extended, ready=b"I")
        query(f"SELECT id FROM {table};", ready=b"I", rows=[])
        query(f"DROP TABLE {table};", ready=b"I")
        created = False
        print("[ORDERED BEGIN MODES " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            try:
                client.simple_query(sock, "ROLLBACK;")
                if created:
                    client.simple_query(sock, f"DROP TABLE {table};")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
