#!/usr/bin/env python3
"""Savepoint rollback and release restore the parent transaction's read-only mode."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "savepoint_read_only_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "savepoint_read_only_" + uuid.uuid4().hex
    created = False

    def query(sql, extended=False, state=None, ready=b"T", rows=None, tag=None):
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
            assert result[0] == rows, (sql, result, rows)
        if tag is not None:
            assert result[4] == tag, (sql, result, tag)

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);", ready=b"I")
        created = True
        for extended in (False, True):
            for fail in (False, True):
                query("BEGIN READ WRITE;", extended)
                query("SAVEPOINT mode;", extended)
                query("SET TRANSACTION READ ONLY;", extended)
                if fail:
                    query(f"INSERT INTO {table} VALUES(1);", extended, "25006", b"E")
                query("ROLLBACK TO mode;", extended)
                query(f"INSERT INTO {table} VALUES(1);", extended)
                query("ROLLBACK;", extended, ready=b"I")
            query("BEGIN READ WRITE;", extended)
            query("SAVEPOINT mode;", extended)
            query("SET TRANSACTION READ ONLY;", extended)
            query(f"INSERT INTO {table} VALUES(1);", extended, "25006", b"E")
            query("COMMIT AND CHAIN;", extended, tag="ROLLBACK")
            query(f"INSERT INTO {table} VALUES(1);", extended)
            query("ROLLBACK;", extended, ready=b"I")
            # PostgreSQL restores prevXactReadOnly on subcommit too.
            query("BEGIN;", extended)
            query("SAVEPOINT mode;", extended)
            query("SET TRANSACTION READ ONLY;", extended)
            query("RELEASE SAVEPOINT mode;", extended)
            query(f"INSERT INTO {table} VALUES(1);", extended)
            query("ROLLBACK;", extended, ready=b"I")
            # READ ONLY cannot be relaxed inside a user subtransaction,
            # even before the first query.
            query("BEGIN READ ONLY;", extended)
            query("SAVEPOINT mode;", extended)
            query("SET TRANSACTION READ WRITE;", extended, "25001", b"E")
            query("ROLLBACK TO mode;", extended)
            query(f"INSERT INTO {table} VALUES(1);", extended, "25006", b"E")
            query("ROLLBACK;", extended, ready=b"I")
            # Inner rollback restores its own mode; outer rollback restores
            # the distinct parent baseline.
            query("BEGIN;", extended)
            query("SAVEPOINT outer_mode;", extended)
            query("SET TRANSACTION READ ONLY;", extended)
            query("SAVEPOINT inner_mode;", extended)
            query("ROLLBACK TO inner_mode;", extended)
            query(f"INSERT INTO {table} VALUES(1);", extended, "25006", b"E")
            query("ROLLBACK TO outer_mode;", extended)
            query(f"INSERT INTO {table} VALUES(1);", extended)
            query("ROLLBACK;", extended, ready=b"I")
        query(f"SELECT id FROM {table};", ready=b"I", rows=[])
        query(f"DROP TABLE {table};", ready=b"I")
        created = False
        print("[SAVEPOINT READ ONLY " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
