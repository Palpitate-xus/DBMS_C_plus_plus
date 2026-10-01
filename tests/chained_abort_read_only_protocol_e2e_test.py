#!/usr/bin/env python3
"""Abort of a CHAIN-origin block restores its inherited read-only mode."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "chained_abort_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "chained_abort_" + uuid.uuid4().hex
    created = False

    def query(sql, state=None, ready=b"I", tag=None, extended=False):
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
        if tag is not None:
            assert result[4] == tag, (sql, result[4], tag)

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);")
        created = True
        for extended in (False, True):
            for ending in ("COMMIT", "ROLLBACK"):
                def block(sql, state=None, ready=b"T", tag=None):
                    query(sql, state, ready, tag, extended)

                block("BEGIN READ ONLY;")
                block("COMMIT AND CHAIN;", tag="COMMIT")
                block(f"INSERT INTO {table} VALUES(1);", "25006", b"E")
                block(ending + " AND CHAIN;", tag="ROLLBACK")
                block(f"INSERT INTO {table} VALUES(1);", "25006", b"E")
                block("ROLLBACK;", ready=b"I", tag="ROLLBACK")

                # Abort restores the inherited baseline, not a later SET.
                block("BEGIN READ ONLY;")
                block("COMMIT AND CHAIN;")
                block("SET TRANSACTION READ WRITE;")
                block(f"INSERT INTO {table} VALUES(1);")
                block(f"INSERT INTO {table} VALUES(1);", "23505", b"E")
                block(ending + " AND CHAIN;", tag="ROLLBACK")
                block(f"INSERT INTO {table} VALUES(1);", "25006", b"E")
                block("ROLLBACK;", ready=b"I")

                block("BEGIN READ WRITE;")
                block("COMMIT AND CHAIN;")
                block("SET TRANSACTION READ ONLY;")
                block(f"INSERT INTO {table} VALUES(1);", "25006", b"E")
                block(ending + " AND CHAIN;", tag="ROLLBACK")
                block(f"INSERT INTO {table} VALUES(1);")
                block("ROLLBACK;", ready=b"I")

                # A successful ending inherits the current mode, including
                # SET TRANSACTION, rather than the abort-restoration baseline.
                block("BEGIN;")
                block("COMMIT AND CHAIN;")
                block("SET TRANSACTION READ ONLY;")
                block(ending + " AND CHAIN;")
                block(f"INSERT INTO {table} VALUES(1);", "25006", b"E")
                block("ROLLBACK;", ready=b"I")

                # A plain ending discards the previous CHAIN baseline.
                block("BEGIN READ ONLY;")
                block("COMMIT AND CHAIN;")
                block("ROLLBACK;", ready=b"I")
                block("BEGIN READ ONLY;")
                block(f"INSERT INTO {table} VALUES(1);", "25006", b"E")
                block(ending + " AND CHAIN;", tag="ROLLBACK")
                block(f"INSERT INTO {table} VALUES(1);")
                block("ROLLBACK;", ready=b"I")
        query(f"SELECT id FROM {table};", tag="SELECT 0")
        query(f"DROP TABLE {table};")
        created = False
        print("[CHAINED ABORT READ ONLY " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
