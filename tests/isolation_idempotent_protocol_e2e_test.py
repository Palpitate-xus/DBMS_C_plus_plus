#!/usr/bin/env python3
"""Same-level SET is legal and preserves snapshots; changed levels reject child blocks."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "isolation_idempotent_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "isolation_idempotent_" + uuid.uuid4().hex
    created = False
    peers = []

    def query(sql, extended=False, state=None, ready=b"T", rows=None, target=None):
        active = sock if target is None else target
        if extended:
            active.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                           client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                           client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                           client.typed(b"S", b""))
            messages = client.read_until_ready(active)
        else:
            messages = client.simple_query(active, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);", ready=b"I")
        created = True
        query(f"INSERT INTO {table} VALUES(0);", ready=b"I")
        peer = socket.create_connection(
            (host if reference else "127.0.0.1", port if reference else server["port"]), timeout=120)
        peers.append(peer)
        if reference:
            client.startup_reference(peer, user, database, password)
        else:
            client.startup(peer, "alice", "info")
        for extended in (False, True):
            for level in ("READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"):
                other = "READ COMMITTED" if level != "READ COMMITTED" else "SERIALIZABLE"
                query("BEGIN ISOLATION LEVEL " + level + ";", extended)
                query(f"SELECT id FROM {table};", extended, rows=[["0"]])
                query("SET TRANSACTION ISOLATION LEVEL " + level + ";", extended)
                query("BEGIN ISOLATION LEVEL " + level + ";", extended)
                query("SAVEPOINT same_level;", extended)
                query("SET TRANSACTION ISOLATION LEVEL " + level + ";", extended)
                query("SET TRANSACTION ISOLATION LEVEL " + other + ";", extended, "25001", b"E")
                query("ROLLBACK TO same_level;", extended)
                query("ROLLBACK;", extended, ready=b"I")

                # No snapshot has been taken: a different level is still
                # forbidden within a user subtransaction.
                query("BEGIN ISOLATION LEVEL " + level + ";", extended)
                query("SAVEPOINT child;", extended)
                query("SET TRANSACTION ISOLATION LEVEL " + other + ";", extended, "25001", b"E")
                query("ROLLBACK TO child;", extended)
                query("SET TRANSACTION ISOLATION LEVEL " + level + ";", extended)
                query("RELEASE SAVEPOINT child;", extended)
                query("SET TRANSACTION ISOLATION LEVEL " + other + ";", extended)
                query("ROLLBACK;", extended, ready=b"I")

            query("BEGIN ISOLATION LEVEL REPEATABLE READ;", extended)
            query(f"SELECT id FROM {table} ORDER BY id;", extended, rows=[["0"]])
            query(f"INSERT INTO {table} VALUES(1);", ready=b"I", target=peer)
            query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;", extended)
            query(f"SELECT id FROM {table} ORDER BY id;", extended, rows=[["0"]])
            query("BEGIN ISOLATION LEVEL REPEATABLE READ;", extended)
            query(f"SELECT id FROM {table} ORDER BY id;", extended, rows=[["0"]])
            query("ROLLBACK;", extended, ready=b"I")
            query(f"SELECT id FROM {table} ORDER BY id;", extended,
                  ready=b"I", rows=[["0"], ["1"]])
            query(f"DELETE FROM {table} WHERE id=1;", ready=b"I", target=peer)
        query(f"DROP TABLE {table};", ready=b"I")
        created = False
        print("[ISOLATION IDEMPOTENT " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        for peer in peers:
            try:
                client.simple_query(peer, "ROLLBACK;")
            finally:
                peer.close()
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
