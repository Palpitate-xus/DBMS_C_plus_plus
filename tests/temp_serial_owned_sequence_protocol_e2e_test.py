#!/usr/bin/env python3
"""Session SERIAL owns a real temporary sequence through rollback and cleanup."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "temp_serial_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "tmpser_" + uuid.uuid4().hex[:12]
    sequence = table + "_id_seq"
    peer = None

    def query(sql, extended=False, state=None, ready=b"I", rows=None, target=None):
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
        for extended in (False, True):
            query(f"CREATE TEMP TABLE {table}(id SERIAL PRIMARY KEY,v TEXT);", extended)
            query(f"INSERT INTO {table}(v) VALUES('one') RETURNING id;", extended, rows=[["1"]])
            query(f"SELECT currval('{sequence}');", extended, rows=[["1"]])
            query(f"SELECT nextval('pg_temp.{sequence}');", extended, rows=[["2"]])
            query(f"INSERT INTO {table}(v) VALUES('three') RETURNING id;", extended, rows=[["3"]])
            query(f"INSERT INTO {table} VALUES(NULL,'bad');", extended, "23502")
            query(f"TRUNCATE {table} RESTART IDENTITY;", extended)
            query(f"INSERT INTO {table}(v) VALUES('restart') RETURNING id;", extended, rows=[["1"]])
            query("BEGIN;", extended, ready=b"T")
            query("SAVEPOINT child;", extended, ready=b"T")
            query(f"DROP TABLE {table};", extended, ready=b"T")
            query("ROLLBACK TO child;", extended, ready=b"T")
            query(f"SELECT id,v FROM {table};", extended, ready=b"T", rows=[["1", "restart"]])
            query(f"SELECT nextval('{sequence}');", extended, ready=b"T", rows=[["2"]])
            query("ROLLBACK;", extended)
            query(f"ALTER TABLE {table} RENAME TO {table}_renamed;", extended)
            query(f"TRUNCATE {table}_renamed RESTART IDENTITY;", extended)
            query(f"INSERT INTO {table}_renamed(v) VALUES('renamed') RETURNING id;", extended, rows=[["1"]])
            query(f"DROP TABLE {table}_renamed;", extended)
            query(f"SELECT nextval('{sequence}');", extended, "42P01")
            query("BEGIN;", extended, ready=b"T")
            query(f"CREATE TEMP TABLE {table}(id SERIAL);", extended, ready=b"T")
            query("ROLLBACK;", extended)
            query(f"SELECT nextval('{sequence}');", extended, "42P01")
            query(f"CREATE TEMP TABLE {table}(id SERIAL) ON COMMIT DROP;", extended)
            query(f"SELECT nextval('{sequence}');", extended, "42P01")
        peer = socket.create_connection((host if reference else "127.0.0.1",
                                         port if reference else server["port"]), timeout=120)
        if reference:
            client.startup_reference(peer, user, database, password)
        else:
            client.startup(peer, "alice", "info")
        query(f"CREATE TEMP TABLE {table}(id SERIAL);", target=peer)
        query(f"INSERT INTO {table} DEFAULT VALUES RETURNING id;", target=peer, rows=[["1"]])
        query(f"SELECT nextval('{sequence}');", state="42P01")
        query(f"CREATE TEMP TABLE {table}(id SERIAL);", target=sock)
        query(f"INSERT INTO {table} DEFAULT VALUES RETURNING id;", rows=[["1"]])
        query(f"SELECT nextval('{sequence}');", target=peer, rows=[["2"]])
        query(f"DROP TABLE {table};")
        query(f"DROP TABLE {table};", target=peer)
        print("[TEMP SERIAL OWNED SEQUENCE " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if peer is not None:
            peer.close()
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
