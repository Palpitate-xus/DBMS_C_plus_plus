#!/usr/bin/env python3
"""Repeated BEGIN preserves unstated options and existing transaction resources."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "redundant_begin_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "redundant_begin_" + uuid.uuid4().hex
    created = False
    peers = []
    key = int(uuid.uuid4().hex[:8], 16)

    def query(sql, extended=False, state=None, ready=b"T", rows=None, tag=None, target=None):
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
        if tag is not None:
            assert result[4] == tag, (sql, result, tag)
        return messages

    def warning(messages):
        notices = [client.diagnostic_fields(body) for kind, body in messages if kind == b"N"]
        assert len(notices) == 1, notices
        assert notices[0][b"C"] == b"25001", notices
        assert notices[0][b"S"] == b"WARNING", notices

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);", ready=b"I")
        created = True
        peer = socket.create_connection(
            (host if reference else "127.0.0.1", port if reference else server["port"]), timeout=120)
        peers.append(peer)
        if reference:
            client.startup_reference(peer, user, database, password)
        else:
            client.startup(peer, "alice", "info")
        for extended in (False, True):
            # Repeated BEGIN must not release transaction locks or erase
            # their pre/post-savepoint ownership boundaries.
            query("BEGIN;", extended)
            query(f"SELECT pg_advisory_xact_lock({key});", extended)
            query("SAVEPOINT keep;", extended)
            query(f"SELECT pg_advisory_xact_lock({key + 1});", extended)
            repeated = query("BEGIN;", extended, tag="BEGIN")
            query("BEGIN;", target=peer)
            query(f"SELECT pg_try_advisory_xact_lock({key});", rows=[["f"]], target=peer)
            query(f"SELECT pg_try_advisory_xact_lock({key + 1});", rows=[["f"]], target=peer)
            warning(repeated)
            query("ROLLBACK TO keep;", extended)
            query(f"SELECT pg_try_advisory_xact_lock({key + 1});", rows=[["t"]], target=peer)
            query(f"SELECT pg_try_advisory_xact_lock({key});", rows=[["f"]], target=peer)
            query("ROLLBACK;", extended, ready=b"I")
            query(f"SELECT pg_try_advisory_xact_lock({key});", rows=[["t"]], target=peer)
            query("ROLLBACK;", ready=b"I", target=peer)
            for ending in ("BEGIN;", "BEGIN WORK;", "START TRANSACTION;"):
                query("BEGIN READ ONLY;", extended)
                repeated = query(ending, extended,
                                 tag="START TRANSACTION" if ending.startswith("START") else "BEGIN")
                query(f"INSERT INTO {table} VALUES(10);", extended, "25006", b"E")
                warning(repeated)
                query("ROLLBACK;", extended, ready=b"I")
            for ending in ("BEGIN;", "START TRANSACTION;"):
                query("BEGIN READ WRITE;", extended)
                query(f"INSERT INTO {table} VALUES(10);", extended)
                query("SAVEPOINT keep;", extended)
                query(f"INSERT INTO {table} VALUES(20);", extended)
                repeated = query(ending, extended,
                                 tag="START TRANSACTION" if ending.startswith("START") else "BEGIN")
                query(f"INSERT INTO {table} VALUES(30);", extended)
                warning(repeated)
                query("ROLLBACK TO SAVEPOINT keep;", extended)
                query(f"SELECT id FROM {table} ORDER BY id;", extended, rows=[["10"]])
                query("ROLLBACK;", extended, ready=b"I")
            # Explicit options still take effect before the first query.
            # PostgreSQL does not ignore these just because BEGIN warns.
            query("BEGIN READ ONLY;", extended)
            repeated = query("BEGIN READ WRITE;", extended, tag="BEGIN")
            query(f"INSERT INTO {table} VALUES(40);", extended)
            warning(repeated)
            query("ROLLBACK;", extended, ready=b"I")
            query("BEGIN READ WRITE;", extended)
            repeated = query("START TRANSACTION READ ONLY;", extended, tag="START TRANSACTION")
            query(f"INSERT INTO {table} VALUES(40);", extended, "25006", b"E")
            warning(repeated)
            query("ROLLBACK;", extended, ready=b"I")
            # Explicit changes after snapshot use obey SET TRANSACTION's
            # error rules, rather than silently ignoring a failed setter.
            query("BEGIN READ ONLY;", extended)
            query(f"SELECT id FROM {table};", extended, rows=[])
            warning(query("BEGIN READ WRITE;", extended, "25001", b"E"))
            query("ROLLBACK;", extended, ready=b"I")
            query("BEGIN;", extended)
            query(f"SELECT id FROM {table};", extended, rows=[])
            warning(query("BEGIN ISOLATION LEVEL SERIALIZABLE;", extended, "25001", b"E"))
            query("ROLLBACK;", extended, ready=b"I")
        # A user BEGIN promotes an extended-query implicit block. It does
        # not warn as though an explicit BEGIN had already been issued.
        def packets(sql, flush=False):
            return (client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                    client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                    client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                    client.typed(b"H" if flush else b"S", b""))

        sock.sendall(packets("SELECT 1;", flush=True))
        while client.read_message(sock)[0] != b"C":
            pass
        sock.sendall(packets("BEGIN;"))
        messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages)
        assert result[1] is None and result[4] == "BEGIN", result
        assert messages[-1] == (b"Z", b"T"), messages[-1]
        assert not any(kind == b"N" for kind, _ in messages), messages
        query("ROLLBACK;", True, ready=b"I")
        query(f"SELECT id FROM {table};", ready=b"I", rows=[])
        query(f"DROP TABLE {table};", ready=b"I")
        created = False
        print("[REDUNDANT BEGIN " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
