#!/usr/bin/env python3
"""CHAIN requires an explicit block and retains options across SQL aliases."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "chain_block_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "chain_block_" + uuid.uuid4().hex
    created = False

    def packets(sql, flush=False):
        return (client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                client.typed(b"H" if flush else b"S", b""))

    def check(messages, state=None, ready=b"I", rows=None, tag=None):
        result = runner.decode_wire_result(messages)
        assert result[1] == state, result
        assert messages[-1] == (b"Z", ready), messages[-1]
        if rows is not None:
            assert result[0] == rows, (result, rows)
        if tag is not None:
            assert result[4] == tag, (result, tag)

    def query(sql, extended=False, **expected):
        if extended:
            sock.sendall(packets(sql))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        check(messages, **expected)

    try:
        endings = ("COMMIT", "ROLLBACK", "END WORK", "ABORT TRANSACTION")
        for extended in (False, True):
            for ending in endings:
                query(ending + " AND CHAIN;", extended, state="25P01", rows=[])
                query(ending + " AND NO CHAIN;", extended,
                      tag="COMMIT" if ending.startswith(("COMMIT", "END")) else "ROLLBACK")
            query("COMMIT AND CHAIN junk;", extended, state="42601")
        query("SELECT 1; COMMIT AND CHAIN;", state="25P01", rows=[["1"]])
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);")
        created = True
        # WORK/TRANSACTION before TO must retain the outer transaction and
        # its pre-savepoint writes, rather than dispatching a full rollback.
        for extended in (False, True):
            for modifier in ("WORK", "TRANSACTION"):
                query("BEGIN;", extended, ready=b"T")
                query(f"INSERT INTO {table} VALUES(10);", extended, ready=b"T")
                query("SAVEPOINT recovery;", extended, ready=b"T")
                query(f"INSERT INTO {table} VALUES(20);", extended, ready=b"T")
                query(f"INSERT INTO {table} VALUES(20);", extended, state="23505", ready=b"E")
                query(f"ROLLBACK {modifier} /*target*/ TO SAVEPOINT recovery;", extended,
                      ready=b"T", tag="ROLLBACK")
                query(f"SELECT id FROM {table} ORDER BY id;", extended,
                      ready=b"T", rows=[["10"]])
                query("ROLLBACK;", extended)
        # Failed-block COMMIT/END rewriting must not discard invalid suffixes
        # and silently accept an otherwise invalid transaction ending.
        for extended in (False, True):
            for ending in ("COMMIT", "END WORK"):
                for suffix in ("AND CHAIN junk", "AND", "garbage"):
                    query("BEGIN;", extended, ready=b"T")
                    query(f"INSERT INTO {table} VALUES(30);", extended, ready=b"T")
                    query(f"INSERT INTO {table} VALUES(30);", extended,
                          state="23505", ready=b"E")
                    query(ending + " " + suffix + ";", extended,
                          state="42601", ready=b"E")
                    query("SELECT 1;", extended, state="25P02", ready=b"E")
                    query("ROLLBACK;", extended, tag="ROLLBACK")
        for ending in endings:
            query("BEGIN READ ONLY;", ready=b"T")
            query(ending + " /*option*/ AND /*split*/ CHAIN;", ready=b"T")
            query(f"INSERT INTO {table} VALUES(1);", state="25006", ready=b"E")
            query("ROLLBACK;", tag="ROLLBACK")
        # A physically aborted ordinary BEGIN block is still a logical block.
        # Abort after a previous CHAIN has distinct GUC restoration semantics;
        # its read-only persistence is tracked separately, not assumed here.
        query("BEGIN READ ONLY;", ready=b"T")
        query(f"INSERT INTO {table} VALUES(1);", state="25006", ready=b"E")
        query("COMMIT AND CHAIN;", ready=b"T", tag="ROLLBACK")
        query(f"INSERT INTO {table} VALUES(1);", ready=b"T")
        query("ROLLBACK;")
        query("BEGIN;", True, ready=b"T")
        query("COMMIT /*option*/ AND /*split*/ CHAIN;", True, ready=b"T", tag="COMMIT")
        query("ROLLBACK;", True, tag="ROLLBACK")
        # Two Execute messages before Sync form an implicit block, not a
        # user BEGIN block. CHAIN must not promote it to a lasting block.
        sock.sendall(packets("SELECT 1;", flush=True))
        while client.read_message(sock)[0] != b"C":
            pass
        sock.sendall(packets("COMMIT AND CHAIN;", flush=True))
        messages = []
        while True:
            message = client.read_message(sock)
            messages.append(message)
            if message[0] in (b"E", b"C"):
                break
        result = runner.decode_wire_result(messages)
        assert result[1] == "25P01", result
        sock.sendall(client.typed(b"S", b""))
        check(client.read_until_ready(sock))
        query("SELECT 2;", rows=[["2"]])
        query(f"SELECT id FROM {table};", rows=[])
        query(f"DROP TABLE {table};")
        created = False
        print("[TRANSACTION CHAIN BLOCK " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
