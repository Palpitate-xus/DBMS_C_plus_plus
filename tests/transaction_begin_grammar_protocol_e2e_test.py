#!/usr/bin/env python3
"""BEGIN grammar, mode lists, and syntax-error priority against PostgreSQL 18."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "begin_grammar_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "begin_grammar_" + uuid.uuid4().hex
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
        if state is not None:
            assert not any(kind == b"C" for kind, _ in messages), (sql, messages)
            assert not any(kind == b"N" for kind, _ in messages), (sql, messages)

    invalid_options = (
        "READ COMMITTED", "READ UNCOMMITTED", "REPEATABLE READ", "SERIALIZABLE",
        "ISOLATION READ COMMITTED", "ISOLATION SERIALIZABLE",
        "ISOLATION LEVEL", "ISOLATION LEVEL READ", "ISOLATION LEVEL REPEATABLE",
        ", READ ONLY", "READ ONLY,", "READ ONLY,, READ WRITE",
        "ISOLATION LEVEL READ COMMITTED, , READ ONLY",
    )
    valid_options = (
        ("ISOLATION LEVEL READ COMMITTED READ WRITE", False),
        ("ISOLATION /* level */ LEVEL READ COMMITTED, /* mode */ READ WRITE", False),
        ("ISOLATION LEVEL READ COMMITTED, READ WRITE", False),
        ("READ ONLY, ISOLATION LEVEL REPEATABLE READ", True),
        ("ISOLATION LEVEL SERIALIZABLE, READ ONLY, NOT DEFERRABLE", True),
        ("READ ONLY READ WRITE", False),
        ("READ WRITE, READ ONLY", True),
        ("ISOLATION LEVEL READ COMMITTED, ISOLATION LEVEL SERIALIZABLE", False),
        ("ISOLATION LEVEL SERIALIZABLE ISOLATION LEVEL READ COMMITTED", False),
        ("DEFERRABLE NOT DEFERRABLE", False),
        ("READ ONLY, READ WRITE ISOLATION LEVEL REPEATABLE READ, NOT DEFERRABLE", False),
    )
    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);", ready=b"I")
        created = True
        for extended in (False, True):
            for prefix in ("BEGIN", "BEGIN WORK", "BEGIN TRANSACTION", "START TRANSACTION"):
                for options in invalid_options:
                    query(prefix + " " + options + ";", extended, "42601", b"I")
                for options, read_only in valid_options:
                    query(prefix + " " + options + ";", extended,
                          tag="START TRANSACTION" if prefix.startswith("START") else "BEGIN")
                    query(f"INSERT INTO {table} VALUES(10);", extended,
                          "25006" if read_only else None, b"E" if read_only else b"T")
                    query("ROLLBACK;", extended, ready=b"I")
            # Invalid BEGIN is a syntax error, not a nested-BEGIN warning.
            # It aborts the active child, and retains the parent's recovery point.
            query("BEGIN;", extended)
            query(f"INSERT INTO {table} VALUES(1);", extended)
            query("SAVEPOINT parent;", extended)
            query(f"INSERT INTO {table} VALUES(2);", extended)
            query("BEGIN READ COMMITTED;", extended, "42601", b"E")
            query(f"SELECT id FROM {table};", extended, "25P02", b"E")
            for options in invalid_options:
                query("BEGIN " + options + ";", extended, "42601", b"E")
            query("BEGIN READ ONLY;", extended, "25P02", b"E")
            query("ROLLBACK TO parent;", extended)
            query(f"SELECT id FROM {table} ORDER BY id;", extended, rows=[["1"]])
            query("ROLLBACK;", extended, ready=b"I")
            query(f"SELECT id FROM {table};", extended, ready=b"I", rows=[])
        query(f"DROP TABLE {table};", ready=b"I")
        created = False
        print("[TRANSACTION BEGIN GRAMMAR " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
