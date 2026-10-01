#!/usr/bin/env python3
"""Successful AND CHAIN preserves READ ONLY, including SET TRANSACTION."""

import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "chain_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "chain_ro_" + uuid.uuid4().hex
    created = False

    def query(sql, rows=None, state=None, ready=b"I", tag=None):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)
        if tag is not None:
            assert result[4] == tag, (sql, result[4], tag)

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);")
        created = True
        for ending in ("COMMIT", "ROLLBACK"):
            query("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY;", ready=b"T")
            query(ending + " AND CHAIN;", ready=b"T", tag=ending)
            query(f"INSERT INTO {table} VALUES(10);", [], "25006", b"E")
            query("ROLLBACK;", tag="ROLLBACK")
        query("BEGIN;", ready=b"T")
        query("SET TRANSACTION READ ONLY;", ready=b"T")
        query("COMMIT AND CHAIN;", ready=b"T", tag="COMMIT")
        query(f"INSERT INTO {table} VALUES(20);", [], "25006", b"E")
        query("COMMIT;", tag="ROLLBACK")
        # A failed user subtransaction retains its parent's read-only mode.
        query("BEGIN READ ONLY;", ready=b"T")
        query("SAVEPOINT recover;", ready=b"T")
        query(f"INSERT INTO {table} VALUES(30);", [], "25006", b"E")
        query("COMMIT AND CHAIN;", ready=b"T", tag="ROLLBACK")
        query(f"INSERT INTO {table} VALUES(30);", [], "25006", b"E")
        query("ROLLBACK;")
        # Top-level abort resets characteristics before the failed block ends;
        # PostgreSQL therefore starts this chain read-write, not read-only.
        query("BEGIN READ ONLY;", ready=b"T")
        query(f"INSERT INTO {table} VALUES(40);", [], "25006", b"E")
        query("COMMIT AND CHAIN;", ready=b"T", tag="ROLLBACK")
        query(f"INSERT INTO {table} VALUES(40);", ready=b"T")
        query("ROLLBACK;")
        query("BEGIN;", ready=b"T")
        query(f"INSERT INTO {table} VALUES(1);", ready=b"T")
        query("COMMIT AND CHAIN;", ready=b"T", tag="COMMIT")
        query(f"INSERT INTO {table} VALUES(2);", ready=b"T")
        query("ROLLBACK AND CHAIN;", ready=b"T", tag="ROLLBACK")
        query(f"INSERT INTO {table} VALUES(3);", ready=b"T")
        query("ROLLBACK;")
        query("BEGIN READ ONLY;", ready=b"T")
        query("COMMIT AND NO CHAIN;", tag="COMMIT")
        query(f"INSERT INTO {table} VALUES(4);")
        query(f"SELECT id FROM {table} ORDER BY id;", [["1"], ["4"]])
        query(f"DROP TABLE {table};")
        created = False
        print("[TRANSACTION CHAIN READ ONLY " +
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
