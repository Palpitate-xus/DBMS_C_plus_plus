#!/usr/bin/env python3
"""Temporary indexes publish and remove real session-namespace catalog rows."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "temp_index_runner", root / "tests/compat/pg_diff_runner.py")
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
    table = "tmpidx_" + uuid.uuid4().hex[:12]
    index = table + "_index"

    def query(sql, extended=False, rows=None, state=None, ready=b"I"):
        if extended:
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S"))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)

    def count(expected, extended=False, ready=b"I"):
        query(f"SELECT count(*) FROM pg_class WHERE relname='{index}' "
              "AND relkind='i' AND relpersistence='t';", extended,
              [[str(expected)]], ready=ready)

    try:
        for extended in (False, True):
            query(f"CREATE TEMP TABLE {table}(id INT,v TEXT);", extended)
            query(f"INSERT INTO {table} VALUES(1,'kept'),(2,'other');", extended)
            query(f"CREATE INDEX {index} ON {table}(id);", extended)
            count(1, extended)
            query(f"CREATE INDEX {index} ON {table}(id);", extended, state="42P07")
            query("BEGIN;", extended, ready=b"T")
            query("SAVEPOINT before_drop;", extended, ready=b"T")
            query(f"DROP INDEX pg_temp.{index};", extended, ready=b"T")
            count(0, extended, b"T")
            query("ROLLBACK TO before_drop;", extended, ready=b"T")
            count(1, extended, b"T")
            query(f"SELECT id,v FROM {table} WHERE id=1;", extended,
                  [["1", "kept"]], ready=b"T")
            query("ROLLBACK;", extended)
            query(f"DROP INDEX {index};", extended)
            count(0, extended)
            query("BEGIN;", extended, ready=b"T")
            query(f"CREATE INDEX {index} ON {table}(id);", extended, ready=b"T")
            count(1, extended, b"T")
            query("ROLLBACK;", extended)
            count(0, extended)
            query(f"CREATE INDEX {index} ON {table}(id);", extended)
            query("BEGIN;", extended, ready=b"T")
            query("SAVEPOINT before_discard;", extended, ready=b"T")
            query("DISCARD TEMP;", extended, ready=b"T")
            count(0, extended, b"T")
            query("ROLLBACK TO before_discard;", extended, ready=b"T")
            count(1, extended, b"T")
            query(f"SELECT id,v FROM {table} WHERE id=1;", extended,
                  [["1", "kept"]], ready=b"T")
            query("COMMIT;", extended)
            count(1, extended)
            query("DISCARD TEMP;", extended)
            count(0, extended)
            query(f"DROP INDEX pg_temp.{index};", extended, state="42704")
        print("[TEMP INDEX CATALOG " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
