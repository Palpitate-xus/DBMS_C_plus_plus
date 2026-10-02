#!/usr/bin/env python3
"""Temporary column changes update owned sequences and survive subabort."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "temp_alter_pgdiff", root / "tests/compat/pg_diff_runner.py")
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
    table = "tmpcol_" + uuid.uuid4().hex[:12]
    sequence = table + "_id_seq"

    def query(sql, extended=False, state=None, ready=b"I", rows=None,
              headers=None, oids=None, tag=None):
        if extended:
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S", b""))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        for expected, actual in ((rows, result[0]), (headers, result[3]),
                                 (oids, result[5]), (tag, result[4])):
            if expected is not None:
                assert actual == expected, (sql, result, expected)

    try:
        for extended in (False, True):
            query(f"CREATE TEMP TABLE {table}(id SERIAL,v VARCHAR(10));", extended)
            query(f"INSERT INTO {table}(v) VALUES('one') RETURNING id;", extended, rows=[["1"]])
            query(f"ALTER TABLE {table} ADD COLUMN note VARCHAR(5) DEFAULT 'kept';", extended)
            query(f"ALTER TABLE {table} RENAME COLUMN id TO new_id;", extended)
            query(f"INSERT INTO {table}(v) VALUES('two') RETURNING new_id;", extended, rows=[["2"]])
            query("BEGIN;", extended, ready=b"T")
            query("SAVEPOINT before_drop;", extended, ready=b"T")
            query(f"ALTER TABLE {table} DROP COLUMN new_id;", extended, ready=b"T")
            query(f"SELECT nextval('{sequence}');", extended, "42P01", b"E")
            query("ROLLBACK TO before_drop;", extended, ready=b"T")
            query(f"SELECT new_id,v,note FROM {table} ORDER BY new_id;", extended,
                  ready=b"T", rows=[["1", "one", "kept"], ["2", "two", "kept"]],
                  headers=["new_id", "v", "note"], oids=[23, 1043, 1043], tag="SELECT 2")
            query(f"SELECT nextval('{sequence}');", extended, ready=b"T", rows=[["3"]])
            query("ROLLBACK;", extended)
            query(f"ALTER TABLE pg_temp.{table} DROP COLUMN new_id;", extended)
            query(f"SELECT nextval('{sequence}');", extended, "42P01")
            query(f"SELECT v,note FROM {table} ORDER BY v;", extended,
                  rows=[["one", "kept"], ["two", "kept"]],
                  headers=["v", "note"], oids=[1043, 1043], tag="SELECT 2")
            query(f"DROP TABLE {table};", extended)
        print("[TEMP ALTER COLUMN CATALOG " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
