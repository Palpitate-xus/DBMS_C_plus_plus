#!/usr/bin/env python3
"""pg_temp remains a namespace alias when no temporary objects exist."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "temp_index_pgdiff", root / "tests/compat/pg_diff_runner.py")
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
    missing = table + "_missing"

    def query(sql, extended=False, state=None, rows=None, tag=None, ready=b"I"):
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
            assert result[0] == rows, (sql, result)
        if tag is not None:
            assert result[4] == tag, (sql, result)

    try:
        namespace_started = False
        for extended in (False, True):
            for discard in (None, "ALL"):
                if discard:
                    query(f"CREATE TEMP TABLE {table}(id INT);", extended)
                    namespace_started = True
                    query("DISCARD " + discard + ";", extended, tag="DISCARD " + discard)
                missing_state = "42704" if namespace_started else "3F000"
                query(f"DROP INDEX pg_temp.{missing};", extended, missing_state)
                query(f'DROP INDEX "pg_temp".{missing};', extended, missing_state)
                query(f"DROP INDEX IF EXISTS pg_temp.{missing};", extended, tag="DROP INDEX")
                query(f"DROP INDEX {table}.{missing};", extended, "3F000")
                query(f'DROP INDEX "PG_TEMP".{missing};', extended, "3F000")
                query("SELECT 11;", extended, rows=[["11"]])
            query("BEGIN;", extended, ready=b"T")
            query("DISCARD ALL;", extended, "25001", ready=b"E")
            query("ROLLBACK;", extended)
        print("[PG TEMP MISSING INDEX " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
