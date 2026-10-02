#!/usr/bin/env python3
"""A SELECT terminator is not a fourth projection in Describe metadata."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "projection_terminator_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    try:
        for extended in (False, True):
            for suffix in ("", ";", "; /* trailing */", "; -- trailing\n"):
                sql = "SELECT 1 AS n, NULL AS absent, '; -- /* literal */' AS data" + suffix
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
                assert result[1] is None, (sql, result)
                assert result[0] == [["1", None, "; -- /* literal */"]], (sql, result)
                assert result[3] == ["n", "absent", "data"], (sql, result)
                assert result[4] == "SELECT 1", (sql, result)
                assert result[5] == [23, 25, 25], (sql, result)
                assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        print("[SELECT PROJECTION TERMINATOR " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
