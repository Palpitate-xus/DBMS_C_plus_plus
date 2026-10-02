#!/usr/bin/env python3
"""Legal interior nested comments must not become SELECT projections."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "nested_comments_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
            for comment in ("/* outer /* inner */ end */", "/* a /* b /* c */ b */ a */",
                            "-- ignored\r", "-- ignored\r\n"):
                sql = "SELECT " + comment + " 1 AS n, " + comment + " '-- /* literal */' AS data;"
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
                assert result[0] == [["1", "-- /* literal */"]], (sql, result)
                assert result[3] == ["n", "data"] and result[5] == [23, 25], (sql, result)
                assert result[4] == "SELECT 1" and messages[-1] == (b"Z", b"I"), (sql, result)
        print("[NESTED SQL COMMENTS " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
