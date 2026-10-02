#!/usr/bin/env python3
"""Postfix cast types stop before AS, preserving prepared output aliases."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "cast_alias_runner", root / "tests/compat/pg_diff_runner.py")
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
    cases = (("1::integer", "1", 23), ("NULL::text", None, 25),
             ("1::integer::text", "1", 25), ("1::bigint", "1", 20))
    try:
        for expression, expected, oid in cases:
            sql = f'SELECT {expression} AS "Label Name";'
            sock.sendall(client.typed(b"P", b"cast_alias\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"D", b"Scast_alias\0") + client.typed(b"H"))
            messages = []
            while not any(kind == b"T" for kind, _ in messages):
                message = client.read_message(sock)
                assert message[0] != b"E", (sql, message)
                messages.append(message)
            assert [(field[0], field[3]) for field in client.row_description_fields(messages)] == [
                (b"Label Name", oid)], (sql, messages)
            sock.sendall(client.typed(b"B", b"\0cast_alias\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) + client.typed(b"S"))
            result = runner.decode_wire_result(client.read_until_ready(sock), include_types=True)
            assert (result[0], result[1], result[3], result[5], result[4]) == (
                [[expected]], None, ["Label Name"], [oid], "SELECT 1"), (sql, result)
            sock.sendall(client.typed(b"C", b"Scast_alias\0") + client.typed(b"S"))
            assert client.read_until_ready(sock)[-1] == (b"Z", b"I")
        print("[CAST EXPLICIT ALIAS " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
