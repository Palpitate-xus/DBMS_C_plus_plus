#!/usr/bin/env python3
"""Basic numeric templates retain decimal precision and PostgreSQL rounding."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "exact_format_runner", root / "tests/compat/pg_diff_runner.py")
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
    cases = (("0.5", "FM999", "1"), ("-0.5", "FM999", "-1"),
             ("1.005", "FM999.00", "1.01"), ("-1.005", "FM999.00", "-1.01"),
             ("9.995", "FM99.00", "10.00"), ("-9.995", "FM99.00", "-10.00"),
             ("9007199254740993", "FM9999999999999999", "9007199254740993"),
             ("9007199254740993::bigint", "FM9999999999999999", "9007199254740993"),
             ("9223372036854775807", "FM9999999999999999999", "9223372036854775807"),
             ("9007199254740993.005", "FM9999999999999999.00", "9007199254740993.01"),
             ("1.125", "FM999V99", "113"), ("-1.125", "FM999V99", "-113"),
             ("0.005", "FM999V99", "1"), ("-0.005", "FM999V99", "-1"),
             ("999.995", "FM99.00", "##.##"), ("-999.995", "FM99.00", "-##.##"))
    try:
        for extended in (False, True):
            for number, template, expected in cases:
                sql = f"SELECT to_char({number},'{template}');"
                if extended:
                    sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                                 client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                                 client.typed(b"D", b"P\0") +
                                 client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                                 client.typed(b"S"))
                    messages = client.read_until_ready(sock)
                else:
                    messages = client.simple_query(sock, sql)
                result = runner.decode_wire_result(messages, include_types=True)
                assert (result[0], result[1], result[3], result[5], result[4]) == (
                    [[expected]], None, ["to_char"], [25], "SELECT 1"), (sql, result, expected)
                assert messages[-1] == (b"Z", b"I"), messages
        print("[TO CHAR EXACT ROUNDING " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
