#!/usr/bin/env python3
"""Numeric zero, optional FM digits and sign follow the rounded output."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "numeric_zero_runner", root / "tests/compat/pg_diff_runner.py")
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
    cases = []
    for number in ("0", "0.004", "-0.004"):
        cases.extend((number, template, expected) for template, expected in (
            ("999", "   0"), ("FM999", "0"), ("0999", " 0000"),
            ("999.99", "    .00"), ("FM999.99", "0."),
            ("FM999.00", ".00"), ("FM999.09", ".0"),
            ("FM999.90", ".00"), ("FM0999.99", "0000.")))
    for number, sign in (("0.5", ""), ("-0.5", "-")):
        cases.extend((number, template, expected) for template, expected in (
            ("999.99", ("    " if not sign else "   -") + ".50"),
            ("FM999.99", sign + ".5"), ("FM999.00", sign + ".50"),
            ("FM0999.99", sign + "0000.5")))
    cases.extend((("1234.5", "FM9999.99", "1234.5"),
                  ("1234.5", "FM9999.00", "1234.50"),
                  ("3.1", "FM999.99", "3.1"),
                  ("3.1", "FM999.00", "3.10"),
                  ("-3.1", "999.99", "  -3.10")))
    cases.extend((("0", "L9999", "     0"),
                  ("-0.004", "L9999", "     0"),
                  ("0", "FML9999", " 0"),
                  ("0.5", "FML9999.99", " .5"),
                  ("-0.5", "FML9999.99", " -.5")))
    for number in ("482", "-482", "1234", "-1234"):
        sign = "-" if number.startswith("-") else "+"
        digits = number.lstrip("-")
        padded = digits.rjust(4)
        cases.extend((number, template, expected) for template, expected in (
            ("SG9999", sign + padded), ("9999SG", padded + sign),
            ("FMSG9999", sign + digits), ("FM9999SG", digits + sign),
            ("SG9999.00", sign + padded + ".00"),
            ("9999.00SG", padded + ".00" + sign)))
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
        print("[TO CHAR ZERO " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
