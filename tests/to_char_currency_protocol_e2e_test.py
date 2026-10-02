#!/usr/bin/env python3
"""Numeric L templates use backend monetary locale, placement and sign."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "currency_runner", root / "tests/compat/pg_diff_runner.py")
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

    def query(sql, extended, expected=None):
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
        assert result[1] is None and messages[-1] == (b"Z", b"I"), (sql, result, messages[-1])
        if expected is not None:
            assert (result[0], result[3], result[5], result[4]) == (
                [[expected]], ["to_char"], [25], "SELECT 1"), (sql, result, expected)

    try:
        for extended in (False, True):
            for locale, symbol in (("C", " "), ("C.UTF-8", " "), ("en_US.utf8", "$")):
                query("SET lc_monetary = '" + locale + "';", extended)
                for number, digits in ((482, "482"), (-482, "-482")):
                    body = ("  " if number > 0 else " ") + digits
                    for template, value in (("L9999", symbol + body),
                                            ("9999L", body + symbol),
                                            ("FML9999", symbol + digits),
                                            ("FM9999L", digits + symbol),
                                            ("L9999.99", symbol + body + ".00"),
                                            ("9999.99L", body + ".00" + symbol)):
                        for spelling in (template, template.lower()):
                            query(f"SELECT to_char({number},'{spelling}');", extended, value)
            query("SET lc_monetary='C';", extended)
            query("SELECT to_char(482,'L9999');", extended, "   482")
        print("[TO CHAR CURRENCY " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
