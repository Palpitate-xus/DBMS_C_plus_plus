#!/usr/bin/env python3
"""A final SQL semicolon is not an extra ORDER BY expression."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "order_terminator_runner", root / "tests/compat/pg_diff_runner.py")
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
    table = "order_end_" + uuid.uuid4().hex[:12]
    try:
        for sql in (f"CREATE TABLE {table}(id INT);",
                    f"INSERT INTO {table} VALUES(1),(2),(3);"):
            result = runner.decode_wire_result(client.simple_query(sock, sql))
            assert result[1] is None, (sql, result)
        for extended in (False, True):
            patterns = [("id AS n", "n", ["n"], [23], [["3"], ["2"], ["1"]])]
            for expression, datum in (("'; /* literal */'", "; /* literal */"),
                                      ("E'escaped; -- text'", "escaped; -- text"),
                                      ("$body$dollar; /* text */$body$", "dollar; /* text */")):
                patterns.append((f'id AS "n; label",{expression} AS data', "id",
                                 ["n; label", "data"], [23, 25],
                                 [["3", datum], ["2", datum], ["1", datum]]))
            for projection, key, headers, types, rows in patterns:
              for suffix in ("", ";", "; /* trailing */", "; -- trailing\n",
                             "; /* outer /* nested */ end */ -- trailing\n"):
                sql = f"SELECT {projection} FROM {table} ORDER BY {key} DESC" + suffix
                if extended:
                    sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                                 client.typed(b"D", b"S\0") + client.typed(b"H"))
                    metadata = [client.read_message(sock) for _ in range(3)]
                    assert [kind for kind, _ in metadata] == [b"1", b"t", b"T"], (sql, metadata)
                    described = runner.decode_wire_result(metadata, include_types=True)
                    assert (described[0], described[1], described[3], described[4], described[5]) == (
                        [], None, headers, None, types), (sql, described)
                    sock.sendall(client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                                 client.typed(b"D", b"P\0") +
                                 client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                                 client.typed(b"S"))
                    messages = client.read_until_ready(sock)
                else:
                    messages = client.simple_query(sock, sql)
                result = runner.decode_wire_result(messages, include_types=True)
                assert (result[0], result[1], result[3], result[4], result[5]) == (
                    rows, None, headers, "SELECT 3", types), (sql, result)
                assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        print("[ORDER BY TERMINATOR " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        try:
            client.simple_query(sock, "ROLLBACK;")
            client.simple_query(sock, f"DROP TABLE {table};")
        finally:
            if reference:
                sock.close()
            else:
                runner.stop_ours(server)


if __name__ == "__main__":
    main()
