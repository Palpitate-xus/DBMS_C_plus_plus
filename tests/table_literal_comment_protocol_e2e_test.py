#!/usr/bin/env python3
"""Operators/comment delimiters inside table-projected literals are data."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "table_literal_runner", root / "tests/compat/pg_diff_runner.py")
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
    table = "literal_op_" + uuid.uuid4().hex[:12]
    cases = [("'/* literal */'", "/* literal */"), ("'; /* literal */'", "; /* literal */"),
             ("'a + b - c * d / e % f || g'", "a + b - c * d / e % f || g"),
             ("E'escaped; -- text'", "escaped; -- text"),
             ("$body$dollar; /* text */$body$", "dollar; /* text */"),
             ("'a''b * c'", "a'b * c"), ("''", "")]
    try:
        for sql in (f"CREATE TABLE {table}(id INT);", f"INSERT INTO {table} VALUES(1),(2);"):
            result = runner.decode_wire_result(client.simple_query(sock, sql))
            assert result[1] is None, (sql, result)
        for extended in (False, True):
            for expression, datum in cases:
                sql = f"SELECT id,{expression} AS data FROM {table} ORDER BY id;"
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
                assert (result[0], result[1], result[3], result[4], result[5]) == (
                    [["1", datum], ["2", datum]], None, ["id", "data"], "SELECT 2", [23, 25]), (sql, result)
                assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        print("[TABLE LITERAL COMMENT " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
