#!/usr/bin/env python3
"""COPY signed integer text grammar, SQLSTATE and statement atomicity."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "copy_integer_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    created = []

    def query(sql, expected=None):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)
        if expected is not None:
            assert result[0] == expected, (sql, result[0], expected)

    def copy(table, records, state=None):
        sql = f"COPY {table}(id,v) FROM STDIN;".encode()
        sock.sendall(client.typed(b"Q", sql + b"\0"))
        assert client.read_message(sock) == (
            b"G", b"\0" + struct.pack("!H", 2) + b"\0\0" * 2)
        lines = []
        for identifier, value in records:
            field = ("\\N" if value is None else value.replace("\\", "\\\\")
                     .replace("\t", "\\t").replace("\n", "\\n")
                     .replace("\r", "\\r"))
            lines.append(str(identifier) + "\t" + field + "\n")
        sock.sendall(client.typed(b"d", "".join(lines).encode()) + client.typed(b"c", b""))
        messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (records, result)
        assert messages[-1] == (b"Z", b"I"), messages[-1]
        if state is None:
            assert result[4] == "COPY " + str(len(records)), result
        else:
            assert not any(kind == b"C" for kind, _ in messages), messages

    try:
        specifications = [
            ("SMALLINT", "32768", "-32769",
             ["-32768", "32767", " +0x_Ff \t", "0o_10", "0b_10", "1_000", "010", None],
             ["-32768", "32767", "255", "8", "2", "1000", "10", None]),
            ("INT", "2147483648", "-2147483649",
             ["-2147483648", "2147483647", "-0x_80", "0O_10", "+0b_110", "2_147_483_647", " 42\t", None],
             ["-2147483648", "2147483647", "-128", "8", "6", "2147483647", "42", None]),
            ("BIGINT", "9223372036854775808", "-9223372036854775809",
             ["-9223372036854775807", "9223372036854775807", "0x7fff_ffff_ffff_ffff", "-0X7fff_ffff_ffff_ffff", "-0b_1000", "0o_777", "1_000", None],
             ["-9223372036854775807", "9223372036854775807", "9223372036854775807", "-9223372036854775807", "-8", "511", "1000", None]),
        ]
        for datatype, above, below, inputs, outputs in specifications:
            table = "copy_int_" + uuid.uuid4().hex
            query(f"CREATE TABLE {table}(id INT PRIMARY KEY,v {datatype});")
            created.append(table)
            for invalid in ("bad", "", "1.2", "1__2", "0x__ff", "+ 1", "0b2", "0o8"):
                copy(table, [(1000, "1"), (1001, invalid)], "22P02")
                query(f"SELECT id,v FROM {table};", [])
            for overflow in (above, below, "999999999999999999999999999999999999"):
                copy(table, [(1000, "1"), (1001, overflow)], "22003")
                query(f"SELECT id,v FROM {table};", [])
            copy(table, list(enumerate(inputs, 1)))
            expected = [[str(i), value] for i, value in enumerate(outputs, 1)]
            query(f"SELECT id,v FROM {table} ORDER BY id;", expected)
            copy(table, [(1000, "1"), (1001, "bad")], "22P02")
            query(f"SELECT id,v FROM {table} ORDER BY id;", expected)
            query(f"DROP TABLE {table};")
            created.remove(table)
        print("[COPY INTEGER INPUT " +
              ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            try:
                client.simple_query(sock, "ROLLBACK;")
                for table in reversed(created):
                    client.simple_query(sock, f"DROP TABLE {table};")
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
