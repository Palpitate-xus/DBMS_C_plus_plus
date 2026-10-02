#!/usr/bin/env python3
"""Statement and portal Describe use decoded quoted output names before Execute."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "extended_alias_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "extended_alias_" + uuid.uuid4().hex
    created = False
    try:
        for sql in (f"CREATE TABLE {table}(id INT, value TEXT);",
                    f"INSERT INTO {table} VALUES(1, 'data');"):
            result = runner.decode_wire_result(client.simple_query(sock, sql))
            assert result[1] is None, (sql, result)
            created = True
        cases = [
            ('SELECT 42 AS "constant value";', [["42"]], ["constant value"], [23]),
            ('SELECT 1 AS "quote"" alias", 2 AS "dot.alias";',
             [["1", "2"]], ['quote" alias', "dot.alias"], [23, 23]),
            (f'SELECT id AS "physical alias", value AS "text alias" FROM {table};',
             [["1", "data"]], ["physical alias", "text alias"], [23, 25]),
            (f'SELECT id + 1 AS "sum alias" FROM {table};', [["2"]], ["sum alias"], [23]),
            (f'SELECT count(*) AS "row count" FROM {table};', [["1"]], ["row count"], [20]),
            (f'SELECT id AS "zero rows" FROM {table} WHERE FALSE;', [], ["zero rows"], [23]),
            ('SELECT 3 AS plain;', [["3"]], ["plain"], [23]),
        ]
        for sql, rows, headers, oids in cases:
            # Describe the statement before Bind or Execute. There must be
            # no execution row/command from this metadata-only request.
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"D", b"S\0") + client.typed(b"H", b""))
            messages = [client.read_message(sock) for _ in range(3)]
            assert [kind for kind, _ in messages] == [b"1", b"t", b"T"], (sql, messages)
            description = runner.decode_wire_result(messages, include_types=True)
            assert (description[0], description[1], description[3], description[4], description[5]) == (
                [], None, headers, None, oids), (sql, description)
            sock.sendall(client.typed(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0)) +
                         client.typed(b"D", b"P\0") +
                         client.typed(b"E", b"\0" + struct.pack("!I", 0)) +
                         client.typed(b"S", b""))
            messages = client.read_until_ready(sock)
            result = runner.decode_wire_result(messages, include_types=True)
            assert (result[0], result[1], result[3], result[4], result[5]) == (
                rows, None, headers, "SELECT " + str(len(rows)), oids), (sql, result)
            assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        print("[EXTENDED QUOTED ALIAS " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        try:
            if created:
                client.simple_query(sock, f"DROP TABLE {table};")
        finally:
            if reference:
                sock.close()
            else:
                runner.stop_ours(server)


if __name__ == "__main__":
    main()
