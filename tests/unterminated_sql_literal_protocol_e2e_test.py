#!/usr/bin/env python3
"""Raw quote errors must abort before Execute or a valid Simple Query prefix."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "unterminated_literal_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "unterminated_literal_" + uuid.uuid4().hex
    created = False
    bad = ("SELECT 'open", "SELECT 'doubled''", "SELECT E'open",
           "SELECT E'escaped\\'", 'SELECT "open', 'SELECT "doubled""',
           "SELECT $$open", "SELECT $tag$open$other$",
           "SELECT 'slash\\'; /* open")

    def query(sql, state=None, ready=b"T", rows=None):
        messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result, rows)
        return messages

    def invalid(sql, parse_only, ready):
        if parse_only:
            sock.sendall(client.typed(b"P", b"\0" + sql.encode() + b"\0\0\0") +
                         client.typed(b"H", b""))
            kind, payload = client.read_message(sock)
            assert kind == b"E", (sql, kind, payload)
            assert client.diagnostic_fields(payload).get(b"C") == b"42601", (sql, payload)
            sock.sendall(client.typed(b"S", b""))
            assert client.read_until_ready(sock) == [(b"Z", ready)], sql
        else:
            messages = query(sql, "42601", ready)
            assert not any(kind in (b"C", b"D", b"N") for kind, _ in messages), (sql, messages)

    def valid(sql, rows, headers, oids, extended):
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
        assert (result[0], result[1], result[3], result[4], result[5]) == (
            rows, None, headers, "SELECT 1", oids), (sql, extended, result)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);", ready=b"I")
        created = True
        query(f"INSERT INTO {table} VALUES(0);", ready=b"I")
        for parse_only in (False, True):
            for sql in bad:
                invalid(sql, parse_only, b"I")
                for child in (False, True):
                    query("BEGIN;")
                    query(f"INSERT INTO {table} VALUES(1);")
                    if child:
                        query("SAVEPOINT child;")
                        query(f"INSERT INTO {table} VALUES(2);")
                    invalid(sql, parse_only, b"E")
                    invalid(sql, parse_only, b"E")
                    query("SELECT 1;", "25P02", b"E")
                    if child:
                        query("ROLLBACK TO child;")
                        query(f"SELECT id FROM {table} ORDER BY id;", rows=[["0"], ["1"]])
                    query("ROLLBACK;", ready=b"I")
                    query(f"SELECT id FROM {table};", ready=b"I", rows=[["0"]])
        for sql in bad:
            invalid(f"INSERT INTO {table} VALUES(9); " + sql, False, b"I")
            query(f"SELECT id FROM {table};", ready=b"I", rows=[["0"]])
        for extended in (False, True):
            valid("SELECT 'slash\\' AS v;", [["slash\\"]], ["v"], [25], extended)
            valid("SELECT 'it''s' AS v;", [["it's"]], ["v"], [25], extended)
            valid("SELECT E'it\\'s';", [["it's"]], ["?column?"], [25], extended)
            valid("SELECT E'quote\\'';", [["quote'"]], ["?column?"], [25], extended)
            valid("SELECT $tag$/* data ' \" */$tag$ AS v;",
                  [["/* data ' \" */"]], ["v"], [25], extended)
            valid("SELECT 1 AS foo$tag$bar$tag$;", [["1"]], ["foo$tag$bar$tag$"], [23], extended)
        # Dollar characters are legal identifier continuations, not a quote
        # opener that can hide the following statement boundary.
        messages = query("SELECT 1 AS foo$tag$bar; SELECT 2;", ready=b"I", rows=[["1"], ["2"]])
        assert sum(kind == b"C" for kind, _ in messages) == 2, messages
        print("[UNTERMINATED SQL LITERAL " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        try:
            client.simple_query(sock, "ROLLBACK;")
            if created:
                client.simple_query(sock, f"DROP TABLE {table};")
        finally:
            if reference:
                sock.close()
            else:
                runner.stop_ours(server)


if __name__ == "__main__":
    main()
