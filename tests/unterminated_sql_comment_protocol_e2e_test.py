#!/usr/bin/env python3
"""An unfinished block is a raw parse error, including before Execute."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "unterminated_comment_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "unterminated_comment_" + uuid.uuid4().hex
    created = False

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
            fields = client.diagnostic_fields(payload)
            assert fields.get(b"C") == b"42601", (sql, fields)
            sock.sendall(client.typed(b"S", b""))
            assert client.read_until_ready(sock) == [(b"Z", ready)], sql
        else:
            messages = query(sql, "42601", ready)
            assert not any(kind in (b"C", b"D", b"N") for kind, _ in messages), (sql, messages)

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY);", ready=b"I")
        created = True
        query(f"INSERT INTO {table} VALUES(0);", ready=b"I")
        for parse_only in (False, True):
            for sql in ("SELECT 1 /* open", "SELECT 1 /* outer /* inner */",
                        "BEGIN /* open", "SHOW transaction_isolation /* open"):
                invalid(sql, parse_only, b"I")
                for child in (False, True):
                    query("BEGIN;")
                    query(f"INSERT INTO {table} VALUES(1);")
                    if child:
                        query("SAVEPOINT child;")
                        query(f"INSERT INTO {table} VALUES(2);")
                    invalid(sql, parse_only, b"E")
                    # Lexical syntax retains precedence in a failed block.
                    invalid(sql, parse_only, b"E")
                    query("SELECT 1;", "25P02", b"E")
                    if child:
                        query("ROLLBACK TO child;")
                        query(f"SELECT id FROM {table} ORDER BY id;", rows=[["0"], ["1"]])
                    query("ROLLBACK;", ready=b"I")
                    query(f"SELECT id FROM {table};", ready=b"I", rows=[["0"]])
        # Raw parsing of a whole Simple Query must fail before any prefix SQL
        # executes, even when that prefix is independently valid.
        invalid(f"INSERT INTO {table} VALUES(9); SELECT 1 /* open", False, b"I")
        query(f"SELECT id FROM {table};", ready=b"I", rows=[["0"]])
        query("SELECT '/* literal */' AS data;", ready=b"I", rows=[["/* literal */"]])
        print("[UNTERMINATED SQL COMMENT " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
