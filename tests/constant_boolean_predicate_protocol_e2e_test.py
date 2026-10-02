#!/usr/bin/env python3
"""Constant SQL boolean predicates filter rows and must never become no WHERE."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "constant_boolean_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
    table = "constant_boolean_" + uuid.uuid4().hex
    created = False

    def query(sql, rows=None, tag=None, extended=False):
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
        assert result[1] is None, (sql, extended, result)
        if rows is not None:
            assert result[0] == rows, (sql, extended, result, rows)
        if tag is not None:
            assert result[4] == tag, (sql, extended, result, tag)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        return result

    try:
        query(f"CREATE TABLE {table}(id INT PRIMARY KEY, v TEXT);")
        created = True
        query(f"INSERT INTO {table} VALUES(1, 'original'), (2, 'original');")
        for extended in (False, True):
            for predicate in ("FALSE", "NULL", "(NULL)", "NOT TRUE", "FALSE OR NULL", "1=0"):
                result = query(f'SELECT id AS "zero rows" FROM {table} WHERE {predicate};',
                               [], "SELECT 0", extended)
                assert (result[3], result[5]) == (["zero rows"], [23]), result
                query(f"UPDATE {table} SET v='changed' WHERE {predicate};", [], "UPDATE 0", extended)
                query(f"DELETE FROM {table} WHERE {predicate};", [], "DELETE 0", extended)
                query(f"SELECT id, v FROM {table} ORDER BY id;",
                      [["1", "original"], ["2", "original"]], "SELECT 2")
            for predicate in ("TRUE", "NOT FALSE", "TRUE AND TRUE", "1=1"):
                query(f"SELECT id FROM {table} WHERE {predicate} ORDER BY id;",
                      [["1"], ["2"]], "SELECT 2", extended)
            query(f"UPDATE {table} SET v='true' WHERE TRUE;", [], "UPDATE 2", extended)
            query(f"SELECT v FROM {table} ORDER BY id;", [["true"], ["true"]])
            query(f"UPDATE {table} SET v='original' WHERE TRUE;", [], "UPDATE 2")
        query(f"DELETE FROM {table} WHERE TRUE;", [], "DELETE 2")
        for predicate in ("FALSE", "TRUE", "NULL", "NOT TRUE", "FALSE OR NULL"):
            query(f"SELECT id FROM {table} WHERE {predicate};", [], "SELECT 0")
            query(f"UPDATE {table} SET v='empty' WHERE {predicate};", [], "UPDATE 0")
            query(f"DELETE FROM {table} WHERE {predicate};", [], "DELETE 0")
        print("[CONSTANT BOOLEAN PREDICATE " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
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
