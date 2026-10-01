#!/usr/bin/env python3
"""Autocommit recheck failures preserve SQLSTATE after rollback and cleanup."""

import importlib.util
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "autocommit_deferred_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def extended(sql):
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
        bind = b"\0\0" + struct.pack("!HHH", 0, 0, 0)
        execute = b"\0" + struct.pack("!I", 0)
        server["sock"].sendall(client.typed(b"P", parse) + client.typed(b"B", bind) +
                               client.typed(b"E", execute) + client.typed(b"S"))
        return client.read_until_ready(server["sock"])

    def query(sql, expected=None, ready=b"I", transport=None):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    def error(sql, expected, ready=b"I", transport=None, expected_rows=None):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert rows == (expected_rows or []) and state == expected, (sql, rows, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        # Extended Execute can complete before Sync triggers the deferred
        # check. PG18.6 then sends CommandComplete followed by ErrorResponse.
        assert any(kind == b"C" for kind, _ in messages) == bool(transport), (sql, messages)

    try:
        query("CREATE TABLE auto_parent(id INT PRIMARY KEY);")
        query("INSERT INTO auto_parent VALUES(1);")
        query("CREATE TABLE auto_child(id INT PRIMARY KEY,pid INT,CONSTRAINT auto_fk FOREIGN KEY(pid) REFERENCES auto_parent(id) DEFERRABLE INITIALLY DEFERRED);")
        for transport in (None, extended):
            error("INSERT INTO auto_child VALUES(1,999);", "23503", transport=transport)
            query("SELECT id,pid FROM auto_child;", [], transport=transport)
            query("INSERT INTO auto_child VALUES(1,1);", transport=transport)
            error("UPDATE auto_child SET pid=999;", "23503", transport=transport)
            query("SELECT id,pid FROM auto_child;", [["1", "1"]], transport=transport)
            error("INSERT INTO auto_child VALUES(2,1),(3,999);", "23503", transport=transport)
            query("SELECT id FROM auto_child;", [["1"]], transport=transport)
            query("DELETE FROM auto_child;", transport=transport)
        query("CREATE TABLE auto_unique(id INT PRIMARY KEY,code INT,CONSTRAINT auto_unique_key UNIQUE(code) DEFERRABLE INITIALLY DEFERRED);")
        query("INSERT INTO auto_unique VALUES(1,7);")
        error("INSERT INTO auto_unique VALUES(2,7);", "23505")
        query("SELECT id,code FROM auto_unique;", [["1", "7"]])
        error("INSERT INTO auto_unique VALUES(2,8),(3,8) RETURNING id;", "23505", transport=extended, expected_rows=[["2"], ["3"]])
        query("SELECT id,code FROM auto_unique;", [["1", "7"]])
        query("BEGIN;", ready=b"T")
        query("SAVEPOINT auto_sp;", ready=b"T")
        query("INSERT INTO auto_child VALUES(1,999);", ready=b"T")
        error("SET CONSTRAINTS auto_fk IMMEDIATE;", "23503", b"E")
        error("SELECT id FROM auto_child;", "25P02", b"E")
        query("ROLLBACK TO SAVEPOINT auto_sp;", ready=b"T")
        query("SELECT id FROM auto_child;", [], b"T")
        query("COMMIT;")
        print("[AUTOCOMMIT DEFERRED CONSTRAINT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
