#!/usr/bin/env python3
"""ADD CHECK rejects existing bad rows with 23514 and publishes no constraint."""

import importlib.util
from pathlib import Path
import socket
import struct
import sys


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "check_add_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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

    def query(sql, extended, state=None, ready=b"I", rows=None, tag=None):
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
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result)
            if sql.startswith("SELECT id,value"):
                names = (["id", "value", "left_value", "right_value"]
                         if sql.startswith("SELECT id,value,left_value,right_value")
                         else ["id", "value"])
                assert result[3] == names and result[5] == [23] * len(names), (sql, result)
        if tag is not None:
            assert result[4] == tag, (sql, result)
        if state is not None:
            assert result[0] == [] and result[4] is None, (sql, result)

    try:
        for extended in (False, True):
            table = "check_add_ext" if extended else "check_add_simple"
            constraint = table + "_positive"
            add = f"ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK(value > 0);"
            query(f"CREATE TABLE {table}(id INT PRIMARY KEY,value INT);", extended, tag="CREATE TABLE")
            query(f"INSERT INTO {table} VALUES(1,-1),(2,NULL);", extended, tag="INSERT 0 2")
            query(add, extended, "23514")
            query(f"SELECT id,value FROM {table} ORDER BY id;", extended,
                  rows=[["1", "-1"], ["2", None]], tag="SELECT 2")
            # The rejected name was not installed, and no CHECK is enforced yet.
            query(f"INSERT INTO {table} VALUES(3,-2);", extended, tag="INSERT 0 1")
            query("BEGIN;", extended, ready=b"T")
            query("SAVEPOINT check_sp;", extended, ready=b"T")
            query(add, extended, "23514", b"E")
            query(f"SELECT id,value FROM {table};", extended, "25P02", b"E")
            query("ROLLBACK TO check_sp;", extended, ready=b"T")
            query(f"UPDATE {table} SET value=1 WHERE value < 0;", extended, ready=b"T", tag="UPDATE 2")
            query(add, extended, ready=b"T", tag="ALTER TABLE")
            query("COMMIT;", extended, tag="COMMIT")
            query(f"SELECT id,value FROM {table} ORDER BY id;", extended,
                  rows=[["1", "1"], ["2", None], ["3", "1"]], tag="SELECT 3")
            query(f"INSERT INTO {table} VALUES(4,NULL);", extended, tag="INSERT 0 1")
            query(f"INSERT INTO {table} VALUES(5,-1);", extended, "23514")
            query(f"UPDATE {table} SET value=-1 WHERE id=1;", extended, "23514")
            query(f"SELECT id,value FROM {table} ORDER BY id;", extended,
                  rows=[["1", "1"], ["2", None], ["3", "1"], ["4", None]], tag="SELECT 4")
            # Earlier actions in a multiple-action ALTER roll back with validation.
            query(f"ALTER TABLE {table} ADD COLUMN unpublished INT, "
                  f"ADD CONSTRAINT {table}_upper CHECK(value < 0);", extended, "23514")
            query(f"SELECT unpublished FROM {table};", extended, "42703")
            query(f"SELECT id,value FROM {table} ORDER BY id;", extended,
                  rows=[["1", "1"], ["2", None], ["3", "1"], ["4", None]], tag="SELECT 4")
            query("BEGIN;", extended, ready=b"T")
            query("SAVEPOINT rewrite_sp;", extended, ready=b"T")
            query(f"ALTER TABLE {table} ADD COLUMN hidden INT, "
                  f"ADD CONSTRAINT {table}_negative CHECK(value < 0);", extended, "23514", b"E")
            query("ROLLBACK TO rewrite_sp;", extended, ready=b"T")
            query(f"SELECT hidden FROM {table};", extended, "42703", b"E")
            query("ROLLBACK TO rewrite_sp;", extended, ready=b"T")
            query("COMMIT;", extended, tag="COMMIT")
            # Two successful rewriting actions must keep all existing rows.
            query(f"ALTER TABLE {table} ADD COLUMN left_value INT, ADD COLUMN right_value INT;",
                  extended, tag="ALTER TABLE")
            query(f"SELECT id,value,left_value,right_value FROM {table} ORDER BY id;", extended,
                  rows=[["1", "1", None, None], ["2", None, None, None],
                        ["3", "1", None, None], ["4", None, None, None]], tag="SELECT 4")
            query(f"DROP TABLE {table};", extended, tag="DROP TABLE")
        print("[CHECK ADD VALIDATION " + ("PG18 ORACLE" if reference else "PROTOCOL E2E") + "] passed")
    finally:
        if reference:
            sock.close()
        else:
            runner.stop_ours(server)


if __name__ == "__main__":
    main()
