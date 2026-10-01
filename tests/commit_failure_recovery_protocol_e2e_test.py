#!/usr/bin/env python3
"""A COMMIT that aborts the engine returns the connection to idle."""

import importlib.util
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "commit_failure_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def extended(sql):
        parse = b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)
        bind = b"\0\0" + struct.pack("!HHH", 0, 0, 0)
        execute = b"\0" + struct.pack("!I", 0)
        server["sock"].sendall(client.typed(b"P", parse) +
                               client.typed(b"B", bind) +
                               client.typed(b"E", execute) + client.typed(b"S"))
        return client.read_until_ready(server["sock"])

    def query(sql, expected=None, ready=b"I", transport=None):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1], ready)
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    def error(sql, expected, ready, transport=None):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert rows == [] and state == expected, (sql, rows, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1], ready)
        assert not any(kind == b"C" for kind, _ in messages), (sql, messages)

    try:
        query("CREATE TABLE commit_parent(id INT PRIMARY KEY);")
        query("CREATE TABLE commit_child(id INT PRIMARY KEY,pid INT,CONSTRAINT commit_fk FOREIGN KEY(pid) REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED);")
        query("INSERT INTO commit_parent VALUES(1);")
        query("INSERT INTO commit_child VALUES(1,1);")
        for ending in ("COMMIT;", "END;", "COMMIT AND CHAIN;",
                       "PREPARE TRANSACTION 'commit_failure_prepared';"):
            query("BEGIN;", ready=b"T")
            query("INSERT INTO commit_parent VALUES(2);", ready=b"T")
            query("INSERT INTO commit_child VALUES(2,999);", ready=b"T")
            error(ending, "23503", b"I")
            query("SELECT id FROM commit_parent ORDER BY id;", [["1"]])
            query("SELECT id,pid FROM commit_child ORDER BY id;", [["1", "1"]])
        query("CREATE TABLE commit_unique(id INT PRIMARY KEY,code INT,CONSTRAINT commit_unique_key UNIQUE(code) DEFERRABLE INITIALLY DEFERRED);")
        query("INSERT INTO commit_unique VALUES(1,5);")
        query("BEGIN;", ready=b"T")
        query("INSERT INTO commit_unique VALUES(2,5);", ready=b"T")
        error("COMMIT;", "23505", b"I")
        query("SELECT id,code FROM commit_unique;", [["1", "5"]])

        # An in-transaction constraint error still leaves a real failed
        # transaction: only a transaction-ending failure returns idle.
        query("BEGIN;", ready=b"T")
        query("SAVEPOINT commit_sp;", ready=b"T")
        query("INSERT INTO commit_child VALUES(2,999);", ready=b"T")
        error("SET CONSTRAINTS commit_fk IMMEDIATE;", "23503", b"E")
        error("SELECT id FROM commit_child;", "25P02", b"E")
        query("ROLLBACK TO SAVEPOINT commit_sp;", ready=b"T")
        query("SELECT id,pid FROM commit_child;", [["1", "1"]], ready=b"T")
        query("COMMIT;")

        query("BEGIN;", ready=b"T", transport=extended)
        query("INSERT INTO commit_child VALUES(2,999);", ready=b"T", transport=extended)
        error("COMMIT;", "23503", b"I", transport=extended)
        query("SELECT id,pid FROM commit_child;", [["1", "1"]], transport=extended)
        query("BEGIN;", ready=b"T", transport=extended)
        query("INSERT INTO commit_parent VALUES(2);", ready=b"T", transport=extended)
        query("INSERT INTO commit_child VALUES(2,2);", ready=b"T", transport=extended)
        query("COMMIT;", transport=extended)
        query("SELECT id,pid FROM commit_child ORDER BY id;", [["1", "1"], ["2", "2"]])
        print("[COMMIT FAILURE RECOVERY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
