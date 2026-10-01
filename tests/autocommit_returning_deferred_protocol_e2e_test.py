#!/usr/bin/env python3
"""Rows precede a deferred autocommit error without claiming durable success."""

import importlib.util
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "autocommit_returning_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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

    def query(sql, expected=None, transport=None):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    def error(sql, expected, expected_rows, transport=None, completion=None):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        rows, state, message, _, tag = runner.decode_wire_result(messages)
        assert state == expected and rows == expected_rows, (sql, state, rows, message)
        assert tag == completion, (sql, tag, completion)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        kinds = [kind for kind, _ in messages]
        error_at = kinds.index(b"E")
        assert all(index < error_at for index, kind in enumerate(kinds) if kind == b"D"), (sql, kinds)
        if transport is None:
            assert b"C" not in kinds, (sql, kinds)
            assert b"T" in kinds, (sql, kinds)

    try:
        query("CREATE TABLE returning_parent(id INT PRIMARY KEY);")
        query("INSERT INTO returning_parent VALUES(1);")
        query("CREATE TABLE returning_child(id INT PRIMARY KEY,pid INT,content TEXT,CONSTRAINT returning_fk FOREIGN KEY(pid) REFERENCES returning_parent(id) DEFERRABLE INITIALLY DEFERRED);")
        for transport in (None, extended):
            completion = "INSERT 0 2" if transport else None
            error("INSERT INTO returning_child VALUES(1,999,'NULL'),(2,1,'') RETURNING id,content;", "23503", [["1", "NULL"], ["2", ""]], transport, completion)
            query("SELECT id FROM returning_child;", [], transport)
            query("INSERT INTO returning_child VALUES(10,1,NULL);", transport=transport)
            completion = "UPDATE 1" if transport else None
            error("UPDATE returning_child SET pid=999 RETURNING id,pid,content;", "23503", [["10", "999", None]], transport, completion)
            query("SELECT id,pid,content FROM returning_child;", [["10", "1", None]], transport)
            query("DELETE FROM returning_child;", transport=transport)
        query("CREATE TABLE returning_unique(id INT PRIMARY KEY,code INT,CONSTRAINT returning_unique_key UNIQUE(code) DEFERRABLE INITIALLY DEFERRED);")
        query("INSERT INTO returning_unique VALUES(1,7);")
        error("INSERT INTO returning_unique VALUES(2,7) RETURNING id;", "23505", [["2"]])
        error("INSERT INTO returning_unique VALUES(2,8),(3,8) RETURNING id;", "23505", [["2"], ["3"]], extended, "INSERT 0 2")
        query("SELECT id,code FROM returning_unique;", [["1", "7"]])
        # An executor error is a different phase: it must not reuse a
        # previous command's buffered RETURNING rows.
        messages = client.simple_query(server["sock"], "INSERT INTO returning_unique VALUES(1,9) RETURNING id;")
        rows, state, message, _, tag = runner.decode_wire_result(messages)
        assert state == "23505" and rows == [] and tag is None, (rows, state, message, tag)
        assert messages[-1] == (b"Z", b"I")
        query("SELECT id,code FROM returning_unique;", [["1", "7"]])
        print("[AUTOCOMMIT RETURNING DEFERRED PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
