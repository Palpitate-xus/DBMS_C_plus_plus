#!/usr/bin/env python3
"""Quoted UPDATE/DELETE predicate identifiers survive the engine bridge."""

import importlib.util
import struct
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "quoted_predicate_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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

    def query(sql, expected=None, tag=None, ready=b"I", transport=None):
        messages = transport(sql) if transport else client.simple_query(server["sock"], sql)
        rows, state, message, _, command_tag = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if expected is not None:
            assert rows == expected, (sql, rows, expected)
        if tag is not None:
            assert command_tag == tag, (sql, command_tag, tag)

    def error(sql, expected, ready=b"E"):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert rows == [] and state == expected, (sql, rows, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])

    try:
        query('CREATE TABLE quoted_predicate("child id" INT PRIMARY KEY,payload TEXT,"nullable text" TEXT,"a""b" TEXT,"key.name" INT);')
        query("INSERT INTO quoted_predicate VALUES(1,'original',NULL,'has space',7),(2,'other','','plain',8);")
        query('UPDATE quoted_predicate SET payload=\'changed\' WHERE "child id"=1 RETURNING "child id",payload;', [["1", "changed"]], "UPDATE 1")
        query('UPDATE quoted_predicate SET payload=\'empty\' WHERE "nullable text"=\'\' RETURNING "child id";', [["2"]], "UPDATE 1")
        query('UPDATE quoted_predicate SET payload=\'null\' WHERE "nullable text" IS NULL RETURNING "child id";', [["1"]], "UPDATE 1")
        query('UPDATE quoted_predicate SET payload=\'like\' WHERE "a""b" LIKE \'has space%\' RETURNING "child id";', [["1"]], "UPDATE 1")
        query('DELETE FROM quoted_predicate WHERE "key.name"=7 AND "child id"<>2 RETURNING "child id",payload;', [["1", "like"]], "DELETE 1", transport=extended)
        query('DELETE FROM quoted_predicate WHERE "nullable text" IS NULL;', tag="DELETE 0")
        query('DELETE FROM quoted_predicate WHERE "nullable text" IS NOT NULL RETURNING "child id";', [["2"]], "DELETE 1")
        query('SELECT "child id" FROM quoted_predicate;', [])
        query('CREATE TABLE quoted_parent("key id" INT PRIMARY KEY);')
        query('INSERT INTO quoted_parent VALUES(1);')
        query('CREATE TABLE quoted_child("child id" INT PRIMARY KEY,"parent id" INT REFERENCES quoted_parent("key id"));')
        query('INSERT INTO quoted_child VALUES(1,1);')
        query("BEGIN;", ready=b"T")
        query("SAVEPOINT quoted_sp;", ready=b"T")
        error('UPDATE quoted_child SET "parent id"=999 WHERE "child id"=1 RETURNING "child id";', "23503")
        error('SELECT "child id" FROM quoted_child;', "25P02")
        query("ROLLBACK TO SAVEPOINT quoted_sp;", ready=b"T")
        query('SELECT "child id","parent id" FROM quoted_child;', [["1", "1"]], ready=b"T")
        query("COMMIT;")
        query('DELETE FROM quoted_child WHERE "child id">=1 AND "child id"<=1 RETURNING "child id";', [["1"]], "DELETE 1")
        print("[QUOTED COLUMN PREDICATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
