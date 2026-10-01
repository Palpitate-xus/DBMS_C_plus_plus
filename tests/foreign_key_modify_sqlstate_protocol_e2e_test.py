#!/usr/bin/env python3
"""Immediate UPDATE and DELETE FK failures retain precise 23503 metadata."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "fk_modify_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None):
        rows, state, message, _, _ = runner.decode_wire_result(client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    def error(sql, expected="23503"):
        rows, state, message, _, _ = runner.decode_wire_result(client.simple_query(server["sock"], sql))
        assert rows == [] and state == expected, (sql, rows, state, message)

    try:
        query("CREATE TABLE modify_parent(id INT PRIMARY KEY,code TEXT UNIQUE);")
        query("CREATE TABLE modify_child(id INT PRIMARY KEY,pid INT REFERENCES modify_parent(id),code TEXT REFERENCES modify_parent(code));")
        query("INSERT INTO modify_parent VALUES(1,'kept'),(2,'other');")
        query("INSERT INTO modify_child VALUES(10,1,'kept'),(20,2,'other');")
        for sql in (
                "UPDATE modify_child SET pid=77 WHERE id=10 RETURNING id;",
                "UPDATE modify_child SET code='absent' WHERE id=10 RETURNING id;",
                "UPDATE modify_parent SET id=3 WHERE id=1 RETURNING id;",
                "UPDATE modify_parent SET code='new' WHERE id=1 RETURNING id;",
                "DELETE FROM modify_parent WHERE id=1 RETURNING id;"):
            error(sql)
        query("SELECT id,pid,code FROM modify_child ORDER BY id;", [["10", "1", "kept"], ["20", "2", "other"]])
        query("SELECT id,code FROM modify_parent ORDER BY id;", [["1", "kept"], ["2", "other"]])
        query("BEGIN;")
        query("SAVEPOINT modify_sp;")
        error("UPDATE modify_child SET pid=78 WHERE id=20;")
        error("SELECT id FROM modify_child;", "25P02")
        query("ROLLBACK TO SAVEPOINT modify_sp;")
        query("SELECT id,pid FROM modify_child ORDER BY id;", [["10", "1"], ["20", "2"]])
        query("COMMIT;")
        error("UPDATE modify_child SET pid=CASE WHEN id=10 THEN 2 ELSE 79 END RETURNING id;")
        query("SELECT id,pid FROM modify_child ORDER BY id;", [["10", "1"], ["20", "2"]])
        query("CREATE TABLE modify_self(id INT PRIMARY KEY,pid INT REFERENCES modify_self(id));")
        query("INSERT INTO modify_self(id) VALUES(1);")
        query("UPDATE modify_self SET pid=1 WHERE id=1;")
        error("UPDATE modify_self SET id=2 WHERE id=1;")
        query("SELECT id,pid FROM modify_self;", [["1", "1"]])
        query("DELETE FROM modify_self WHERE id=1;")
        print("[FOREIGN KEY MODIFY SQLSTATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
