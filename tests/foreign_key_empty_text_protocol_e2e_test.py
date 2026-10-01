#!/usr/bin/env python3
"""FK checks distinguish empty referenced keys from real SQL NULL."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "fk_empty_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None, tag=None):
        rows, state, message, _, command_tag = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        if expected is not None:
            assert rows == expected, (sql, rows, expected)
        if tag is not None:
            assert command_tag == tag, (sql, command_tag, tag)

    def error(sql, expected="23503"):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert rows == [] and state == expected, (sql, rows, state, message)

    try:
        query("CREATE TABLE empty_parent(id INT PRIMARY KEY,code TEXT UNIQUE);")
        query("CREATE TABLE empty_child(id INT PRIMARY KEY,code TEXT REFERENCES empty_parent(code));")
        query("INSERT INTO empty_parent VALUES(1,'kept'),(2,NULL);")
        query("INSERT INTO empty_child VALUES(10,'kept'),(20,NULL);")
        error("UPDATE empty_child SET code='' WHERE id=10 RETURNING id,code;")
        error("UPDATE empty_child SET code='' WHERE id=20 RETURNING id,code;")
        error("INSERT INTO empty_child VALUES(30,'') RETURNING id,code;")
        query("SELECT id,code FROM empty_child ORDER BY id;", [["10", "kept"], ["20", None]], "SELECT 2")
        query("BEGIN;")
        query("SAVEPOINT empty_sp;")
        error("UPDATE empty_child SET code='' WHERE id=20;")
        error("SELECT id FROM empty_child;", "25P02")
        query("ROLLBACK TO SAVEPOINT empty_sp;")
        query("SELECT id,code FROM empty_child ORDER BY id;", [["10", "kept"], ["20", None]])
        query("COMMIT;")
        query("INSERT INTO empty_parent VALUES(3,'');")
        query("UPDATE empty_child SET code='' WHERE id=10 RETURNING code;", [[""]], "UPDATE 1")
        query("UPDATE empty_child SET code=NULL WHERE id=10 RETURNING code;", [[None]], "UPDATE 1")
        query("DELETE FROM empty_parent WHERE id=3;")

        query("CREATE TABLE empty_composite_parent(a TEXT,b INT,UNIQUE(a,b));")
        query("CREATE TABLE empty_composite_child(id INT PRIMARY KEY,a TEXT,b INT,FOREIGN KEY(a,b) REFERENCES empty_composite_parent(a,b));")
        query("INSERT INTO empty_composite_parent VALUES(NULL,7);")
        query("INSERT INTO empty_composite_child VALUES(1,NULL,7),(2,'',NULL);")
        error("INSERT INTO empty_composite_child VALUES(3,'',7);")
        error("UPDATE empty_composite_child SET a='' WHERE id=1;")
        error("UPDATE empty_composite_child SET b=7 WHERE id=2;")
        query("INSERT INTO empty_composite_parent VALUES('',7);")
        query("UPDATE empty_composite_child SET a='',b=7;")
        query("SELECT id,a,b FROM empty_composite_child ORDER BY id;", [["1", "", "7"], ["2", "", "7"]])

        query("CREATE TABLE empty_self(id INT PRIMARY KEY,code TEXT UNIQUE,parent_code TEXT REFERENCES empty_self(code));")
        query("INSERT INTO empty_self(id) VALUES(1);")
        error("UPDATE empty_self SET parent_code='' WHERE id=1;")
        query("UPDATE empty_self SET code='',parent_code='' WHERE id=1;")
        query("SELECT code,parent_code FROM empty_self;", [["", ""]])

        query("CREATE TABLE empty_deferred(id INT PRIMARY KEY,code TEXT,CONSTRAINT deferred_empty_fk FOREIGN KEY(code) REFERENCES empty_parent(code) DEFERRABLE INITIALLY DEFERRED);")
        query("INSERT INTO empty_deferred VALUES(10,'kept');")
        query("BEGIN;")
        query("UPDATE empty_deferred SET code='' WHERE id=10;")
        error("COMMIT;")
        query("SELECT id,code FROM empty_deferred;", [["10", "kept"]])
        query("BEGIN;")
        query("INSERT INTO empty_deferred VALUES(20,'');")
        error("COMMIT;")
        query("SELECT id,code FROM empty_deferred;", [["10", "kept"]])
        query("BEGIN;")
        query("SAVEPOINT deferred_empty_sp;")
        query("UPDATE empty_deferred SET code='' WHERE id=10;")
        error("SET CONSTRAINTS deferred_empty_fk IMMEDIATE;")
        query("ROLLBACK TO SAVEPOINT deferred_empty_sp;")
        query("SELECT id,code FROM empty_deferred;", [["10", "kept"]])
        query("COMMIT;")
        query("BEGIN;")
        query("UPDATE empty_deferred SET code='' WHERE id=10;")
        query("UPDATE empty_deferred SET code=NULL WHERE id=10;")
        query("COMMIT;")
        query("SELECT id,code FROM empty_deferred;", [["10", None]])
        query("BEGIN;")
        query("INSERT INTO empty_deferred VALUES(20,'');")
        query("INSERT INTO empty_parent VALUES(3,'');")
        query("COMMIT;")
        query("SELECT id,code FROM empty_deferred ORDER BY id;", [["10", None], ["20", ""]])
        query("BEGIN;")
        query("DELETE FROM empty_deferred WHERE id=20;")
        query("DELETE FROM empty_parent WHERE id=3;")
        query("INSERT INTO empty_deferred VALUES(30,'');")
        query("DELETE FROM empty_deferred WHERE id=30;")
        query("COMMIT;")
        query("SELECT id,code FROM empty_deferred;", [["10", None]])
        print("[FOREIGN KEY EMPTY TEXT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
