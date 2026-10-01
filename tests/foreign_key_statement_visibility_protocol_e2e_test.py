#!/usr/bin/env python3
"""Immediate INSERT self-FKs see the complete SQL statement's own writes."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "fk_statement_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None, ready=b"I"):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1], ready)
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    def error(sql, expected="23503", ready=b"I"):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert rows == [] and state == expected, (sql, rows, state, message)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1], ready)

    try:
        query("CREATE TABLE statement_nodes(id INT PRIMARY KEY,pid INT REFERENCES statement_nodes(id));")
        query("INSERT INTO statement_nodes VALUES(1,2),(2,NULL) RETURNING id,pid;", [["1", "2"], ["2", None]])
        query("INSERT INTO statement_nodes VALUES(3,4),(4,3);")
        error("INSERT INTO statement_nodes VALUES(5,6),(6,999) RETURNING id;")
        query("SELECT id,pid FROM statement_nodes ORDER BY id;", [["1", "2"], ["2", None], ["3", "4"], ["4", "3"]])
        query("BEGIN;", ready=b"T")
        query("SAVEPOINT statement_sp;", ready=b"T")
        error("INSERT INTO statement_nodes VALUES(5,6),(6,999);", ready=b"E")
        error("SELECT id FROM statement_nodes;", "25P02", b"E")
        query("ROLLBACK TO SAVEPOINT statement_sp;", ready=b"T")
        query("SELECT id,pid FROM statement_nodes ORDER BY id;", [["1", "2"], ["2", None], ["3", "4"], ["4", "3"]], b"T")
        query("COMMIT;")
        query("CREATE TABLE statement_source(id INT,pid INT);")
        query("INSERT INTO statement_source VALUES(10,11),(11,11);")
        query("INSERT INTO statement_nodes SELECT id,pid FROM statement_source;")
        query("SELECT id,pid FROM statement_nodes WHERE id>=10 ORDER BY id;", [["10", "11"], ["11", "11"]])
        query("CREATE TABLE statement_unique(id INT PRIMARY KEY,code TEXT UNIQUE,parent_code TEXT REFERENCES statement_unique(code));")
        query("INSERT INTO statement_unique VALUES(1,'first',NULL),(2,'second','first');")
        query("INSERT INTO statement_unique VALUES(3,'third','fourth'),(4,'fourth','third');")
        error("INSERT INTO statement_unique VALUES(5,'fifth','sixth'),(6,'sixth','missing');")
        query("SELECT id,code,parent_code FROM statement_unique ORDER BY id;", [["1", "first", None], ["2", "second", "first"], ["3", "third", "fourth"], ["4", "fourth", "third"]])
        query("CREATE TABLE statement_pair(id INT PRIMARY KEY,a INT,b TEXT,pa INT,pb TEXT,UNIQUE(a,b),FOREIGN KEY(pa,pb) REFERENCES statement_pair(a,b));")
        query("INSERT INTO statement_pair VALUES(1,7,'first',NULL,NULL),(2,8,'other',7,'first');")
        query("SELECT id,a,b,pa,pb FROM statement_pair ORDER BY id;", [["1", "7", "first", None, None], ["2", "8", "other", "7", "first"]])
        query("CREATE TABLE statement_char(id INT PRIMARY KEY,code CHAR(4) UNIQUE,parent_code CHAR(2) REFERENCES statement_char(code));")
        query("INSERT INTO statement_char VALUES(1,'aa',NULL),(2,'bb','aa');")
        query("CREATE TABLE statement_char_primary(code CHAR(4) PRIMARY KEY,parent_code CHAR(2) REFERENCES statement_char_primary(code));")
        query("INSERT INTO statement_char_primary VALUES('aa',NULL),('bb','aa');")
        query("CREATE TABLE statement_long_primary(code TEXT PRIMARY KEY,parent_code TEXT REFERENCES statement_long_primary(code));")
        error("INSERT INTO statement_long_primary VALUES('12345678901234567890first',NULL),('child','12345678901234567890other');")
        query("SELECT code FROM statement_long_primary;", [])
        query("CREATE TABLE statement_deferred(id INT PRIMARY KEY,pid INT,CONSTRAINT statement_fk FOREIGN KEY(pid) REFERENCES statement_deferred(id) DEFERRABLE INITIALLY DEFERRED);")
        query("BEGIN;", ready=b"T")
        query("INSERT INTO statement_deferred VALUES(1,999);", ready=b"T")
        error("COMMIT;")
        query("SELECT id FROM statement_deferred;", [])
        print("[FOREIGN KEY STATEMENT VISIBILITY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
