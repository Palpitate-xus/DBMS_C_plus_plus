#!/usr/bin/env python3
"""An INSERT can satisfy a self-reference with its own pending row."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "fk_self_insert_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE self_insert_nodes(id INT PRIMARY KEY,pid INT REFERENCES self_insert_nodes(id));")
        query("INSERT INTO self_insert_nodes VALUES(1,1) RETURNING id,pid;", [["1", "1"]], "INSERT 0 1")
        error("INSERT INTO self_insert_nodes VALUES(2,999) RETURNING id;")
        query("INSERT INTO self_insert_nodes VALUES(2,2);")
        query("INSERT INTO self_insert_nodes VALUES(3,3),(4,4) RETURNING id;", [["3"], ["4"]], "INSERT 0 2")
        error("INSERT INTO self_insert_nodes VALUES(5,5),(6,999) RETURNING id;")
        error("INSERT INTO self_insert_nodes VALUES(1,1);", "23505")
        query("SELECT id,pid FROM self_insert_nodes ORDER BY id;", [["1", "1"], ["2", "2"], ["3", "3"], ["4", "4"]])
        query("BEGIN;")
        query("SAVEPOINT self_insert_sp;")
        query("INSERT INTO self_insert_nodes VALUES(5,5);")
        error("INSERT INTO self_insert_nodes VALUES(6,999);")
        error("SELECT id FROM self_insert_nodes;", "25P02")
        query("ROLLBACK TO SAVEPOINT self_insert_sp;")
        query("SELECT id,pid FROM self_insert_nodes ORDER BY id;", [["1", "1"], ["2", "2"], ["3", "3"], ["4", "4"]])
        query("COMMIT;")
        query("CREATE TABLE self_insert_unique(id INT PRIMARY KEY,code TEXT UNIQUE,parent_code TEXT REFERENCES self_insert_unique(code));")
        error("INSERT INTO self_insert_unique VALUES(9,NULL,'');")
        query("INSERT INTO self_insert_unique VALUES(1,'own','own'),(2,'','');")
        query("INSERT INTO self_insert_unique VALUES(3,NULL,'');")
        error("INSERT INTO self_insert_unique VALUES(4,NULL,'missing');")
        query("INSERT INTO self_insert_unique VALUES(4,NULL,NULL);")
        query("SELECT id,code,parent_code FROM self_insert_unique ORDER BY id;", [["1", "own", "own"], ["2", "", ""], ["3", None, ""], ["4", None, None]])
        query("CREATE TABLE self_insert_pair(id INT PRIMARY KEY,a INT,b TEXT,pa INT,pb TEXT,UNIQUE(a,b),FOREIGN KEY(pa,pb) REFERENCES self_insert_pair(a,b));")
        query("INSERT INTO self_insert_pair VALUES(1,7,'',7,'');")
        query("INSERT INTO self_insert_pair VALUES(2,8,'other',7,'');")
        error("INSERT INTO self_insert_pair VALUES(3,9,'third',9,'missing');")
        query("INSERT INTO self_insert_pair VALUES(3,9,'third',NULL,'missing');")
        query("SELECT id,a,b,pa,pb FROM self_insert_pair ORDER BY id;", [["1", "7", "", "7", ""], ["2", "8", "other", "7", ""], ["3", "9", "third", None, "missing"]])
        query("CREATE TABLE self_insert_default(id INT PRIMARY KEY,code TEXT DEFAULT 'default' UNIQUE,parent_code TEXT DEFAULT 'default' REFERENCES self_insert_default(code));")
        query("INSERT INTO self_insert_default(id) VALUES(1) RETURNING code,parent_code;", [["default", "default"]])
        query('CREATE SCHEMA "self insert schema";')
        query('CREATE TABLE "self insert schema"."self.node"("key id" INT PRIMARY KEY,"parent id" INT REFERENCES "self insert schema"."self.node"("key id"));')
        query('INSERT INTO "self insert schema"."self.node" VALUES(1,1) RETURNING "key id","parent id";', [["1", "1"]], "INSERT 0 1")
        query('SELECT "key id","parent id" FROM "self insert schema"."self.node";', [["1", "1"]])
        print("[FOREIGN KEY SELF INSERT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
