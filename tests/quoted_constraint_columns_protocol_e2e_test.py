#!/usr/bin/env python3
"""DDL keeps relation quote boundaries and decodes constraint column names."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "quoted_constraint_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        if expected is not None:
            assert rows == expected, (sql, rows, expected)

    def error(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert rows == [] and state == "23503", (sql, rows, state, message)

    try:
        query('CREATE SCHEMA "quoted constraint schema";')
        query('CREATE TABLE "quoted constraint schema"."self.node"("key id" INT PRIMARY KEY,"parent id" INT REFERENCES "quoted constraint schema"."self.node"("key id"));')
        query('INSERT INTO "quoted constraint schema"."self.node"("key id") VALUES(1);')
        query('UPDATE "quoted constraint schema"."self.node" SET "parent id"=1;')
        query('SELECT "key id","parent id" FROM "quoted constraint schema"."self.node";', [["1", "1"]])
        query('CREATE TABLE "quoted constraint schema"."p""arent"("parent id" INT,"a key" INT,"b""key" TEXT,PRIMARY KEY("parent id"),UNIQUE("a key","b""key"));')
        query('INSERT INTO "quoted constraint schema"."p""arent" VALUES(1,7,\'value\');')
        query('CREATE TABLE quoted_child("child id" INT PRIMARY KEY,"parent key" INT REFERENCES "quoted constraint schema"."p""arent"("parent id"),"a key" INT,"b""key" TEXT);')
        query('INSERT INTO quoted_child VALUES(1,1,7,\'value\');')
        error('INSERT INTO quoted_child VALUES(2,99,NULL,NULL) RETURNING "child id";')
        query('ALTER TABLE quoted_child ADD CONSTRAINT pair_fk FOREIGN KEY("a key","b""key") REFERENCES "quoted constraint schema"."p""arent"("a key","b""key");')
        error('INSERT INTO quoted_child VALUES(2,1,7,\'missing\');')
        error('UPDATE quoted_child SET "b""key"=\'missing\';')
        query('SELECT "child id","parent key","a key","b""key" FROM quoted_child;', [["1", "1", "7", "value"]])
        query('BEGIN;')
        query('SAVEPOINT quoted_sp;')
        query('ALTER TABLE quoted_child DROP CONSTRAINT pair_fk;')
        query('ROLLBACK TO SAVEPOINT quoted_sp;')
        query('COMMIT;')
        error('UPDATE quoted_child SET "b""key"=\'missing\';')
        query('CREATE TABLE folded_constraint(ID INT,VALUE TEXT,PRIMARY KEY(ID),UNIQUE(VALUE));')
        query('INSERT INTO folded_constraint VALUES(1,\'value\');')
        query('CREATE TABLE folded_reference(ID INT PRIMARY KEY,PID INT,FOREIGN KEY(PID) REFERENCES FOLDED_CONSTRAINT(ID));')
        query('INSERT INTO folded_reference VALUES(1,1);')
        query('SELECT id,pid FROM folded_reference;', [["1", "1"]])
        print("[QUOTED CONSTRAINT COLUMNS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
