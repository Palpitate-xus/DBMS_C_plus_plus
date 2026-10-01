#!/usr/bin/env python3
"""TRUNCATE resets real owned sequences, not unrelated nextval defaults."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "truncate_owned_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected=None):
        rows, state, message, _, tag = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        if expected is not None:
            assert rows == expected, (sql, rows, expected)
        if sql.startswith("TRUNCATE "):
            assert tag == "TRUNCATE TABLE", (sql, tag)
        return rows

    try:
        query("CREATE SEQUENCE restart_owned START 7 INCREMENT 3;")
        query("CREATE SEQUENCE restart_unowned START 31 INCREMENT 2;")
        query("CREATE TABLE restart_table(id INT DEFAULT nextval('restart_owned'),code TEXT);")
        query("ALTER SEQUENCE restart_owned OWNED BY restart_table.id;")
        query("CREATE TABLE restart_other(id INT DEFAULT nextval('restart_unowned'));")
        query("INSERT INTO restart_table(code) VALUES('before') RETURNING id;", [["7"]])
        query("INSERT INTO restart_other DEFAULT VALUES RETURNING id;", [["31"]])
        query("TRUNCATE restart_table CONTINUE IDENTITY;")
        query("INSERT INTO restart_table(code) VALUES('continue') RETURNING id;", [["10"]])
        query("TRUNCATE restart_table RESTART IDENTITY;")
        query("SELECT currval('restart_owned');", [["10"]])
        query("INSERT INTO restart_table(code) VALUES('restart') RETURNING id;", [["7"]])
        query("BEGIN;")
        query("SAVEPOINT restart_sp;")
        query("TRUNCATE restart_table RESTART IDENTITY;")
        query("INSERT INTO restart_table(code) VALUES('transaction') RETURNING id;", [["7"]])
        query("ROLLBACK TO SAVEPOINT restart_sp;")
        query("SELECT id,code FROM restart_table;", [["7", "restart"]])
        query("SELECT nextval('restart_owned');", [["10"]])
        query("COMMIT;")
        query('CREATE SCHEMA "restart space";')
        query('CREATE SEQUENCE "restart space"."seq.dot" START 19 INCREMENT 4;')
        query('CREATE TABLE "restart space"."table.dot"("id value" INT DEFAULT nextval(\'"restart space"."seq.dot"\'));')
        query('ALTER SEQUENCE "restart space"."seq.dot" OWNED BY "restart space"."table.dot"."id value";')
        query('INSERT INTO "restart space"."table.dot" DEFAULT VALUES RETURNING "id value";', [["19"]])
        query('TRUNCATE restart_table,restart_other,"restart space"."table.dot" RESTART IDENTITY;')
        query("INSERT INTO restart_table(code) VALUES('multi') RETURNING id;", [["7"]])
        query("INSERT INTO restart_other DEFAULT VALUES RETURNING id;", [["33"]])
        query('INSERT INTO "restart space"."table.dot" DEFAULT VALUES RETURNING "id value";', [["19"]])
        query("SELECT nextval('restart_unowned');", [["35"]])
        query("CREATE SEQUENCE restart_parent_seq START 11;")
        query("CREATE SEQUENCE restart_child_seq START 21;")
        query("CREATE TABLE restart_parent(id INT PRIMARY KEY DEFAULT nextval('restart_parent_seq'));")
        query("CREATE TABLE restart_child(id INT PRIMARY KEY DEFAULT nextval('restart_child_seq'),pid INT REFERENCES restart_parent(id));")
        query("ALTER SEQUENCE restart_parent_seq OWNED BY restart_parent.id;")
        query("ALTER SEQUENCE restart_child_seq OWNED BY restart_child.id;")
        query("INSERT INTO restart_parent DEFAULT VALUES RETURNING id;", [["11"]])
        query("INSERT INTO restart_child(pid) VALUES(11) RETURNING id;", [["21"]])
        query("TRUNCATE restart_parent RESTART IDENTITY CASCADE;")
        query("INSERT INTO restart_parent DEFAULT VALUES RETURNING id;", [["11"]])
        query("INSERT INTO restart_child(pid) VALUES(11) RETURNING id;", [["21"]])
        query("CREATE SEQUENCE restart_descending_seq START -7 INCREMENT -3;")
        query("CREATE TABLE restart_descending(id INT DEFAULT nextval('restart_descending_seq'));")
        query("ALTER SEQUENCE restart_descending_seq OWNED BY restart_descending.id;")
        query("INSERT INTO restart_descending DEFAULT VALUES RETURNING id;", [["-7"]])
        query("SELECT nextval('restart_descending_seq');", [["-10"]])
        query("TRUNCATE restart_descending RESTART IDENTITY;")
        query("INSERT INTO restart_descending DEFAULT VALUES RETURNING id;", [["-7"]])
        print("[TRUNCATE OWNED SEQUENCE RESTART PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
