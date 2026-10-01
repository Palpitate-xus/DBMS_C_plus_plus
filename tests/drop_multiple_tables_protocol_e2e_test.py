#!/usr/bin/env python3
"""A DROP target list is atomic and retains temporary-name state on error."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "drop_multiple_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, tag = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows, tag

    def error(sql, expected):
        _, state, message, _, _ = runner.decode_wire_result(client.simple_query(server["sock"], sql))
        assert state == expected, (sql, state, message)

    try:
        query("CREATE TABLE drop_many_a(id INT);")
        query("CREATE TABLE drop_many_b(id INT);")
        query("INSERT INTO drop_many_a VALUES(1);")
        query("INSERT INTO drop_many_b VALUES(2);")
        error("DROP TABLE drop_many_a,drop_many_missing;", "42P01")
        assert query("SELECT id FROM drop_many_a;")[0] == [["1"]]
        assert query("SELECT id FROM drop_many_b;")[0] == [["2"]]
        query("CREATE VIEW drop_many_view AS SELECT 42 AS id;")
        error("DROP TABLE drop_many_a,drop_many_view;", "42809")
        assert query("SELECT id FROM drop_many_a;")[0] == [["1"]]
        assert query("SELECT id FROM drop_many_view;")[0] == [["42"]]
        query("BEGIN;")
        query("SAVEPOINT drop_many_sp;")
        error("DROP TABLE drop_many_a,drop_many_missing;", "42P01")
        query("ROLLBACK TO SAVEPOINT drop_many_sp;")
        assert query("SELECT id FROM drop_many_a;")[0] == [["1"]]
        assert query("DROP TABLE drop_many_a,drop_many_b;")[1] == "DROP TABLE"
        query("ROLLBACK;")
        assert query("SELECT id FROM drop_many_a;")[0] == [["1"]]
        assert query("SELECT id FROM drop_many_b;")[0] == [["2"]]
        assert query("DROP TABLE drop_many_a,drop_many_a,public.drop_many_b;")[1] == "DROP TABLE"
        query('CREATE TABLE "comma,a"(id INT);')
        query('CREATE TABLE "comma,b"(id INT);')
        query('DROP TABLE "comma,a","comma,b";')
        query("CREATE TABLE drop_many_shadow(id INT);")
        query("INSERT INTO drop_many_shadow VALUES(9);")
        query("CREATE TEMP TABLE drop_many_shadow(id INT);")
        query("CREATE TEMP TABLE drop_many_temp_b(id INT);")
        query("INSERT INTO drop_many_shadow VALUES(7);")
        error("DROP TABLE drop_many_shadow,drop_many_missing;", "42P01")
        assert query("SELECT id FROM drop_many_shadow;")[0] == [["7"]]
        assert query("SELECT id FROM public.drop_many_shadow;")[0] == [["9"]]
        query("DROP TABLE IF EXISTS drop_many_missing,drop_many_shadow,drop_many_temp_b;")
        assert query("SELECT id FROM drop_many_shadow;")[0] == [["9"]]
        query("CREATE SEQUENCE drop_many_owned_seq;")
        query("CREATE TABLE drop_many_owned(id INT DEFAULT nextval('drop_many_owned_seq') PRIMARY KEY,code TEXT);")
        query("ALTER SEQUENCE drop_many_owned_seq OWNED BY drop_many_owned.id;")
        query("INSERT INTO drop_many_owned(code) VALUES('kept');")
        assert query("SELECT currval('drop_many_owned_seq');")[0] == [["1"]]
        error("DROP TABLE drop_many_owned,drop_many_missing;", "42P01")
        assert query("SELECT id,code FROM drop_many_owned;")[0] == [["1", "kept"]]
        assert query("SELECT currval('drop_many_owned_seq');")[0] == [["1"]]
        assert query("INSERT INTO drop_many_owned(code) VALUES('next') RETURNING id;")[0] == [["2"]]
        print("[DROP MULTIPLE TABLES PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
