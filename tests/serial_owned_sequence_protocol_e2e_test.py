#!/usr/bin/env python3
"""Permanent SERIAL defaults use real owned sequences, not anonymous counters."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "serial_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def error(sql, expected):
        _, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected, (sql, state, message)

    try:
        query("CREATE TABLE serial_owned(id SERIAL PRIMARY KEY,code TEXT);")
        assert query("INSERT INTO serial_owned(code) VALUES('kept') RETURNING id;") == [["1"]]
        assert query("SELECT currval('serial_owned_id_seq');") == [["1"]]
        assert query("SELECT nextval('serial_owned_id_seq');") == [["2"]]
        assert query("INSERT INTO serial_owned(code) VALUES('next') RETURNING id;") == [["3"]]
        query("INSERT INTO serial_owned VALUES(99,'explicit');")
        assert query("INSERT INTO serial_owned(code) VALUES('after explicit') RETURNING id;") == [["4"]]
        error("INSERT INTO serial_owned VALUES(NULL,'bad');", "23502")
        query("DROP TABLE serial_owned;")
        error("SELECT nextval('serial_owned_id_seq');", "42P01")
        query("CREATE SEQUENCE serial_collision_id_seq;")
        query("CREATE TABLE serial_collision(id SERIAL);")
        assert query("INSERT INTO serial_collision DEFAULT VALUES RETURNING id;") == [["1"]]
        assert query("SELECT currval('serial_collision_id_seq1');") == [["1"]]
        assert query("SELECT nextval('serial_collision_id_seq');") == [["1"]]
        query("DROP TABLE serial_collision;")
        error("SELECT nextval('serial_collision_id_seq1');", "42P01")
        assert query("SELECT nextval('serial_collision_id_seq');") == [["2"]]
        query("CREATE TABLE serial_types(a SMALLSERIAL,b SERIAL,c BIGSERIAL,d SERIAL2,e SERIAL4,f SERIAL8);")
        assert query("INSERT INTO serial_types DEFAULT VALUES RETURNING a,b,c,d,e,f;") == [["1"] * 6]
        assert query("SELECT setval('serial_types_a_seq',32767);") == [["32767"]]
        error("SELECT nextval('serial_types_a_seq');", "2200H")
        query("DROP TABLE serial_types;")
        long_table, long_column = "表" * 12, "列" * 12
        long_sequence = "表" * 9 + "_" + "列" * 9 + "_seq"
        query(f'CREATE TABLE "{long_table}"("{long_column}" SERIAL);')
        assert query(f'INSERT INTO "{long_table}" DEFAULT VALUES RETURNING "{long_column}";') == [["1"]]
        assert query(f"SELECT currval('{long_sequence}');") == [["1"]]
        query(f'DROP TABLE "{long_table}";')
        error(f"SELECT nextval('{long_sequence}');", "42P01")
        error("CREATE TABLE serial_bad(id SERIAL DEFAULT 7);", "42601")
        error("SELECT id FROM serial_bad;", "42P01")
        error("SELECT nextval('serial_bad_id_seq');", "42P01")
        error("CREATE TABLE serial_array_bad(id SERIAL[]);", "0A000")
        query('CREATE SCHEMA "serial schema";')
        query('CREATE TABLE "serial schema"."odd.table"("it\'s" SERIAL);')
        assert query('INSERT INTO "serial schema"."odd.table" DEFAULT VALUES RETURNING "it\'s";') == [["1"]]
        assert query('SELECT currval(\'"serial schema"."odd.table_it\'\'s_seq"\');') == [["1"]]
        query('DROP TABLE "serial schema"."odd.table";')
        error('SELECT nextval(\'"serial schema"."odd.table_it\'\'s_seq"\');', "42P01")
        query("BEGIN;")
        query("CREATE TABLE serial_rollback(id SERIAL);")
        query("INSERT INTO serial_rollback DEFAULT VALUES;")
        query("ROLLBACK;")
        error("SELECT id FROM serial_rollback;", "42P01")
        error("SELECT nextval('serial_rollback_id_seq');", "42P01")
        query("CREATE TABLE serial_rollback(id SERIAL);")
        error("SELECT currval('serial_rollback_id_seq');", "55000")
        assert query("INSERT INTO serial_rollback DEFAULT VALUES RETURNING id;") == [["1"]]
        assert query("SELECT nextval('serial_rollback_id_seq');") == [["2"]]
        query("TRUNCATE serial_rollback RESTART IDENTITY;")
        assert query("SELECT currval('serial_rollback_id_seq');") == [["2"]]
        assert query("INSERT INTO serial_rollback DEFAULT VALUES RETURNING id;") == [["1"]]
        query("BEGIN;")
        query("TRUNCATE serial_rollback RESTART IDENTITY;")
        query("INSERT INTO serial_rollback DEFAULT VALUES;")
        query("ROLLBACK;")
        assert query("SELECT id FROM serial_rollback;") == [["1"]]
        assert query("SELECT nextval('serial_rollback_id_seq');") == [["2"]]
        print("[SERIAL OWNED SEQUENCE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
