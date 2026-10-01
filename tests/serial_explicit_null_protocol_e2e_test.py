#!/usr/bin/env python3
"""SERIAL's implicit NOT NULL conflicts with an explicit NULL declaration."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "serial_null_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, rows=None, state=None, ready=b"I"):
        messages = client.simple_query(server["sock"], sql)
        result = runner.decode_wire_result(messages)
        assert result[1] == state, (sql, result)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        if rows is not None:
            assert result[0] == rows, (sql, result[0], rows)

    try:
        for alias in ("SMALLSERIAL", "SERIAL", "BIGSERIAL", "SERIAL2", "SERIAL4", "SERIAL8"):
            query(f"CREATE TABLE serial_null_bad(id {alias} NULL);", [], "42601")
            query("SELECT id FROM serial_null_bad;", [], "42P01")
            query("SELECT nextval('serial_null_bad_id_seq');", [], "42P01")
        for declaration in ("SERIAL NULL NOT NULL", "SERIAL NOT NULL NULL",
                            "SERIAL PRIMARY KEY NULL", "SERIAL NULL PRIMARY KEY"):
            query(f"CREATE TABLE serial_null_bad(id {declaration});", [], "42601")
        query("CREATE TABLE serial_null_alter(base INT);")
        query("BEGIN;", ready=b"T")
        query("INSERT INTO serial_null_alter VALUES(7);", ready=b"T")
        query("SAVEPOINT kept;", ready=b"T")
        query("ALTER TABLE serial_null_alter ADD COLUMN id SERIAL NULL;", [], "42601", b"E")
        query("SELECT 1;", [], "25P02", b"E")
        query("ROLLBACK TO SAVEPOINT kept;", ready=b"T")
        query("SELECT base FROM serial_null_alter;", [["7"]], ready=b"T")
        query("COMMIT;")
        query("SELECT nextval('serial_null_alter_id_seq');", [], "42P01")
        query("CREATE TABLE serial_null_ok(id SERIAL NOT NULL);")
        query("INSERT INTO serial_null_ok DEFAULT VALUES RETURNING id;", [["1"]])
        query("INSERT INTO serial_null_ok VALUES(NULL);", [], "23502")
        query("CREATE TABLE ordinary_nullable(id INT NULL DEFAULT NULL);")
        query("INSERT INTO ordinary_nullable DEFAULT VALUES RETURNING id;", [[None]])
        print("[SERIAL EXPLICIT NULL PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
