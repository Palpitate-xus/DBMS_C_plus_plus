#!/usr/bin/env python3
"""INSERT foreign-key failures retain SQLSTATE and statement atomicity."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "fk_insert_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def error(sql, expected="23503"):
        _, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected, (sql, state, message)

    try:
        query("CREATE TABLE fk_ins_parent(id INT PRIMARY KEY,code INT UNIQUE,g INT,UNIQUE(code,g));")
        query("INSERT INTO fk_ins_parent VALUES(7,9,11);")
        query("CREATE TABLE fk_ins_pk(id INT PRIMARY KEY,pid INT REFERENCES fk_ins_parent(id));")
        query("CREATE TABLE fk_ins_unique(id INT PRIMARY KEY,code INT REFERENCES fk_ins_parent(code));")
        query("CREATE TABLE fk_ins_pair(id INT PRIMARY KEY,code INT,g INT,FOREIGN KEY(code,g) REFERENCES fk_ins_parent(code,g));")
        error("INSERT INTO fk_ins_pk VALUES(1,99) RETURNING id;")
        error("INSERT INTO fk_ins_unique VALUES(1,7);")
        error("INSERT INTO fk_ins_pair VALUES(1,9,12);")
        for table in ("fk_ins_pk", "fk_ins_unique", "fk_ins_pair"):
            assert query(f"SELECT id FROM {table};") == []
        query("INSERT INTO fk_ins_pk VALUES(1,7);")
        query("INSERT INTO fk_ins_unique VALUES(1,9);")
        query("INSERT INTO fk_ins_pair VALUES(1,9,11);")
        error("INSERT INTO fk_ins_pk VALUES(2,7),(3,99);")
        assert query("SELECT id,pid FROM fk_ins_pk ORDER BY id;") == [["1", "7"]]
        error("INSERT INTO fk_ins_pk SELECT 4,99 FROM fk_ins_parent;")
        assert query("SELECT id,pid FROM fk_ins_pk ORDER BY id;") == [["1", "7"]]
        query("BEGIN;")
        query("SAVEPOINT fk_ins_sp;")
        error("INSERT INTO fk_ins_unique VALUES(2,7);")
        error("SELECT id FROM fk_ins_unique;", "25P02")
        query("ROLLBACK TO SAVEPOINT fk_ins_sp;")
        assert query("SELECT id,code FROM fk_ins_unique;") == [["1", "9"]]
        query("COMMIT;")
        query("INSERT INTO fk_ins_pair VALUES(2,NULL,99);")
        assert query("SELECT id,code,g FROM fk_ins_pair ORDER BY id;") == [["1", "9", "11"], ["2", None, "99"]]
        print("[FOREIGN KEY INSERT SQLSTATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
