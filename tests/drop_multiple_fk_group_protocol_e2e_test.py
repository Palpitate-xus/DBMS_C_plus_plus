#!/usr/bin/env python3
"""Internal foreign keys do not block atomic DROP target groups."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "drop_fk_group_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        _, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected, (sql, state, message)

    def create_pair():
        query("CREATE TABLE drop_group_parent(id INT PRIMARY KEY);")
        query("CREATE TABLE drop_group_child(id INT PRIMARY KEY,pid INT REFERENCES drop_group_parent(id));")
        query("INSERT INTO drop_group_parent VALUES(1);")
        query("INSERT INTO drop_group_child VALUES(2,1);")

    try:
        create_pair()
        query("BEGIN;")
        assert query("DROP TABLE public.drop_group_parent,drop_group_child;")[1] == "DROP TABLE"
        query("ROLLBACK;")
        assert query("SELECT id FROM drop_group_parent;")[0] == [["1"]]
        assert query("SELECT id,pid FROM drop_group_child;")[0] == [["2", "1"]]
        error("INSERT INTO drop_group_child VALUES(3,99);", "23503")
        assert query("DROP TABLE drop_group_parent,drop_group_child;")[1] == "DROP TABLE"
        create_pair()
        query("CREATE TABLE drop_group_survivor(id INT PRIMARY KEY,pid INT REFERENCES drop_group_parent(id));")
        query("INSERT INTO drop_group_survivor VALUES(3,1);")
        error("DROP TABLE drop_group_child,drop_group_parent;", "2BP01")
        assert query("SELECT id FROM drop_group_parent;")[0] == [["1"]]
        assert query("SELECT id,pid FROM drop_group_child;")[0] == [["2", "1"]]
        assert query("SELECT id,pid FROM drop_group_survivor;")[0] == [["3", "1"]]
        error("INSERT INTO drop_group_child VALUES(4,99);", "23503")
        error("INSERT INTO drop_group_survivor VALUES(4,99);", "23503")
        query("DROP TABLE drop_group_parent,drop_group_child CASCADE;")
        assert query("SELECT id,pid FROM drop_group_survivor;")[0] == [["3", "1"]]
        query("INSERT INTO drop_group_survivor VALUES(4,99);")
        query("DROP TABLE drop_group_survivor;")
        query("CREATE TABLE drop_cycle_a(id INT PRIMARY KEY,bid INT);")
        query("CREATE TABLE drop_cycle_b(id INT PRIMARY KEY,aid INT REFERENCES drop_cycle_a(id));")
        query("ALTER TABLE drop_cycle_a ADD CONSTRAINT drop_cycle_a_fkey FOREIGN KEY(bid) REFERENCES drop_cycle_b(id);")
        assert query("DROP TABLE drop_cycle_a,public.drop_cycle_b;")[1] == "DROP TABLE"
        error("SELECT id FROM drop_cycle_a;", "42P01")
        error("SELECT id FROM drop_cycle_b;", "42P01")
        print("[DROP MULTIPLE FK GROUP PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
