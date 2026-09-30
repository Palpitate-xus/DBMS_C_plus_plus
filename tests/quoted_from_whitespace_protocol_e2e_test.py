#!/usr/bin/env python3
"""Whitespace inside a delimited relation name is not an alias separator."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "quoted_whitespace_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    try:
        query('CREATE TABLE "From Rows" (id INT PRIMARY KEY,v TEXT);')
        query('INSERT INTO "From Rows" VALUES (1,\'NULL\'),(2,\'\'),(3,NULL);')
        for _ in range(2):
            assert query('SELECT id,v FROM "From Rows" ORDER BY id;') == [
                ["1", "NULL"], ["2", ""], ["3", None]]
            assert query('SELECT q.id FROM "From Rows" AS q WHERE q.id=2;') == [["2"]]
            assert query('SELECT q.id FROM "From Rows" q WHERE q.id=1;') == [["1"]]
            assert query('SELECT id FROM public."From Rows" ORDER BY id;') == [
                ["1"], ["2"], ["3"]]
        query('CREATE TABLE "From  ""Quoted, Rows" (id INT);')
        query('INSERT INTO "From  ""Quoted, Rows" VALUES (4);')
        assert query('SELECT id FROM "From  ""Quoted, Rows";') == [["4"]]
        assert query('SELECT q.id FROM "From  ""Quoted, Rows" AS q WHERE q.id=4;') == [["4"]]
        query("CREATE TABLE from_plain_alias (id INT);")
        query("INSERT INTO from_plain_alias VALUES (5);")
        assert query("SELECT q.id FROM from_plain_alias AS q WHERE q.id=5;") == [["5"]]
        assert query("SELECT 42;") == [["42"]]
        print("[QUOTED FROM WHITESPACE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
