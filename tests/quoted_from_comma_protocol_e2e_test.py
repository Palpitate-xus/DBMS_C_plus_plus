#!/usr/bin/env python3
"""FROM comma rewriting must preserve delimited relation identifiers."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "quoted_from_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query('CREATE TABLE "From,Rows" (id INT PRIMARY KEY,v TEXT);')
        query('INSERT INTO "From,Rows" VALUES (1,\'NULL\'),(2,\'\');')
        for _ in range(2):
            assert query('SELECT id,v FROM "From,Rows" ORDER BY id;') == [["1", "NULL"], ["2", ""]]
            assert query('SELECT q.id FROM "From,Rows" AS q WHERE q.id=2;') == [["2"]]
            assert query('SELECT id FROM public."From,Rows" ORDER BY id;') == [["1"], ["2"]]
        query('CREATE TABLE "From""Quoted,Rows" (id INT);')
        query('INSERT INTO "From""Quoted,Rows" VALUES (3);')
        assert query('SELECT id FROM "From""Quoted,Rows";') == [["3"]]
        query("CREATE TABLE from_comma_left (id INT);")
        query("CREATE TABLE from_comma_right (id INT);")
        query("INSERT INTO from_comma_left VALUES (1),(2);")
        query("INSERT INTO from_comma_right VALUES (3),(4);")
        assert query("SELECT a.id,b.id FROM from_comma_left a,from_comma_right b "
                     "ORDER BY a.id,b.id;") == [["1", "3"], ["1", "4"], ["2", "3"], ["2", "4"]]
        assert query('SELECT id FROM "From,Rows" WHERE id IN (1,2) ORDER BY id;') == [["1"], ["2"]]
        assert query("SELECT 42;") == [["42"]]
        print("[QUOTED FROM COMMA PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
