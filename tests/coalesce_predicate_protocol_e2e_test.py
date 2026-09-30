#!/usr/bin/env python3
"""COALESCE is non-strict and returns real empty/NULL-looking text values."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "coalesce_predicate_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE coalesce_predicate(f INT,t TEXT);")
        query("INSERT INTO coalesce_predicate VALUES (0,''),(1,'NULL'),(NULL,NULL);")
        assert query("SELECT count(*) FILTER (WHERE coalesce(f,0)=0) FROM coalesce_predicate;") == [["2"]]
        assert query("SELECT f,t FROM coalesce_predicate WHERE coalesce(t,'fallback')='' ORDER BY 1;") == [["0", ""]]
        assert query("SELECT f,t FROM coalesce_predicate WHERE coalesce(t,'fallback')='NULL' ORDER BY 1;") == [["1", "NULL"]]
        assert query("SELECT f,t FROM coalesce_predicate WHERE coalesce(t,'fallback')='fallback' ORDER BY 1;") == [[None, None]]
        assert query("SELECT count(*) FILTER (WHERE abs(f)=0) FROM coalesce_predicate;") == [["1"]]
        assert query("SELECT 42;") == [["42"]]
        print("[COALESCE PREDICATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
