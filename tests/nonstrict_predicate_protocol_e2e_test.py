#!/usr/bin/env python3
"""NULLIF/GREATEST/LEAST must not inherit strict scalar predicate gates."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "nonstrict_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE nonstrict_values(id INT,f INT,g INT,t TEXT,u TEXT);")
        query("INSERT INTO nonstrict_values VALUES (1,0,NULL,'',NULL),(2,NULL,2,NULL,'NULL'),(3,1,0,'x','');")
        assert query("SELECT count(*) FILTER (WHERE nullif(f,g)=0) FROM nonstrict_values;") == [["1"]]
        assert query("SELECT count(*) FILTER (WHERE greatest(f,g)=0) FROM nonstrict_values;") == [["1"]]
        assert query("SELECT count(*) FILTER (WHERE least(f,g)=0) FROM nonstrict_values;") == [["2"]]
        assert query("SELECT id,t,u FROM nonstrict_values WHERE nullif(t,u)='' ORDER BY id;") == [["1", "", None]]
        assert query("SELECT id,t,u FROM nonstrict_values WHERE greatest(t,u)='' ORDER BY id;") == [["1", "", None]]
        assert query("SELECT id,t,u FROM nonstrict_values WHERE least(t,u)='' ORDER BY id;") == [["1", "", None], ["3", "x", ""]]
        assert query("SELECT id,t,u FROM nonstrict_values WHERE greatest(t,u)='NULL' ORDER BY id;") == [["2", None, "NULL"]]
        assert query("SELECT 42;") == [["42"]]
        print("[NONSTRICT PREDICATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
