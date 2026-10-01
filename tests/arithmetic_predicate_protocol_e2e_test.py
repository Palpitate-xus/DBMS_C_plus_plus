#!/usr/bin/env python3
"""Computed predicates retain decimal values, grouping, NULL and lazy branches."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "arithmetic_predicate_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE arithmetic_predicates(id INT PRIMARY KEY,n NUMERIC);")
        query("INSERT INTO arithmetic_predicates VALUES(1,1),(2,2),(3,NULL);")
        assert query("SELECT id FROM arithmetic_predicates WHERE n/0.5>1 ORDER BY id;") == [["1"], ["2"]]
        for predicate in ["n/0.5>3", "n/00.5>3", "n/(0.25+0.25)>3", "n/0.5>3 AND id=2"]:
            rows = query("SELECT id FROM arithmetic_predicates WHERE " + predicate + " ORDER BY id;")
            assert rows == [["2"]], (predicate, rows)
        assert query("SELECT id,n FROM arithmetic_predicates WHERE coalesce(1,1/0)=1 ORDER BY id;") == [
            ["1", "1"], ["2", "2"], ["3", None]]
        assert query("SELECT count(*) FILTER(WHERE n/0.5>1) FROM arithmetic_predicates;") == [["2"]]
        _, state, _, _, _ = runner.decode_wire_result(client.simple_query(
            server["sock"], "SELECT id FROM arithmetic_predicates WHERE n/0>1;"))
        assert state == "22012", state
        query("CREATE TABLE arithmetic_predicates_empty(id INT,n NUMERIC);")
        assert query("SELECT id FROM arithmetic_predicates_empty WHERE n/0>1;") == []
        assert query("SELECT id FROM arithmetic_predicates_empty WHERE coalesce(1,1/0)=1;") == []
        for predicate, expected in [("1/0>1", "22012"), ("1/0", "42804")]:
            _, state, _, _, _ = runner.decode_wire_result(client.simple_query(
                server["sock"], "SELECT id FROM arithmetic_predicates_empty WHERE " + predicate + ";"))
            assert state == expected, (predicate, state)
        assert query("SELECT 42;") == [["42"]]
        print("[ARITHMETIC PREDICATE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
