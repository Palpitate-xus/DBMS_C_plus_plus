#!/usr/bin/env python3
"""Composite row-valued IN/NOT IN subqueries preserve SQL NULL semantics."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "composite_not_in_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, _ = runner.decode_wire_result(messages)
        assert state is None, (sql, state, message)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        assert rows == expected, (sql, rows, expected)

    try:
        query("CREATE TABLE composite_not_in_outer(a INT,b INT);", [])
        query("INSERT INTO composite_not_in_outer VALUES(1,2),(4,5),(3,NULL),(NULL,3);", [])
        query("CREATE TABLE composite_not_in_inner(a INT,b INT);", [])
        query("INSERT INTO composite_not_in_inner VALUES(3,NULL);", [])

        # A row inequality in any non-NULL field makes that comparison FALSE;
        # a NULL only makes the row comparison UNKNOWN when no field differs.
        query(
            "SELECT a,b FROM composite_not_in_outer "
            "WHERE (a,b) NOT IN (SELECT a,b FROM composite_not_in_inner) "
            "ORDER BY a;",
            [["1", "2"], ["4", "5"]])
        query(
            "SELECT a,b FROM composite_not_in_outer "
            "WHERE (a,b) IN (SELECT a,b FROM composite_not_in_inner) "
            "ORDER BY a;",
            [])

        query("INSERT INTO composite_not_in_inner VALUES(1,2);", [])
        query(
            "SELECT a,b FROM composite_not_in_outer "
            "WHERE (a,b) IN (SELECT a,b FROM composite_not_in_inner) "
            "ORDER BY a;",
            [["1", "2"]])
        query(
            "SELECT a,b FROM composite_not_in_outer "
            "WHERE (a,b) NOT IN (SELECT a,b FROM composite_not_in_inner) "
            "ORDER BY a;",
            [["4", "5"]])

        query("DELETE FROM composite_not_in_inner;", [])
        query(
            "SELECT a,b FROM composite_not_in_outer "
            "WHERE (a,b) IN (SELECT a,b FROM composite_not_in_inner) "
            "ORDER BY a;",
            [])
        query(
            "SELECT a,b FROM composite_not_in_outer "
            "WHERE (a,b) NOT IN (SELECT a,b FROM composite_not_in_inner) "
            "ORDER BY a;",
            [["1", "2"], ["3", None], ["4", "5"], [None, "3"]])
        print("[COMPOSITE NOT IN NULL SEMANTICS E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
