#!/usr/bin/env python3
"""ORDER BY NULLS placement survives a composite NOT IN plan."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "composite_not_in_order_by_pgdiff",
        root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE notin_order_outer(a INT,b INT);", [])
        query("INSERT INTO notin_order_outer VALUES "
              "(1,2),(NULL,3),(4,5),(3,NULL);", [])
        query("CREATE TABLE notin_order_inner(a INT,b INT);", [])

        base = ("SELECT a,b FROM notin_order_outer "
                "WHERE (a,b) NOT IN "
                "(SELECT a,b FROM notin_order_inner) ")
        query(base + "ORDER BY a NULLS FIRST;",
              [[None, "3"], ["1", "2"], ["3", None], ["4", "5"]])
        query(base + "ORDER BY a DESC NULLS LAST;",
              [["4", "5"], ["3", None], ["1", "2"], [None, "3"]])
        query(base + "ORDER BY a DESC;",
              [[None, "3"], ["4", "5"], ["3", None], ["1", "2"]])

        query("CREATE TABLE notin_order_text(v TEXT);", [])
        query("INSERT INTO notin_order_text VALUES "
              "(NULL),(''),('a'),('b');", [])
        query("CREATE TABLE notin_order_text_empty(v TEXT);", [])
        query("SELECT v FROM notin_order_text "
              "WHERE v NOT IN "
              "(SELECT v FROM notin_order_text_empty) "
              "ORDER BY v DESC NULLS LAST;",
              [["b"], ["a"], [""], [None]])
        print("[COMPOSITE NOT IN ORDER BY NULLS E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
