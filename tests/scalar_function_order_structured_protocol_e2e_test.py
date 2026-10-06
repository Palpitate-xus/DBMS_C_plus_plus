#!/usr/bin/env python3
"""Function ORDER BY must not route structured scalar rows through display text."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("scalar_order_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in ("CREATE TABLE scalar_function_order(id INT, fallback INT, txt TEXT);",
                    "INSERT INTO scalar_function_order VALUES"
                    "(1,NULL,'a b'),(NULL,2,NULL),(3,NULL,''),(NULL,4,'NULL'),(1,NULL,'a b');"):
            assert query(sql)[1] is None, sql
        cases = (
            ("SELECT coalesce(id,fallback) AS key, txt FROM scalar_function_order "
             "ORDER BY coalesce(id,fallback);",
             [["1", "a b"], ["1", "a b"], ["2", None], ["3", ""], ["4", "NULL"]],
             ["key", "txt"], [23, 25]),
            ("SELECT upper(txt) AS rendered, txt FROM scalar_function_order "
             "ORDER BY coalesce(id,fallback) DESC;",
             [["NULL", "NULL"], ["", ""], [None, None], ["A B", "a b"], ["A B", "a b"]],
             ["rendered", "txt"], [25, 25]),
            ("SELECT upper(txt) AS rendered, txt FROM scalar_function_order "
             "ORDER BY upper(txt) NULLS FIRST;",
             [[None, None], ["", ""], ["A B", "a b"], ["A B", "a b"], ["NULL", "NULL"]],
             ["rendered", "txt"], [25, 25]),
            ("SELECT DISTINCT coalesce(id,fallback) AS key, txt FROM scalar_function_order "
             "ORDER BY coalesce(id,fallback) LIMIT 3 OFFSET 1;",
             [["2", None], ["3", ""], ["4", "NULL"]],
             ["key", "txt"], [23, 25]),
        )
        for sql, rows, names, types in cases:
            actual = query(sql)
            assert actual[1] is None, (sql, actual)
            assert actual[0] == rows and actual[3] == names and actual[5] == types, (sql, actual)
            assert actual[4] == f"SELECT {len(rows)}", (sql, actual)
        print("[SCALAR FUNCTION ORDER STRUCTURED PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
