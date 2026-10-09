#!/usr/bin/env python3
"""WITH inside a value or subquery must not enter top-level CTE rewriting."""

import importlib.util
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("cte_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    collect_errors = "--collect-errors" in sys.argv
    failures = []
    try:
        for statement in ("CREATE TABLE cte_boundary_rows (id INT);",
                          "INSERT INTO cte_boundary_rows VALUES (1);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], statement)
            assert state is None, (statement, state, message)
        for statement, expected in [
            ("SELECT 'with data' AS value FROM cte_boundary_rows;", [["with data"]]),
            ("SELECT 'start with data' AS value FROM cte_boundary_rows;", [["start with data"]]),
            ("SELECT (SELECT 'with data' FROM cte_boundary_rows) AS value "
             "FROM cte_boundary_rows;", [["with data"]]),
            ("WITH selected AS (SELECT id FROM cte_boundary_rows) SELECT id FROM selected;", [["1"]]),
            (("WITH unused AS (SELECT id FROM cte_boundary_rows) "
              "SELECT 1 AS answer;"), [["1"]]),
            (("WITH unused AS (SELECT id FROM cte_boundary_rows) "
              "SELECT 'a b'::text AS answer;"), [["a b"]]),
            (("WITH unused AS (SELECT id FROM cte_boundary_rows) "
              "SELECT unnest(ARRAY[1, 2]);"), [["1"], ["2"]]),
        ]:
            if collect_errors:
                print("CTE_CLAUSE_BOUNDARY_ORIGINAL_SQL", statement, flush=True)
            rows, state, message, _ = runner.ours_query(client, server["sock"], statement)
            try:
                assert state is None, (statement, state, message)
                assert rows == expected, (statement, rows, expected)
            except AssertionError as failure:
                if not collect_errors:
                    raise
                failures.append(failure.args)
                print("CTE_CLAUSE_BOUNDARY_STRONG_FAILURE", failure.args, flush=True)

        decoded = runner.decode_wire_result(
            client.simple_query(
                server["sock"],
                "WITH unused AS (SELECT id FROM cte_boundary_rows) "
                "SELECT true AS flag;"),
            include_types=True)
        rows, state, message, headers, command_tag, type_oids = decoded
        assert state is None, (state, message)
        assert rows == [["t"]], rows
        assert headers == ["flag"], headers
        assert command_tag == "SELECT 1", command_tag
        assert type_oids == [16], type_oids

        for statement, expected in (
                ("SELECT (SELECT 'with data' FROM cte_boundary_rows) AS value "
                 "FROM cte_boundary_rows;", [["with data"]]),
                ("SELECT (SELECT NULL FROM cte_boundary_rows) AS value "
                 "FROM cte_boundary_rows;", [[None]]),
                ("SELECT (SELECT 'with data' FROM cte_boundary_rows WHERE false) "
                 "AS value FROM cte_boundary_rows;", [[None]]),
                ("SELECT (SELECT (SELECT 'nested')) AS value "
                 "FROM cte_boundary_rows;", [["nested"]])):
            decoded = runner.decode_wire_result(client.simple_query(
                server["sock"], statement), include_types=True)
            rows, state, message, headers, command_tag, type_oids = decoded
            assert state is None, (statement, state, message)
            assert rows == expected, (statement, rows, expected)
            assert headers == ["value"] and type_oids == [25], (statement, decoded)
            assert command_tag == "SELECT 1", (statement, decoded)
        assert not failures, ("complete original CTE clause boundary matrix failed", failures)
        print("[CTE CLAUSE BOUNDARY E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
