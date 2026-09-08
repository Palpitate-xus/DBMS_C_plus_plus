#!/usr/bin/env python3
"""Assert review-closeout SQL results using an isolated local DBMS instance."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "review_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        expected = [
            [["0", "63"], [None, "63"]],
            [["0", None, "42"], ["1", None, "21"],
             [None, "0", "63"], [None, None, "63"]],
            [["0", "42"], ["1", "21"], [None, "63"]],
            [["0", "3"], [None, "3"]],
        ]
        selects = 0
        case = root / "tests/compat/cases/grouping_sets_expr.sql"
        for statement in case.read_text(encoding="utf-8").splitlines():
            if not statement.strip() or statement.lstrip().startswith("--"):
                continue
            rows, state, message, _ = runner.ours_query(client, server["sock"], statement)
            assert state is None, (statement, state, message)
            if statement.upper().startswith("SELECT"):
                assert rows == expected[selects], (statement, rows, expected[selects])
                selects += 1
        assert selects == len(expected)
        print("[REVIEW SQL E2E] GROUPING SETS / ROLLUP / CUBE expressions passed")
        collection_cases = [
            ("SELECT v % 2 AS p, array_agg(id ORDER BY id DESC) FROM diff_gsexpr "
             "GROUP BY v % 2 ORDER BY p;", [["0", "{3,1}"], ["1", "{2}"]]),
            ("SELECT v % 2 AS p, string_agg(id::text, '|' ORDER BY id DESC) FROM diff_gsexpr "
             "GROUP BY v % 2 ORDER BY p;", [["0", "3|1"], ["1", "2"]]),
            ("SELECT id, array_agg(id + 1 ORDER BY id DESC) FROM diff_gsexpr "
             "GROUP BY id ORDER BY id DESC;", [["3", "{4}"], ["2", "{3}"], ["1", "{2}"]]),
            ("SELECT string_agg(id::text, '|' ORDER BY id DESC) FROM diff_gsexpr;",
             [["3|2|1"]]),
            ("SELECT array_agg(id * 2 ORDER BY id DESC) FROM diff_gsexpr;", [["{6,4,2}"]]),
            ("SELECT array_agg(id ORDER BY id DESC) FILTER (WHERE id > 1) "
             "FROM diff_gsexpr;", [["{3,2}"]]),
            ("SELECT array_agg(id) FILTER (WHERE id > 99), "
             "string_agg(id::text, '|') FILTER (WHERE id > 99) FROM diff_gsexpr;",
             [[None, None]]),
            ("SELECT id, sum(v) FROM diff_gsexpr GROUP BY id ORDER BY id DESC;",
             [["3", "32"], ["2", "21"], ["1", "10"]]),
        ]
        for statement, expected_rows in collection_cases:
            rows, state, message, _ = runner.ours_query(client, server["sock"], statement)
            assert state is None, (statement, state, message)
            assert rows == expected_rows, (statement, rows, expected_rows)
        print("[REVIEW SQL E2E] grouped / plain collection aggregate routing passed")
        cases = [
            ("SELECT (SELECT id FROM diff_gsexpr ORDER BY id DESC LIMIT 1) AS picked "
             "FROM diff_gsexpr WHERE id = 1;", [["3"]]),
            ("SELECT (SELECT id FROM diff_gsexpr LIMIT 1 OFFSET 1) AS picked "
             "FROM diff_gsexpr WHERE id = 1;", [["2"]]),
            ("SELECT EXISTS (SELECT 1 FROM diff_gsexpr LIMIT 0) AS present "
             "FROM diff_gsexpr WHERE id = 1;", [["f"]]),
            ("SELECT NOT EXISTS (SELECT 1 FROM diff_gsexpr LIMIT 1 OFFSET 3) AS absent "
             "FROM diff_gsexpr WHERE id = 1;", [["t"]]),
            ("SELECT EXISTS (SELECT ' from missing ' FROM diff_gsexpr WHERE id = 1) "
             "AS present FROM diff_gsexpr WHERE id = 1;", [["t"]]),
        ]
        for statement, expected_rows in cases:
            rows, state, message, _ = runner.ours_query(client, server["sock"], statement)
            assert state is None, (statement, state, message)
            assert rows == expected_rows, (statement, rows, expected_rows)
        print("[REVIEW SQL E2E] projection subquery clauses passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
