#!/usr/bin/env python3
"""Table CASE projections retain nested calls, predicate scope and SQL NULL."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("case_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    try:
        for sql in ("CREATE TABLE case_rows(id INT, txt TEXT);",
                    "INSERT INTO case_rows VALUES(1,'a b'),(2,NULL),(3,''),(4,'NULL');"):
            assert query(sql)[1] is None, sql
        cases = (
            ("SELECT id, CASE WHEN txt IS NULL THEN 'empty' ELSE upper(txt) END AS rendered "
             "FROM case_rows ORDER BY id;",
             [["1", "A B"], ["2", "empty"], ["3", ""], ["4", "NULL"]], [23, 25]),
            ("SELECT id, CASE id WHEN 1 THEN txt WHEN 2 THEN NULL ELSE 'other' END AS rendered "
             "FROM case_rows ORDER BY id;",
             [["1", "a b"], ["2", None], ["3", "other"], ["4", "other"]], [23, 25]),
            ("SELECT id, CASE WHEN id > 0 AND txt IS NOT NULL THEN "
             "CASE WHEN txt = '' THEN 'blank' ELSE txt END ELSE 'empty' END AS rendered "
             "FROM case_rows ORDER BY id;",
             [["1", "a b"], ["2", "empty"], ["3", "blank"], ["4", "NULL"]], [23, 25]),
            ("SELECT id, CASE WHEN txt IS NULL THEN NULL ELSE id + 10 END AS rendered "
             "FROM case_rows ORDER BY id;",
             [["1", "11"], ["2", None], ["3", "13"], ["4", "14"]], [23, 23]),
        )
        for sql, rows, types in cases:
            actual = query(sql)
            assert actual[1] is None, (sql, actual)
            assert actual[0] == rows and actual[5] == types, (sql, actual)
            assert actual[3] == ["id", "rendered"] and actual[4] == "SELECT 4", (sql, actual)
        invalid = query("SELECT CASE WHEN true THEN 'ok' ELSE missing END FROM case_rows;")
        assert invalid[1] == "42703" and invalid[0] == [] and invalid[4] is None, invalid
        print("[TABLE CASE AST PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
