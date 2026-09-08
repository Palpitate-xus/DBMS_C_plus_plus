#!/usr/bin/env python3
"""Zero LIMIT and offsets at/past the end must return no input rows."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("limit_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    try:
        for sql in ("CREATE TABLE limit_rows (id INT);",
                    "INSERT INTO limit_rows VALUES (1), (2), (3);",
                    "CREATE TABLE limit_other (id INT);",
                    "INSERT INTO limit_other VALUES (1), (2), (3);"):
            _, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None, (sql, state, message)
        for suffix, expected in [("LIMIT 0", []), ("OFFSET 3", []), ("OFFSET 99", []),
                                 ("LIMIT ALL OFFSET 3", []), ("FETCH FIRST 0 ROWS ONLY", []),
                                 ("LIMIT 2 OFFSET 9223372036854775807", []),
                                 ("LIMIT 9223372036854775807 OFFSET 1", [["2"], ["3"]]),
                                 ("LIMIT 2 OFFSET 1", [["2"], ["3"]])]:
            sql = "SELECT id FROM limit_rows ORDER BY id " + suffix + ";"
            rows, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None and rows == expected, (sql, state, message, rows, expected)
        join = " FROM limit_rows JOIN limit_other ON limit_rows.id = limit_other.id "
        for sql, expected in [
            ("SELECT id + 1 FROM limit_rows LIMIT 0;", []),
            ("SELECT count(*) FROM limit_rows LIMIT 0;", []),
            ("SELECT id, count(*) FROM limit_rows GROUP BY id OFFSET 3;", []),
            ("SELECT id, row_number() OVER (ORDER BY id) FROM limit_rows LIMIT 0;", []),
            ("SELECT id, row_number() OVER (ORDER BY id) AS num FROM limit_rows OFFSET 3;", []),
            ("SELECT limit_rows.id" + join + "LIMIT 0;", []),
            ("SELECT limit_rows.id" + join + "OFFSET 3;", []),
            ("SELECT count(*)" + join + "LIMIT 1;", [["3"]]),
            ("SELECT count(*)" + join + "LIMIT 0;", []),
            ("SELECT count(*)" + join + "OFFSET 1;", []),
        ]:
            rows, state, message, _ = runner.ours_query(client, server["sock"], sql)
            assert state is None and rows == expected, (sql, state, message, rows, expected)
        print("[LIMIT OFFSET BOUNDARY E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
