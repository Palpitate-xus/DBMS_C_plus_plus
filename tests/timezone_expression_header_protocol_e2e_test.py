#!/usr/bin/env python3
"""AT TIME ZONE is a timezone function for implicit result-column naming."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "timezone_header_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql, expected_rows, expected_headers):
        rows, state, message, headers, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        assert rows == expected_rows, (sql, rows)
        assert headers == expected_headers, (sql, headers)

    try:
        query("SELECT timestamp '2026-08-15 14:30:05' AT TIME ZONE 'UTC',timestamptz '2026-08-17 10:00:00+00' AT TIME ZONE 'America/New_York','2024-06-01 00:30:00'::timestamp AT TIME ZONE 'UTC+8';",
              [["2026-08-15 14:30:05+00", "2026-08-17 06:00:00", "2024-06-01 08:30:00+00"]],
              ["timezone", "timezone", "timezone"])
        query("SELECT (timestamp '2026-08-15 14:30:05' AT TIME ZONE 'UTC'),timestamp '2026-08-15 14:30:05' AT TIME ZONE 'UTC' AS instant;",
              [["2026-08-15 14:30:05+00", "2026-08-15 14:30:05+00"]], ["timezone", "instant"])
        query("SELECT -1::numeric,1::numeric+2::numeric;",
              [["-1", "3"]], ["?column?", "?column?"])
        print("[TIMEZONE EXPRESSION HEADER PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
