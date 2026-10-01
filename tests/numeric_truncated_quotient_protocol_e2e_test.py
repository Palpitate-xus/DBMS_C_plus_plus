#!/usr/bin/env python3
"""MOD, % and DIV use a truncated quotient rather than a rounded one."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "numeric_truncation_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        for left, right, remainder, quotient in [
            ("100000000000000000000001", "3", "2", "33333333333333333333333"),
            ("-100000000000000000000001", "3", "-2", "-33333333333333333333333"),
            ("100000000000000000000001", "-3", "2", "-33333333333333333333333"),
            ("-100000000000000000000001", "-3", "-2", "33333333333333333333333"),
            ("10.50", "3.0", "1.50", "3"), ("0.00", "7", "0.00", "0")]:
            sql = "SELECT mod(" + left + "::numeric," + right + "::numeric)," + left + "::numeric%" + right + "::numeric,div(" + left + "::numeric," + right + "::numeric);"
            rows = query(sql)
            assert rows == [[remainder, remainder, quotient]], (sql, rows)
        assert query("SELECT 42;") == [["42"]]
        print("[NUMERIC TRUNCATED QUOTIENT PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
