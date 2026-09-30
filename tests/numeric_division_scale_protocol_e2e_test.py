#!/usr/bin/env python3
"""numeric quotient scale uses the exact inputs, including zero and high dscale."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "division_scale_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    cases = [
        ("0", "1", "0.00000000000000000000"),
        ("0.00", "10000", "0.000000000000000000000000"),
        ("0", "0.0001", "0.0000000000000000"),
        ("1", "10000", "0.000100000000000000000000"),
        ("1", "1.000000000000000000000001", "0.999999999999999999999999"),
        ("100000000000000000000", "3", "33333333333333333333"),
        ("1", "0.00000001", "100000000.000000000000"),
        ("-1", "100000000", "-0.0000000100000000000000000000"),
        ("0.00000001", "3", "0.0000000033333333333333333333"),
        ("100000000", "7", "14285714.285714285714"),
        ("9999999999999999", "7", "1428571428571428.4286"),
        ("123.45", "0.001", "123450.000000000000"),
        ("1.2345678901234567890123456789", "1", "1.2345678901234567890123456789"),
    ]
    try:
        for a, b, expected in cases:
            assert query(f"SELECT {a}::numeric/{b}::numeric AS q;") == [[expected]]
        query("CREATE TABLE division_scale_rows(id INT PRIMARY KEY,n NUMERIC);")
        query("INSERT INTO division_scale_rows VALUES (1,0.00),(2,100000000);")
        assert query("SELECT id,n/7::numeric AS q FROM division_scale_rows ORDER BY id;") == [
            ["1", "0.00000000000000000000"], ["2", "14285714.285714285714"]]
        assert query("SELECT 42;") == [["42"]]
        print("[NUMERIC DIVISION SCALE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
