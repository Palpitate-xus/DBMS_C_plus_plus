#!/usr/bin/env python3
"""Explicit numeric casts cannot take the integer arithmetic shortcut."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "numeric_cast_division_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE numeric_cast_division(id INT PRIMARY KEY,n NUMERIC);")
        query("INSERT INTO numeric_cast_division VALUES(1,0.00),(2,100000000);")
        rows = query("SELECT id,n::numeric/7 AS q,id::numeric/7 AS r FROM numeric_cast_division ORDER BY id;")
        assert rows == [
            ["1", "0.00000000000000000000", "0.14285714285714285714"],
            ["2", "14285714.285714285714", "0.28571428571428571429"]], rows
        assert query("SELECT id::bigint/7 AS q FROM numeric_cast_division ORDER BY id;") == [["0"], ["0"]]
        assert query("SELECT 42;") == [["42"]]
        print("[NUMERIC CAST DIVISION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
