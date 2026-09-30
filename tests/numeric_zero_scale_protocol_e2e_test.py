#!/usr/bin/env python3
"""Zero magnitude must not erase numeric display scale."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "numeric_zero_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        assert query("SELECT '-0.00'::numeric AS a,'0.000'::numeric AS b,-'0.000'::numeric AS c;") == [["0.00", "0.000", "0.000"]]
        assert query("SELECT '0.00'::numeric+'1.0'::numeric AS a,'0.00'::numeric*'1.0'::numeric AS b,'1.25'::numeric-'1.25'::numeric AS c;") == [["1.00", "0.000", "0.00"]]
        assert query("SELECT round('0.001'::numeric,2);") == [["0.00"]]
        query("CREATE TABLE numeric_zero_values(id INT,v NUMERIC);")
        query("INSERT INTO numeric_zero_values VALUES (1,-0.00),(2,0.000);")
        assert query("SELECT id,v FROM numeric_zero_values ORDER BY id;") == [["1", "0.00"], ["2", "0.000"]]
        assert query("SELECT count(DISTINCT v) FROM numeric_zero_values;") == [["1"]]
        assert query("SELECT 42;") == [["42"]]
        print("[NUMERIC ZERO SCALE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
