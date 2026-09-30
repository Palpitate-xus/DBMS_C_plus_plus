#!/usr/bin/env python3
"""TIMING FALSE hides node times while keeping actual rows and loops."""

import importlib.util
import itertools
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "timing_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return "\n".join(row[0] for row in rows)

    try:
        query("CREATE TABLE explain_timing (id INT PRIMARY KEY);")
        query("INSERT INTO explain_timing VALUES (1),(2);")
        for timing, costs, buffers in itertools.product((False, True), repeat=3):
            flag = lambda value: "TRUE" if value else "FALSE"
            sql = ("EXPLAIN (ANALYZE TRUE,TIMING " + flag(timing) +
                   ",COSTS " + flag(costs) + ",BUFFERS " + flag(buffers) +
                   ") SELECT id FROM explain_timing WHERE id=1;")
            for _ in range(2):
                text = query(sql)
                assert ("actual time=" in text) == timing, (sql, text)
                assert ("Total runtime:" in text) == timing, (sql, text)
                assert text.count("rows=1 loops=2") == 2, (sql, text)
                assert "Actual rows: 1" in text, (sql, text)
                assert ("cost=" in text) == costs, (sql, text)
                assert ("Buffers:" in text) == buffers, (sql, text)
        assert "(actual " not in query("EXPLAIN SELECT id FROM explain_timing WHERE id=1;")
        assert query("SELECT 42;") == "42"
        print("[EXPLAIN TIMING PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
