#!/usr/bin/env python3
"""Native ANALYZE retains typed min/max and exact histogram boundaries."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "analyze_typed_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def boundaries(table, original, low, high):
        # Read only the test server's own isolated artifact. The separately
        # tracked legacy catalog projection/WHERE path is not exercised here.
        lines = (Path(server["dir"]) / "info" / ".stats").read_text().splitlines()
        line = next(line for line in lines if line.startswith(table + " v "))
        fields = line.split("|")
        assert fields[1:3] == [low, high], line
        buckets = [bucket.split(",") for bucket in fields[3].split(";")]
        assert len(buckets) == 10 and buckets[0][0] == low and buckets[-1][1] == high, line
        assert all(value in original for bucket in buckets for value in bucket), line

    try:
        query("CREATE TABLE fractional_stats (id INT PRIMARY KEY,v DOUBLE PRECISION);")
        fractional = {f"{i}.25" for i in range(1, 21)}
        query("INSERT INTO fractional_stats VALUES " +
              ",".join(f"({i},{i}.25)" for i in range(20, 0, -1)) + ";")
        query("ANALYZE fractional_stats;")
        boundaries("fractional_stats", fractional, "1.25", "20.25")
        assert query("SELECT id FROM fractional_stats WHERE v=20.25;") == [["20"]]
        query("CREATE TABLE exact_stats (id INT PRIMARY KEY,v NUMERIC);")
        numeric = {f"123456789012345678901234567890.{i + 10}" for i in range(1, 21)}
        query("INSERT INTO exact_stats VALUES " +
              ",".join(f"({i},123456789012345678901234567890.{i + 10})"
                       for i in range(20, 0, -1)) + ";")
        query("ANALYZE exact_stats;")
        boundaries("exact_stats", numeric,
                   "123456789012345678901234567890.11",
                   "123456789012345678901234567890.30")
        assert query("SELECT id FROM exact_stats ORDER BY id;") == [[str(i)] for i in range(1, 21)]
        assert query("SELECT 42;") == [["42"]]
        print("[ANALYZE TYPED STATISTICS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
