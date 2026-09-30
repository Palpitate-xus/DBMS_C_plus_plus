#!/usr/bin/env python3
"""Full-value index rechecks retain exact equivalent integer SQL spellings."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "integer_recheck_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        query("CREATE TABLE integer_recheck(id INT PRIMARY KEY);")
        query("INSERT INTO integer_recheck VALUES (0),(1);")
        for probe in ["1.0", "1e0", "+0001", "1.5"]:
            expected = 0 if probe == "1.5" else 1
            rows = query("EXPLAIN (FORMAT JSON,ANALYZE TRUE) SELECT id FROM integer_recheck WHERE id=" + probe + ";")
            document = json.loads("\n".join(row[0] for row in rows))
            assert document["actualRows"] == expected, (probe, document)
        assert query("SELECT id FROM integer_recheck WHERE id=1.0 AND abs(id)>0;") == [["1"]]
        query("CREATE TABLE bigint_recheck(id BIGINT PRIMARY KEY);")
        query("INSERT INTO bigint_recheck VALUES (9007199254740992),(9007199254740993);")
        assert query("SELECT id FROM bigint_recheck WHERE id=9007199254740993.0 AND abs(id)>0;") == [["9007199254740993"]]
        assert query("SELECT 42;") == [["42"]]
        print("[INTEGER INDEX RECHECK PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
