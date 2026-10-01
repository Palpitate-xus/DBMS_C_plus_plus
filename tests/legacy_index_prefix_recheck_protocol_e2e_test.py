#!/usr/bin/env python3
"""The legacy equality path rechecks lossy indexed candidates too."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "legacy_index_recheck_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        prefix = "12345678901234567890"
        first, second, missing = prefix + "-alpha", prefix + "-beta", prefix + "-missing"
        query('CREATE TABLE legacy_index_prefix(id INT PRIMARY KEY,k TEXT COLLATE "C");')
        query("INSERT INTO legacy_index_prefix VALUES(1,'" + first + "'),(2,'" + second + "'),(3,'" + prefix + "');")
        query("CREATE INDEX legacy_index_prefix_k ON legacy_index_prefix(k);")
        for key, expected in [(first, "1"), (second, "2"), (prefix, "3"), (missing, None)]:
            rows = query("SELECT id,k FROM legacy_index_prefix WHERE k='" + key + "';")
            assert rows == ([] if expected is None else [[expected, key]]), (key, rows)
        assert query("SELECT id FROM legacy_index_prefix WHERE k='" + first + "' AND id=2;") == []
        assert query("SELECT id FROM legacy_index_prefix WHERE k='" + first + "' AND length(k)>0;") == [["1"]]
        query('CREATE TABLE legacy_primary_prefix(id INT,k TEXT COLLATE "C" PRIMARY KEY);')
        query("INSERT INTO legacy_primary_prefix VALUES(1,'" + first + "');")
        assert query("SELECT id FROM legacy_primary_prefix WHERE k='" + first + "';") == [["1"]]
        assert query("SELECT id FROM legacy_primary_prefix WHERE k='" + missing + "';") == []
        assert query("SELECT id FROM legacy_primary_prefix WHERE k='" + prefix + "';") == []
        assert query("SELECT 42;") == [["42"]]
        print("[LEGACY INDEX PREFIX RECHECK PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
