#!/usr/bin/env python3
"""Cache-hit diagnostics may not corrupt a FORMAT JSON document."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "json_cache_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def expect(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, rows, state, message)
        return "\n".join(row[0] for row in rows)

    def document(sql):
        encoded = expect(sql)
        try:
            return json.loads(encoded)
        except json.JSONDecodeError as error:
            raise AssertionError((sql, encoded)) from error

    try:
        expect("CREATE TABLE json_cache (id INT);")
        expect("INSERT INTO json_cache VALUES (1);")
        query = "SELECT id FROM json_cache WHERE id=1;"
        for options in ("FORMAT JSON", "FORMAT JSON, COSTS FALSE"):
            sql = "EXPLAIN (" + options + ") " + query
            first = document(sql)
            assert document(sql) == first
        # Keep the existing text-mode cache-hit indication and cache behavior.
        assert "[plan cache hit]" not in expect("EXPLAIN " + query)
        assert "[plan cache hit]" in expect("EXPLAIN " + query)
        expect("CREATE INDEX json_cache_id_idx ON json_cache(id);")
        sql = "EXPLAIN (FORMAT JSON) " + query
        after_ddl = document(sql)
        assert "IndexScan" in str(after_ddl), after_ddl
        assert document(sql) == after_ddl
        assert expect("SELECT 42;") == "42"
        print("[EXPLAIN JSON CACHE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
