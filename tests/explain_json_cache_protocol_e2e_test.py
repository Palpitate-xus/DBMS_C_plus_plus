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

    def reject(sql, state, ready=b"I"):
        messages = client.simple_query(server["sock"], sql)
        rows, actual_state, message, _, tag = runner.decode_wire_result(messages)
        assert actual_state == state and rows == [] and tag is None, (sql, actual_state, message, rows, tag)
        assert messages[-1] == (b"Z", ready), (sql, messages[-1])
        assert sum(kind == b"E" for kind, _ in messages) == 1, (sql, messages)
        assert not any(kind in (b"C", b"D") for kind, _ in messages), (sql, messages)

    def actual_rows(sql, wanted):
        analyzed = document("EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + sql)
        assert analyzed.get("actualRows") == wanted, (sql, analyzed, wanted)
        return analyzed

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

        # The bare SELECT * INNER JOIN stays on the physical EXPLAIN plan.
        # JSON must stay valid and stable on cache hits; text retains its
        # existing hit marker, and ANALYZE must execute rather than use it.
        expect("CREATE TABLE cache_join_l(k INT,v TEXT);")
        expect("CREATE TABLE cache_join_r(k INT,v TEXT);")
        expect("INSERT INTO cache_join_l VALUES (1,'a'),(2,'b');")
        expect("INSERT INTO cache_join_r VALUES (1,'x'),(2,'y');")
        join = "SELECT * FROM cache_join_l l JOIN cache_join_r r ON l.k=r.k;"
        join_json = "EXPLAIN (FORMAT JSON) " + join
        initial = document(join_json)
        assert "actualRows" not in initial and document(join_json) == initial, initial
        text = "EXPLAIN " + join
        assert "[plan cache hit]" not in expect(text)
        assert "[plan cache hit]" in expect(text)
        actual_rows(join, 2)
        expect("INSERT INTO cache_join_r VALUES (1,'extra');")
        actual_rows(join, 3)
        assert "[plan cache hit]" not in expect("EXPLAIN ANALYZE " + join)

        # The same cached SQL cannot bypass schema validation. A renamed key
        # fails, and rolling that DDL back restores the exact old binding.
        expect("BEGIN;")
        expect("ALTER TABLE cache_join_r RENAME COLUMN k TO moved;")
        reject(join_json, "42703", ready=b"E")
        expect("ROLLBACK;")
        actual_rows(join, 3)

        # Same-name replacement is a new physical relation. No saved plan
        # may hide a missing relation or a replacement without the key.
        expect("DROP TABLE cache_join_r;")
        reject(join_json, "42P01")
        reject(text, "42P01")
        expect("CREATE TABLE cache_join_r(z INT);")
        reject(join_json, "42703")
        reject(text, "42703")
        expect("DROP TABLE cache_join_r;")
        expect("CREATE TABLE cache_join_r(k INT,v TEXT);")
        expect("INSERT INTO cache_join_r VALUES (2,'new');")
        actual_rows(join, 1)
        replacement = document(join_json)
        assert document(join_json) == replacement
        assert expect("SELECT 42;") == "42"
        print("[EXPLAIN JSON CACHE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
