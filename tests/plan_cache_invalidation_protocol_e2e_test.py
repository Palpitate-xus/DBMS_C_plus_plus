#!/usr/bin/env python3
"""EXPLAIN caches must follow catalog changes, including transactional undo."""

import importlib.util
from pathlib import Path
import socket


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "plan_cache_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    observer = None

    def expect(sql, sock=None, state=None):
        rows, actual_state, message, _, _ = runner.decode_wire_result(
            client.simple_query(sock or server["sock"], sql))
        assert actual_state == state, (sql, rows, actual_state, message)
        return rows

    query = "EXPLAIN SELECT value FROM cache_items WHERE value=7;"

    def plan(node, hit, sock=None):
        rows = expect(query, sock)
        text = str(rows)
        assert node in text, (node, rows)
        assert ("[plan cache hit]" in text) == hit, (hit, rows)
        if node == "TableScan":
            assert "IndexScan" not in text, rows

    try:
        expect("CREATE TABLE cache_items (id INT PRIMARY KEY,value INT);")
        expect("INSERT INTO cache_items VALUES (1,7),(2,8);")
        observer = socket.create_connection(("127.0.0.1", server["port"]),
                                            timeout=runner.wire_timeout())
        client.startup(observer, "alice", "info")
        plan("TableScan", False)
        plan("TableScan", True)
        expect("CREATE INDEX cache_value_idx ON cache_items(value);", observer)
        plan("IndexScan", False)
        plan("IndexScan", True, observer)
        expect("BEGIN;")
        expect("DROP INDEX cache_value_idx;")
        plan("TableScan", False)
        expect("ROLLBACK;")
        plan("IndexScan", False)
        plan("IndexScan", True)
        expect("DROP INDEX cache_value_idx;", observer)
        plan("TableScan", False)
        plan("TableScan", True)

        expect("BEGIN;")
        expect("CREATE INDEX cache_value_idx ON cache_items(value);")
        plan("IndexScan", False)
        plan("IndexScan", True)
        expect("ROLLBACK;")
        plan("TableScan", False)
        plan("TableScan", True)

        expect("BEGIN;")
        expect("SAVEPOINT before_index;")
        expect("CREATE INDEX cache_value_idx ON cache_items(value);")
        plan("IndexScan", False)
        expect("ROLLBACK TO SAVEPOINT before_index;")
        plan("TableScan", False)
        expect("COMMIT;")

        # Reusing a relation name must not reuse the dropped table's plan.
        expect("CREATE INDEX cache_value_idx ON cache_items(value);")
        plan("IndexScan", False)
        expect("DROP TABLE cache_items;")
        expect("CREATE TABLE cache_items (id INT,value INT);")
        expect("INSERT INTO cache_items VALUES (3,7);")
        plan("TableScan", False)
        plan("TableScan", True)
        # Whole-database ANALYZE uses the supported PostgreSQL spelling; the
        # legacy per-relation handler still requires a non-PG TABLE keyword.
        expect("ANALYZE;")
        plan("TableScan", False)
        plan("TableScan", True)
        assert expect("SELECT id FROM cache_items WHERE value=7;") == [["3"]]
        assert expect("SELECT 42;") == [["42"]]
        print("[PLAN CACHE INVALIDATION PROTOCOL E2E] passed")
    finally:
        if observer is not None:
            observer.close()
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
