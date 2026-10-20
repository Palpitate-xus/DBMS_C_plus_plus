#!/usr/bin/env python3
"""Index readers report corruption; independent heap readers stay exact."""

import importlib.util
from pathlib import Path
import struct


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "index_corruption_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def expect(sql, expected_state=None):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected_state, (sql, rows, state, message)
        return rows

    def corrupt(path):
        assert path.is_file(), path
        pages = bytearray(4096 * 3)
        struct.pack_into("=IIH", pages, 0, 1, 3, 2)
        struct.pack_into("=I", pages, 4096 + 3, 2)
        struct.pack_into("=I", pages, 8192 + 3, 1)
        replacement = path.with_name(path.name + ".corrupt-test")
        replacement.write_bytes(pages)
        replacement.replace(path)

    try:
        expect("CREATE TABLE corrupt_items (id INT PRIMARY KEY, tenant INT, state INT);")
        expect("CREATE INDEX corrupt_tenant_idx ON corrupt_items(tenant);")
        expect("CREATE INDEX corrupt_state_idx ON corrupt_items(state);")
        expect("INSERT INTO corrupt_items VALUES (1,7,8);")
        queries = (
            "SELECT id FROM corrupt_items WHERE id=1;",
            "SELECT id FROM corrupt_items WHERE state=8;",
            "SELECT id FROM corrupt_items WHERE tenant=7 AND state=8;",
            "SELECT count(*) FROM corrupt_items WHERE state=8;",
        )
        for sql in queries:
            assert expect(sql) == [["1"]]
        other_queries = (
            ("SELECT tenant,count(*) FROM corrupt_items WHERE state=8 GROUP BY tenant;",
             [["7", "1"]]),
            ("SELECT tenant,count(*) FROM corrupt_items WHERE state=8 "
             "GROUP BY tenant HAVING count(*)>0;", [["7", "1"]]),
            ("SELECT EXISTS(SELECT 1 FROM corrupt_items WHERE state=8);", [["t"]]),
        )
        for sql, rows in other_queries:
            assert expect(sql) == rows
        index_queries = queries[:3] + tuple(sql for sql, _ in other_queries[:-1])
        for sql in index_queries:
            plan = expect("EXPLAIN " + sql)
            assert any(node in str(plan) for node in ("IndexScan", "BitmapHeapScan")), (sql, plan)
        heap_queries = ((queries[3], [["1"]]),
                        ("SELECT count(*) FROM corrupt_items WHERE state=99;", [["0"]]),
                        other_queries[-1],
                        ("SELECT EXISTS(SELECT 1 FROM corrupt_items WHERE state=99);", [["f"]]))
        for sql, _ in heap_queries[:2]:
            plan = expect("EXPLAIN " + sql)
            assert "TableScan(table=corrupt_items)" in str(plan), (sql, plan)
            assert not any(node in str(plan) for node in ("IndexScan", "BitmapHeapScan")), (sql, plan)
        for sql, rows in heap_queries:
            assert expect(sql) == rows

        database = Path(server["dir"]) / "info"
        corrupt(database / "corrupt_items.idx")
        corrupt(database / "corrupt_items_tenant.idx")
        corrupt(database / "corrupt_items_state.idx")
        for sql in index_queries:
            assert expect(sql, "XX001") == []
        # Direct COUNT and EXISTS' prepared child use their actual heap
        # scans, unlike the five verified index readers (including grouped
        # COUNT/HAVING) above. They must not inspect an unused index, nor
        # hide a row/error behind successful empty results: keep both exact
        # matching and nonmatching Boolean controls after corruption.
        for sql, rows in heap_queries:
            assert expect(sql) == rows
        assert expect("SELECT id FROM corrupt_items;") == [["1"]]
        assert expect("SELECT 42;") == [["42"]]
        print("INDEX_READER_ROUTE_COMPLETE 5 INDEX_ERRORS 4 EXACT_HEAP_CONTROLS")
        print("[INDEX CORRUPTION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
