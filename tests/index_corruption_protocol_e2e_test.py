#!/usr/bin/env python3
"""A corrupt B-tree must produce an error rather than successful empty rows."""

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
        primary_plan = expect("EXPLAIN " + queries[0])
        secondary_plan = expect("EXPLAIN " + queries[1])
        assert "IndexScan" in str(primary_plan), primary_plan
        assert "IndexScan" in str(secondary_plan), secondary_plan

        database = Path(server["dir"]) / "info"
        corrupt(database / "corrupt_items.idx")
        corrupt(database / "corrupt_items_tenant.idx")
        corrupt(database / "corrupt_items_state.idx")
        for sql in queries:
            assert expect(sql, "XX001") == []
        for sql, _ in other_queries:
            assert expect(sql, "XX001") == []
        assert expect("SELECT id FROM corrupt_items;") == [["1"]]
        assert expect("SELECT 42;") == [["42"]]
        print("[INDEX CORRUPTION PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
