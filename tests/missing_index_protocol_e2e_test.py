#!/usr/bin/env python3
"""Missing live indexes must never be replaced with successful empty trees."""

import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "missing_index_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def expect(sql, expected_state=None):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected_state, (sql, rows, state, message)
        return rows

    try:
        expect("CREATE TABLE missing_items (id INT PRIMARY KEY, value INT, other INT);")
        expect("INSERT INTO missing_items VALUES (1,7,8);")
        expect("CREATE INDEX missing_value_idx ON missing_items(value);")
        expect("CREATE INDEX missing_other_idx ON missing_items(other);")
        expect("CREATE INDEX missing_both_idx ON missing_items(id,value);")
        bitmap = "SELECT id FROM missing_items WHERE value=7 AND other=8;"
        assert expect(bitmap) == [["1"]]
        database = Path(server["dir"]) / "info"
        paths = (
            (database / "missing_items.idx",
             "SELECT id FROM missing_items WHERE id=1;"),
            (database / "missing_items_value.idx",
             "SELECT id FROM missing_items WHERE value=7;"),
            (database / "missing_items.idx_missing_both_idx", None),
        )
        for path, sql in paths:
            assert path.is_file(), path
            if sql:
                assert expect(sql) == [["1"]]
                assert "IndexScan" in str(expect("EXPLAIN " + sql))
            path.unlink()
            if sql:
                assert expect(sql, "XX001") == []
            if path.name == "missing_items_value.idx":
                assert expect(bitmap, "XX001") == []
            expect("INSERT INTO missing_items VALUES (2,9,10);", "58030")
            assert not path.exists(), path
            assert expect("SELECT id,value FROM missing_items;") == [["1", "7"]]
            assert expect("SELECT 42;") == [["42"]]
            expect("REINDEX TABLE missing_items;")
            assert path.is_file(), path
            if sql:
                assert expect(sql) == [["1"]]
        expect("INSERT INTO missing_items VALUES (1,9,10);", "23505")
        print("[MISSING INDEX PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
