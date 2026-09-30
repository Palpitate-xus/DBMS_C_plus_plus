#!/usr/bin/env python3
"""Bitmap explanations must identify the executed node in text and JSON."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "bitmap_explain_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def expect(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, rows, state, message)
        return rows

    def nodes(node):
        yield node
        for child in node.get("children", []):
            yield from nodes(child)

    try:
        expect("CREATE TABLE bitmap_items (id INT PRIMARY KEY,value INT);")
        expect("INSERT INTO bitmap_items VALUES (1,7),(2,7),(3,8);")
        expect("CREATE INDEX bitmap_value_idx ON bitmap_items USING hash (value);")
        # Calibrate cardinalities explicitly: without ANALYZE the existing
        # planner falls back to absent statistics as zero, a separate issue.
        expect("ANALYZE;")
        query = "SELECT id FROM bitmap_items WHERE value=7 AND id=1;"
        assert expect(query) == [["1"]]
        text = "\n".join(row[0] for row in expect("EXPLAIN " + query))
        assert "BitmapHeapScan(table=bitmap_items)" in text, text
        assert "Unknown" not in text, text
        encoded = "\n".join(row[0] for row in expect("EXPLAIN (FORMAT JSON) " + query))
        document = json.loads(encoded)
        bitmap = [node for node in nodes(document["plan"])
                  if node.get("nodeType") == "BitmapHeapScan"]
        assert len(bitmap) == 1, document
        assert bitmap[0]["table"] == "bitmap_items", bitmap
        assert bitmap[0]["rows"] == 3 and bitmap[0]["cost"] > 0, bitmap
        actual = "\n".join(row[0] for row in expect("EXPLAIN ANALYZE " + query))
        bitmap_lines = [line for line in actual.splitlines()
                        if "BitmapHeapScan(table=bitmap_items)" in line and "actual time=" in line]
        assert len(bitmap_lines) == 1, actual
        assert "rows=1 loops=2" in bitmap_lines[0], actual
        without_costs = "\n".join(row[0] for row in expect("EXPLAIN (COSTS FALSE) " + query))
        assert "BitmapHeapScan(table=bitmap_items)" in without_costs, without_costs
        assert "cost=" not in without_costs, without_costs
        assert expect("SELECT 42;") == [["42"]]
        print("[BITMAP EXPLAIN PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
