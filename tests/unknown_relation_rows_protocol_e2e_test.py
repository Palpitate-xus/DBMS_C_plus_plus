#!/usr/bin/env python3
"""Missing row statistics are unknown, distinct from analyzed empty data."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "unknown_relation_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def explain(sql):
        return json.loads("\n".join(row[0] for row in query("EXPLAIN (FORMAT JSON) " + sql)))

    def nodes(node):
        yield node
        for child in node.get("children", []):
            yield from nodes(child)

    try:
        query("CREATE TABLE unknown_rows (id INT PRIMARY KEY,v INT);")
        query("INSERT INTO unknown_rows VALUES (1,7),(2,7),(3,8);")
        query("CREATE INDEX unknown_rows_v ON unknown_rows USING hash (v);")
        # Earlier legacy SQL auto-ANALYZEd inside the write statement and
        # persisted zero before its rows were visible. If a statistics file
        # exists, remove only this isolated fixture to exercise missing
        # evidence; newer writes may already leave statistics absent.
        stats_file = Path(server["dir"]) / "info" / ".stats"
        if stats_file.is_file():
            stats_file.unlink()
        for sql, kind in (("SELECT id FROM unknown_rows;", "TableScan"),
                          ("SELECT id FROM unknown_rows WHERE id=1 AND v=7;", "BitmapHeapScan")):
            for _ in range(2):
                node = [node for node in nodes(explain(sql)["plan"]) if node["nodeType"] == kind][0]
                assert node["rows"] == 1000 and node["cost"] > 0, node
        query("CREATE TABLE unknown_empty (id INT);")
        query("CREATE TABLE analyzed_empty (id INT);")
        assert explain("SELECT id FROM unknown_empty;")["totalRows"] == 1000
        # Native per-relation ANALYZE currently raises 42601; that separate
        # gap is retained. The supported PostgreSQL whole-database spelling
        # still supplies real zero/nonzero ANALYZE evidence for this test.
        query("ANALYZE;")
        assert explain("SELECT id FROM analyzed_empty;")["totalRows"] == 0
        node = [node for node in nodes(explain("SELECT id FROM unknown_rows;")["plan"])
                if node["nodeType"] == "TableScan"][0]
        assert node["rows"] == 3, node
        assert query("SELECT id FROM unknown_rows ORDER BY id;") == [["1"], ["2"], ["3"]]
        print("[UNKNOWN RELATION ROWS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
