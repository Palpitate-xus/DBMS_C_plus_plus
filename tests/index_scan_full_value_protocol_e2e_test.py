#!/usr/bin/env python3
"""A truncated B-tree key is a candidate, not proof of full SQL equality."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "index_recheck_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def explain(table, key, expected):
        rows = query("EXPLAIN (FORMAT JSON,ANALYZE TRUE) SELECT * FROM " + table + " WHERE k='" + key + "';")
        document = json.loads("\n".join(row[0] for row in rows))
        assert document["actualRows"] == expected, (table, key, document)
        def nodes(node):
            yield node
            for child in node.get("children", []):
                yield from nodes(child)
        assert any(node["nodeType"] == "IndexScan" for node in nodes(document["plan"])), document

    try:
        prefix = "12345678901234567890"
        first, second, absent = [prefix + suffix for suffix in ["-alpha", "-beta", "-missing"]]
        query("CREATE TABLE index_recheck_secondary(id INT PRIMARY KEY,k TEXT);")
        query("INSERT INTO index_recheck_secondary VALUES (1,'" + first + "'),(2,'" + second + "');")
        query("CREATE INDEX index_recheck_k ON index_recheck_secondary(k);")
        for key, count in [(first, 1), (second, 1), (absent, 0)]:
            explain("index_recheck_secondary", key, count)
        assert query("SELECT id,k FROM index_recheck_secondary WHERE k='" + first + "' AND length(k)>0;") == [["1", first]]
        query("CREATE TABLE index_recheck_primary(id INT,k TEXT PRIMARY KEY);")
        query("INSERT INTO index_recheck_primary VALUES (1,'" + first + "');")
        explain("index_recheck_primary", first, 1)
        explain("index_recheck_primary", absent, 0)
        assert query("SELECT 42;") == [["42"]]
        print("[INDEX SCAN FULL VALUE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
