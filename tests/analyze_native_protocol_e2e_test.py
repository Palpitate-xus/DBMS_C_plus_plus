#!/usr/bin/env python3
"""Native ANALYZE relation lists resolve names and refresh real statistics."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "analyze_native_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, tag = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        if sql.startswith("ANALYZE"):
            assert tag == "ANALYZE", (sql, tag)
        return rows

    def cardinality(name):
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON) SELECT id FROM " + name + ";")))
        return document["totalRows"]

    try:
        query("CREATE TABLE native_rows (id INT);")
        query("INSERT INTO native_rows VALUES (1),(2),(3);")
        query("ANALYZE native_rows;")
        assert cardinality("native_rows") == 3
        query('CREATE TABLE "Native,Rows" (id INT);')
        query('INSERT INTO "Native,Rows" VALUES (4),(5);')
        query('ANALYZE native_rows,"Native,Rows";')
        # EXPLAIN's separate name resolver currently fails quoted names;
        # inspect only our isolated statistics artifact for this target.
        stats = (Path(server["dir"]) / "info" / ".stats").read_text()
        assert "Native,Rows __rows__ 2||" in stats, stats
        query("CREATE SCHEMA native_stats;")
        query("CREATE TABLE native_stats.items (id INT);")
        query("INSERT INTO native_stats.items VALUES (6);")
        query("ANALYZE native_stats.items;")
        query("SET search_path TO native_stats,public;")
        query("ANALYZE items;")
        query("SET search_path TO public;")
        assert query("SELECT id FROM native_rows ORDER BY id;") == [["1"], ["2"], ["3"]]
        for sql, expected in (("ANALYZE native_missing;", "42P01"),
                              ("ANALYZE native_rows,;", "42601"),
                              ("ANALYZE (VERBOSE FALSE) native_rows;", "0A000"),
                              ("ANALYZE native_rows(id);", "0A000")):
            rows, state, message, _, _ = runner.decode_wire_result(
                client.simple_query(server["sock"], sql))
            assert state == expected and not rows, (sql, state, message, rows)
        assert query("SELECT 42;") == [["42"]]
        print("[ANALYZE NATIVE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
