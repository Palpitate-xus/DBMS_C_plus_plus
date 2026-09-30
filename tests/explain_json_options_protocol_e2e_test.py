#!/usr/bin/env python3
"""Every COSTS/SETTINGS/BUFFERS combination must produce valid JSON."""

import importlib.util
import itertools
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "explain_options_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
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
        expect("CREATE TABLE explain_options (id INT);")
        expect("INSERT INTO explain_options VALUES (1),(2);")
        for costs, settings, buffers in itertools.product((False, True), repeat=3):
            flag = lambda value: "TRUE" if value else "FALSE"
            sql = ("EXPLAIN (FORMAT JSON, COSTS " + flag(costs) +
                   ", SETTINGS " + flag(settings) + ", BUFFERS " + flag(buffers) +
                   ") SELECT id FROM explain_options;")
            encoded = "\n".join(row[0] for row in expect(sql))
            try:
                document = json.loads(encoded)
            except json.JSONDecodeError as error:
                raise AssertionError((sql, encoded)) from error
            assert ("totalCost" in document) == costs, (sql, document)
            assert ("totalRows" in document) == costs, (sql, document)
            assert ("settings" in document) == settings, (sql, document)
            assert ("buffers" in document) == buffers, (sql, document)
            for node in nodes(document["plan"]):
                assert ("cost" in node) == costs, (sql, node)
                assert ("rows" in node) == costs, (sql, node)
            assert any(node.get("nodeType") == "TableScan"
                       for node in nodes(document["plan"])), document
        assert expect("SELECT id FROM explain_options;") == [["1"], ["2"]]
        assert expect("SELECT 42;") == [["42"]]
        print("[EXPLAIN JSON OPTIONS PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
