#!/usr/bin/env python3
"""JSON ANALYZE is one document with real execution counters, not text."""

import importlib.util
import itertools
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "json_analyze_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def expect(sql, state=None):
        rows, actual_state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert actual_state == state, (sql, rows, actual_state, message)
        return "\n".join(row[0] for row in rows)

    def nodes(node):
        yield node
        for child in node.get("children", []):
            yield from nodes(child)

    try:
        expect("CREATE TABLE json_analyze (id INT PRIMARY KEY,value INT);")
        expect("INSERT INTO json_analyze VALUES (1,7),(2,8);")
        for costs, timing, buffers in itertools.product((False, True), repeat=3):
            flag = lambda value: "TRUE" if value else "FALSE"
            sql = ("EXPLAIN (FORMAT JSON, ANALYZE TRUE, COSTS " + flag(costs) +
                   ", TIMING " + flag(timing) + ", BUFFERS " + flag(buffers) +
                   ") SELECT id FROM json_analyze WHERE id=1;")
            for _ in range(2):
                encoded = expect(sql)
                try:
                    document = json.loads(encoded)
                except json.JSONDecodeError as error:
                    raise AssertionError((sql, encoded)) from error
                assert document["actualRows"] == 1, document
                assert ("executionTimeMs" in document) == timing, document
                assert ("totalCost" in document) == costs, document
                assert ("buffers" in document) == buffers, document
                executed = list(nodes(document["plan"]))
                assert any(node.get("nodeType") == "IndexScan" for node in executed), document
                for node in executed:
                    assert node["actualRows"] == 1 and node["actualLoops"] == 2, node
                    assert ("actualTimeMs" in node) == timing, node
                    assert ("cost" in node) == costs, node
        # The failed analyzed access must not return an earlier successful
        # JSON prefix or keep its relation lock after the error.
        primary = Path(server["dir"]) / "info" / "json_analyze.idx"
        assert primary.is_file(), primary
        primary.unlink()
        assert expect("EXPLAIN (FORMAT JSON, ANALYZE TRUE) SELECT id FROM json_analyze WHERE id=1;",
                      "XX001") == ""
        assert not primary.exists(), primary
        expect("REINDEX TABLE json_analyze;")
        assert expect("SELECT id FROM json_analyze WHERE id=1;") == "1"
        assert expect("SELECT 42;") == "42"
        print("[EXPLAIN JSON ANALYZE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
