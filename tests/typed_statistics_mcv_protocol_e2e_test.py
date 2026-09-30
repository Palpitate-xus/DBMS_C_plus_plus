#!/usr/bin/env python3
"""Typed numeric distinct/MCV and equivalent literal selectivity."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "typed_mcv_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state is None, (sql, state, message)
        return rows

    def expect_filter(document):
        nodes = [document["plan"]]
        filters = []
        while nodes:
            node = nodes.pop()
            if node.get("nodeType") == "Filter":
                filters.append(node)
            nodes.extend(node.get("children", []))
        assert len(filters) == 1 and filters[0]["rows"] == 35, document

    try:
        query("CREATE TABLE numeric_mcv (id INT PRIMARY KEY,v NUMERIC);")
        query("INSERT INTO numeric_mcv VALUES " + ",".join(
            f"({i}," + ("0.50" if i < 20 else "0.500" if i < 35 else "2.00") + ")"
            for i in range(40)) + ";")
        query("ANALYZE numeric_mcv;")
        stats = (Path(server["dir"]) / "info" / ".stats").read_text().splitlines()
        line = next(line for line in stats if line.startswith("numeric_mcv v "))
        fields = line.split("|")
        assert fields[0] == "numeric_mcv v 2", line
        assert fields[4].split(";")[0].startswith("35:"), line
        for probe in ("0.5", "0.50", "0.5000"):
            document = json.loads("\n".join(row[0] for row in query(
                f"EXPLAIN (FORMAT JSON) SELECT id FROM numeric_mcv WHERE v={probe};")))
            expect_filter(document)
            assert query(f"SELECT id FROM numeric_mcv WHERE v={probe} ORDER BY id;") == [
                [str(i)] for i in range(35)]
        query("CREATE TABLE float_mcv (id INT PRIMARY KEY,v DOUBLE PRECISION);")
        query("INSERT INTO float_mcv VALUES " + ",".join(
            f"({i}," + ("1.25" if i < 35 else "2.25") + ")" for i in range(40)) + ";")
        query("ANALYZE float_mcv;")
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON) SELECT id FROM float_mcv WHERE v=1.2500;")))
        expect_filter(document)
        assert query("SELECT 42;") == [["42"]]
        print("[TYPED STATISTICS MCV PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
