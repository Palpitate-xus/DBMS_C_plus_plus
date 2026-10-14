#!/usr/bin/env python3
"""EXPLAIN joins bind two relations and execute a real, instrumented join."""

import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "explain_join_pgdiff", root / "tests" / "compat" / "pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)
    plan_controls = 0
    correlated_controls = 0
    negative_controls = 0

    def query(sql, expected_state=None):
        rows, state, message, _, _ = runner.decode_wire_result(
            client.simple_query(server["sock"], sql))
        assert state == expected_state, (sql, state, message)
        if expected_state:
            assert not rows, (sql, rows)
        return rows

    def nodes(node):
        yield node
        for child in node.get("children", []):
            yield from nodes(child)

    def explain(sql, count, join_count=None, scan_counts=None, source_started=True):
        nonlocal plan_controls
        for _ in range(2):
            plain = json.loads("\n".join(row[0] for row in query(
                "EXPLAIN (FORMAT JSON) " + sql)))
            assert "actualRows" not in plain, plain
            assert any(node["nodeType"].endswith("Join")
                       for node in nodes(plain["plan"])), plain
            plan_controls += 1
            analyzed = json.loads("\n".join(row[0] for row in query(
                "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + sql)))
            assert analyzed["actualRows"] == count, analyzed
            executed = list(nodes(analyzed["plan"]))
            joins = [node for node in executed if node["nodeType"].endswith("Join")]
            expected_join_count = count if join_count is None else join_count
            assert len(joins) == 1, analyzed
            if source_started:
                assert joins[0]["actualRows"] == expected_join_count, analyzed
                assert joins[0]["actualLoops"] > 0, analyzed
            else:
                assert expected_join_count == 0 and "actualRows" not in joins[0] and "actualLoops" not in joins[0], analyzed
            assert len(executed) >= 3, analyzed
            if count:
                assert all(node["actualLoops"] > 0 for node in executed), analyzed
            if scan_counts is not None:
                scans = [node for node in executed if node["nodeType"] == "TableScan"]
                assert len(scans) == len(scan_counts), analyzed
                if source_started:
                    assert sorted(node["actualRows"] for node in scans) == sorted(scan_counts), analyzed
                else:
                    assert scan_counts == [0, 0] and all("actualRows" not in node and "actualLoops" not in node for node in scans), analyzed
            plan_controls += 1
            # An empty outer side legitimately never executes its inner scan.

    try:
        query("CREATE TABLE explain_join_l (k INT,v TEXT);")
        query("CREATE TABLE explain_join_r (k INT,v TEXT);")
        query("INSERT INTO explain_join_l VALUES (1,'a'),(1,'b'),(2,''),(NULL,'NULL');")
        query("INSERT INTO explain_join_r VALUES (1,''),(1,'NULL'),(2,'x'),(NULL,'n');")
        explain("SELECT * FROM explain_join_l JOIN explain_join_r "
                "ON explain_join_l.k=explain_join_r.k;", 5)
        explain("SELECT * FROM public.explain_join_l l INNER JOIN "
                "public.explain_join_r AS r ON r.k=l.k;", 5)
        explain("SELECT * FROM explain_join_l l JOIN explain_join_l r ON l.k=r.k;", 5)
        query('CREATE SCHEMA "Plan Schema";')
        query('CREATE TABLE "Plan Schema"."Left, Rows" ("Key" INT);')
        query('CREATE TABLE "Plan Schema"."Right Rows" ("Key" INT);')
        query('INSERT INTO "Plan Schema"."Left, Rows" VALUES (3),(4);')
        query('INSERT INTO "Plan Schema"."Right Rows" VALUES (4);')
        explain('SELECT * FROM "Plan Schema" . "Left, Rows" AS "L" JOIN '
                '"Plan Schema"."Right Rows" AS "R" ON "L"."Key"="R"."Key";', 1)
        query("CREATE TABLE explain_join_empty (k INT);")
        explain("SELECT * FROM explain_join_l l JOIN explain_join_empty r ON l.k=r.k;", 0)
        # OFFSET is valid and uses the real prepared source tree. The join
        # produces five rows; OFFSET emits only four (or zero), not five.
        explain("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k OFFSET 1;", 4, 5, [4, 4])
        explain("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k OFFSET 5;", 0, 5, [4, 4])
        explain("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k OFFSET 0;", 5, 5, [4, 4])
        explain("SELECT * FROM explain_join_l l JOIN explain_join_l r ON l.k=r.k OFFSET 1;", 4, 5, [4, 4])
        explain("SELECT * FROM explain_join_l l JOIN explain_join_empty r ON l.k=r.k OFFSET 0;", 0, 0, [0, 4])
        explain("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k LIMIT 0 OFFSET 1;", 0, 0, [0, 0], False)
        query("CREATE TABLE explain_join_outer(k INT);")
        query("INSERT INTO explain_join_outer VALUES(1),(2),(9);")
        correlated = ("SELECT o.k,(SELECT r.v FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k "
                      "WHERE l.k=o.k OFFSET 1 LIMIT 1) AS picked FROM explain_join_outer o ORDER BY o.k;")
        # Correlated invocation must reset the same real scan/cache/join
        # positions and caller row, never keep the first invocation's rows.
        for _ in range(2):
            assert query(correlated) == [["1", "NULL"], ["2", None], ["9", None]]
            document = json.loads("\n".join(row[0] for row in query(
                "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + correlated)))
            assert document["actualRows"] == 3, document
            correlated_controls += 2
        for sql, state in (
            ("SELECT * FROM explain_join_l l JOIN explain_join_missing r ON l.k=r.k;", "42P01"),
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.missing=r.k;", "42703"),
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON z.k=r.k;", "42P01"),
            ("SELECT * FROM explain_join_l l JOIN explain_join_r l ON l.k=l.k;", "42712"),
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON explain_join_l.k=r.k;", "42P01"),
            ("SELECT * FROM explain_join_l l LEFT JOIN explain_join_r r ON l.k=r.k;", "0A000"),
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k>r.k;", "0A000"),
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k WHERE l.k=2;", "0A000"),
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k LIMIT 1;", "0A000"),
            ("SELECT l.k FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k;", "0A000"),
            # This is invalid SQL, not an unsupported valid JOIN shape.
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k garbage;", "42601"),
        ):
            query("EXPLAIN (FORMAT JSON,ANALYZE TRUE) " + sql, state)
            negative_controls += 1
        assert query("SELECT 42;") == [["42"]]
        assert (plan_controls, correlated_controls, negative_controls) == (44, 4, 11)
        print("[EXPLAIN JOIN PROTOCOL E2E] passed: 44 plan / 4 correlated / 11 negative / 1 health controls")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
