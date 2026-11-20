#!/usr/bin/env python3
"""EXPLAIN joins bind two relations and execute a real, instrumented join."""

import importlib.util
import json
from collections import Counter
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

    def query(sql, expected_state=None, expected_rows=None, expected_oids=None,
              ordered=False):
        messages = client.simple_query(server["sock"], sql)
        rows, state, message, _, tag, oids = runner.decode_wire_result(
            messages, include_types=True)
        assert messages[-1] == (b"Z", b"I"), (sql, messages[-1])
        assert state == expected_state, (sql, state, message)
        if expected_state:
            assert not rows, (sql, rows)
        if expected_rows is not None:
            if ordered:
                assert rows == expected_rows, (sql, rows, expected_rows)
            else:
                assert Counter(map(tuple, rows)) == Counter(map(tuple, expected_rows)), (sql, rows, expected_rows)
            assert tag == "SELECT " + str(len(expected_rows)), (sql, tag)
        if expected_oids is not None:
            assert oids == expected_oids, (sql, oids, expected_oids)
            assert tag == "SELECT " + str(len(rows)), (sql, tag)
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

        # Normal SQL and EXPLAIN ANALYZE must consume the same admitted JOIN
        # shape. Use bags unless ORDER BY establishes the expected sequence.
        matches = [["1", "a", "1", ""], ["1", "a", "1", "NULL"],
                   ["1", "b", "1", ""], ["1", "b", "1", "NULL"],
                   ["2", "", "2", "x"]]
        left_unmatched = [None, "NULL", None, None]
        right_unmatched = [None, None, None, "n"]
        left_join = "SELECT * FROM explain_join_l l LEFT JOIN explain_join_r r ON l.k=r.k;"
        query(left_join, expected_rows=matches + [left_unmatched], expected_oids=[23, 25, 23, 25])
        explain(left_join, 6)
        where = "SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k WHERE l.k=2;"
        query(where, expected_rows=[["2", "", "2", "x"]], expected_oids=[23, 25, 23, 25])
        explain(where, 1, 5)
        limited = "SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k LIMIT 1;"
        limited_rows = query(limited, expected_oids=[23, 25, 23, 25])
        assert len(limited_rows) == 1 and limited_rows[0] in matches, limited_rows
        explain(limited, 1, 1)
        projected = "SELECT l.k FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k;"
        query(projected, expected_rows=[["1"], ["1"], ["1"], ["1"], ["2"]], expected_oids=[23])
        explain(projected, 5)

        right_join = "SELECT * FROM explain_join_l l RIGHT JOIN explain_join_r r ON l.k=r.k;"
        query(right_join, expected_rows=matches + [right_unmatched], expected_oids=[23, 25, 23, 25])
        explain(right_join, 6)
        full_join = "SELECT * FROM explain_join_l l FULL JOIN explain_join_r r ON l.k=r.k;"
        query(full_join, expected_rows=matches + [left_unmatched, right_unmatched], expected_oids=[23, 25, 23, 25])
        explain(full_join, 7)
        left_empty = "SELECT l.k,l.v,r.k FROM explain_join_l l LEFT JOIN explain_join_empty r ON l.k=r.k;"
        query(left_empty, expected_rows=[["1", "a", None], ["1", "b", None],
                                        ["2", "", None], [None, "NULL", None]], expected_oids=[23, 25, 23])
        explain(left_empty, 4)
        right_empty = "SELECT l.k,r.k,r.v FROM explain_join_empty l RIGHT JOIN explain_join_r r ON l.k=r.k;"
        query(right_empty, expected_rows=[[None, "1", ""], [None, "1", "NULL"],
                                         [None, "2", "x"], [None, None, "n"]], expected_oids=[23, 23, 25])
        explain(right_empty, 4)
        sorted_limit = ("SELECT l.k,l.v,r.v FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k "
                        "ORDER BY l.k,l.v,r.v LIMIT 3;")
        query(sorted_limit, expected_rows=[["1", "a", ""], ["1", "a", "NULL"], ["1", "b", ""]],
              expected_oids=[23, 25, 25], ordered=True)
        explain(sorted_limit, 3, 5)
        derived = ("SELECT p.k,r.v FROM (SELECT k FROM explain_join_l WHERE k=1) p "
                   "JOIN explain_join_r r ON p.k=r.k ORDER BY p.k,r.v;")
        query(derived, expected_rows=[["1", ""], ["1", ""], ["1", "NULL"], ["1", "NULL"]],
              expected_oids=[23, 25], ordered=True)
        explain(derived, 4)
        zero = "SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k LIMIT 0;"
        query(zero, expected_rows=[], expected_oids=[23, 25, 23, 25])
        explain(zero, 0, 0, [0, 0], False)
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
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k>r.k;", "0A000"),
            # This is invalid SQL, not an unsupported valid JOIN shape.
            ("SELECT * FROM explain_join_l l JOIN explain_join_r r ON l.k=r.k garbage;", "42601"),
        ):
            query("EXPLAIN (FORMAT JSON,ANALYZE TRUE) " + sql, state)
            negative_controls += 1
        assert query("SELECT 42;") == [["42"]]
        assert (plan_controls, correlated_controls, negative_controls) == (88, 4, 7)
        print("[EXPLAIN JOIN PROTOCOL E2E] passed: 88 plan / 4 correlated / 7 negative / 1 health controls")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
