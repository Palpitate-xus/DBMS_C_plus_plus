"""Integer JOIN candidate rejection preserves bags, NULLs and outer rows."""
from collections import Counter
import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("integer_join_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    wire = runner.load_protocol_client()
    server = runner.start_ours(wire)
    controls = 0

    def query(sql, expected=None):
        nonlocal controls
        result = runner.decode_wire_result(wire.simple_query(server["sock"], sql), include_types=True)
        assert result[1] is None, (sql, result)
        if expected is not None:
            assert Counter(map(tuple, result[0])) == Counter(map(tuple, expected)), (sql, result, expected)
        controls += 1
        return result[0]

    def plan(sql, filtered):
        document = json.loads("\n".join(row[0] for row in query(
            "EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) " + sql)))
        def nodes(node):
            yield node
            for child in node.get("children", []):
                yield from nodes(child)
        joins = [n for n in nodes(document["plan"]) if n["nodeType"].endswith("Join")]
        assert len(joins) == 1, document
        assert (joins[0].get("integerKeyPrefilter") == "true") == filtered, document
        if not filtered:
            assert "integerKeyLookup" not in joins[0] and "integerKeyLookupBuilt" not in joins[0], document
        return document, joins[0]

    def lookup_plan(sql, *, built, right_min, right_max, mode=None):
        document, join = plan(sql, True)
        assert join.get("integerKeyLookup") == "true", document
        assert (join.get("integerKeyLookupBuilt") == "true") == built, document
        assert join.get("integerKeyLookupMode") == mode, document
        assert len(join.get("children", [])) == 2, document
        right_rows = join["children"][1].get("actualRows", 0)
        assert right_min <= right_rows <= right_max, (sql, right_rows, document)

    try:
        query("CREATE TABLE integer_join_l(id INT,k INT,v TEXT)")
        query("CREATE TABLE integer_join_r(id INT,k INT,v TEXT)")
        query("INSERT INTO integer_join_l VALUES(1,1,NULL),(2,1,'NULL'),(3,NULL,''),(4,-2147483648,'min'),(5,9,'x')")
        query("INSERT INTO integer_join_r VALUES(10,1,''),(11,1,NULL),(12,NULL,'NULL'),(13,-2147483648,'min'),(14,8,'y')")
        matched = [["1", "10"], ["1", "11"], ["2", "10"], ["2", "11"], ["4", "13"]]
        left_only = [["3", None], ["5", None]]
        right_only = [[None, "12"], [None, "14"]]
        for kind, extra in (("INNER", []), ("LEFT", left_only), ("RIGHT", right_only),
                            ("FULL", left_only + right_only)):
            for equality in ("l.k=r.k", "r.k=l.k"):
                query(f"SELECT l.id,r.id FROM integer_join_l l {kind} JOIN integer_join_r r ON {equality}",
                      matched + extra)
                query(f"SELECT count(*) FROM integer_join_l l {kind} JOIN integer_join_r r ON {equality}",
                      [[str(len(matched + extra))]])
        plan("SELECT count(*) FROM integer_join_l l JOIN integer_join_r r ON l.k=r.k", True)
        plan("SELECT count(*) FROM integer_join_l l JOIN integer_join_r r ON r.k=l.k", True)
        # An output alias selects the prepared EXPLAIN graph for LIMIT 1;
        # the legacy EXPLAIN receiver does not admit that shape.
        # A zero-demand consumer must not visit the right side, and the first
        # matching row is found in source order without eagerly building a map.
        lookup_plan("SELECT l.id AS chosen,r.id FROM (SELECT id,k FROM integer_join_l) l JOIN integer_join_r r ON l.k=r.k LIMIT 0",
                    built=False, right_min=0, right_max=0)
        lookup_plan("SELECT l.id AS chosen,r.id FROM (SELECT id,k FROM integer_join_l) l JOIN integer_join_r r ON l.k=r.k LIMIT 1",
                    built=False, right_min=1, right_max=1)
        # Full consumption scans the first right side, then reuses its bounded
        # lookup on subsequent left rows. Five-by-five nested visits are 25.
        lookup_plan("SELECT count(*) FROM integer_join_l l JOIN integer_join_r r ON l.k=r.k",
                    built=True, right_min=5, right_max=24, mode="sorted")
        lookup_plan("SELECT count(*) FROM integer_join_l l JOIN integer_join_r r ON r.k=l.k",
                    built=True, right_min=5, right_max=24, mode="sorted")
        query("SELECT l.id,r.id FROM integer_join_l l JOIN integer_join_r r ON l.k=l.k",
              [[str(l), str(r)] for l in (1, 2, 4, 5) for r in (10, 11, 12, 13, 14)])
        plan("SELECT count(*) FROM integer_join_l l JOIN integer_join_r r ON l.k=l.k", False)
        query("SELECT l.id,r.id FROM integer_join_l l JOIN integer_join_r r ON l.k IS NOT DISTINCT FROM r.k",
              matched + [["3", "12"]])
        query("SELECT count(*) FROM integer_join_l l JOIN integer_join_r r ON l.k=r.k+0", [["5"]])
        plan("SELECT count(*) FROM integer_join_l l JOIN integer_join_r r ON l.k=r.k+0", False)
        query("CREATE TABLE integer_join_big(id BIGINT)")
        query("INSERT INTO integer_join_big VALUES(9007199254740993),(9007199254740992),(-9223372036854775808),(NULL)")
        query("SELECT l.id,r.id FROM integer_join_big l JOIN integer_join_big r ON l.id=r.id",
              [[str(x), str(x)] for x in (9007199254740993, 9007199254740992, -9223372036854775808)])
        query("SELECT count(*) FROM integer_join_big l JOIN integer_join_big r ON l.id=r.id", [["3"]])
        plan("SELECT count(*) FROM integer_join_big l JOIN integer_join_big r ON l.id=r.id", True)
        query("CREATE TABLE integer_join_small(id SMALLINT)")
        query("INSERT INTO integer_join_small VALUES(-32768),(32767),(NULL)")
        query("SELECT l.id,r.id FROM integer_join_small l JOIN integer_join_small r ON l.id=r.id",
              [["-32768", "-32768"], ["32767", "32767"]])
        query("SELECT count(*) FROM integer_join_small l JOIN integer_join_small r ON l.id=r.id", [["2"]])
        plan("SELECT count(*) FROM integer_join_small l JOIN integer_join_small r ON l.id=r.id", True)
        plan("SELECT count(*) FROM integer_join_l l JOIN integer_join_big r ON l.k=r.id", False)
        query("CREATE SEQUENCE integer_join_calls")
        rows = query("SELECT nextval('integer_join_calls') FROM integer_join_l l JOIN integer_join_r r ON l.k=r.k")
        assert rows == [[str(i)] for i in range(1, 6)], rows
        query("SELECT currval('integer_join_calls')", [["5"]])
        query("CREATE TABLE integer_join_empty(id INT)")
        query("SELECT l.id,r.id FROM integer_join_empty l FULL JOIN integer_join_r r ON l.id=r.k",
              [[None, str(x)] for x in (10, 11, 12, 13, 14)])
        query("SELECT l.id,r.id FROM integer_join_l l LEFT JOIN integer_join_empty r ON l.k=r.id",
              [[str(x), None] for x in (1, 2, 3, 4, 5)])
        # Rebind the correlated source for different outer values, including
        # a NULL key, and preserve each subquery's bag count.
        query("SELECT l.id,(SELECT count(*) FROM integer_join_l x JOIN integer_join_r r "
              "ON x.k=r.k WHERE x.id=l.id) FROM integer_join_l l ORDER BY l.id",
              [["1", "2"], ["2", "2"], ["3", "0"], ["4", "1"], ["5", "0"]])
        # The completed right scan can prove a dense direct mapping in either
        # physical direction, including negative, zero and positive values.
        query("CREATE TABLE integer_join_dense_l(k INTEGER)")
        query("INSERT INTO integer_join_dense_l VALUES(-2),(-1),(2),(NULL)")
        for suffix, values, mode in (
            ("ascending", [-2, -1, 0, 1, 2], "direct"),
            ("descending", [2, 1, 0, -1, -2], "direct"),
            ("gap", [-2, -1, 1, 2], "sorted"),
            ("duplicate", [-2, -1, -1, 0, 1, 2], "sorted"),
            ("null", [-2, -1, None, 0, 1, 2], "sorted"),
        ):
            right = "integer_join_dense_" + suffix
            query("CREATE TABLE " + right + "(k INTEGER)")
            literal = lambda value: "NULL" if value is None else str(value)
            query("INSERT INTO " + right + " VALUES" + ",".join("(" + literal(v) + ")" for v in values))
            sql = "SELECT count(*) FROM integer_join_dense_l l JOIN " + right + " r ON l.k=r.k"
            wanted = 4 if suffix == "duplicate" else 3
            query(sql, [[str(wanted)]])
            # The first left row visits every right row. Later non-NULL keys
            # visit each matching ordinal, including both duplicate values.
            visited = len(values) + wanted - 1
            lookup_plan(sql, built=True, right_min=visited, right_max=visited, mode=mode)
        # More than 4096 sparse 16-byte entries exceed the 64 KiB budget.
        # Declining the lookup keeps complete bag semantics and the old scan.
        query("SET work_mem='64kB'")
        query("CREATE TABLE integer_join_budget_l(k INTEGER)")
        query("CREATE TABLE integer_join_budget_r(k INTEGER)")
        query("INSERT INTO integer_join_budget_l VALUES(0),(8192)")
        query("INSERT INTO integer_join_budget_r VALUES" + ",".join("(%d)" % (2 * i) for i in range(4097)))
        budget_join = "SELECT count(*) FROM integer_join_budget_l l JOIN integer_join_budget_r r ON l.k=r.k"
        query(budget_join, [["2"]])
        lookup_plan(budget_join, built=False, right_min=8194, right_max=8194)
        # The same 4097 physical rows become a consecutive sequence through a
        # derived projection. Direct ordinals need no 16-byte-per-row vector.
        query("CREATE TABLE integer_join_dense_budget_l(k INTEGER)")
        query("INSERT INTO integer_join_dense_budget_l VALUES(0),(4096)")
        dense_budget = ("SELECT count(*) FROM integer_join_dense_budget_l l JOIN "
                        "(SELECT k/2 AS k FROM integer_join_budget_r) r ON l.k=r.k")
        query(dense_budget, [["2"]])
        lookup_plan(dense_budget, built=True, right_min=4098, right_max=4098, mode="direct")
        query("RESET work_mem")
        print("[INTEGER JOIN PREFILTER]", controls, "controls passed", flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
