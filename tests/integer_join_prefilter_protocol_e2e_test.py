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
        print("[INTEGER JOIN PREFILTER]", controls, "controls passed", flush=True)
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
