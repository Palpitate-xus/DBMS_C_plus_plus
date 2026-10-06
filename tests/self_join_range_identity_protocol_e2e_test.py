#!/usr/bin/env python3
"""Each FROM occurrence retains its own range identity over shared storage."""

from collections import Counter
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("self_join_pgdiff", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def expect(sql, rows, headers=("id", "id"), types=(23, 23), ordered=False):
        result = query(sql)
        assert result[1] is None, (sql, result)
        if ordered:
            assert result[0] == rows, (sql, result, rows)
        else:
            assert Counter(map(tuple, result[0])) == Counter(map(tuple, rows)), (sql, result, rows)
        assert (result[3], result[4], result[5]) == (list(headers), f"SELECT {len(rows)}", list(types)), (sql, result)

    try:
        for sql in ("CREATE TABLE self_range_rows(id INT,payload TEXT);",
                    "INSERT INTO self_range_rows VALUES(1,'a b'),(2,NULL),(3,'NULL'),(4,'');"):
            assert query(sql)[1] is None, sql
        pairs = [[str(a), str(b)] for a in range(1, 5) for b in range(1, 5)]
        expect("SELECT a.id,b.id FROM self_range_rows a CROSS JOIN self_range_rows b ORDER BY a.id,b.id;",
               pairs, ordered=True)
        expect("SELECT a.id,b.id FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.id=1 AND b.id=2;",
               [["1", "2"]])
        expect("SELECT a.id,b.id FROM self_range_rows a JOIN self_range_rows b ON a.id<b.id;",
               [[str(a), str(b)] for a in range(1, 5) for b in range(a + 1, 5)])
        expect("SELECT sum(a.id),sum(b.id) FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.id=1 AND b.id=2;",
               [["1", "2"]], ("sum", "sum"), (20, 20))
        expect("SELECT b.id,a.id FROM self_range_rows a CROSS JOIN self_range_rows b ORDER BY b.id DESC,a.id;",
               [[str(b), str(a)] for b in range(4, 0, -1) for a in range(1, 5)], ordered=True)
        expect("SELECT a.id+10,b.id+20 FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.id=1 AND b.id=2;",
               [["11", "22"]], ("?column?", "?column?"))
        for kind in ("LEFT", "RIGHT", "FULL"):
            rows = ([[str(a), None] for a in range(1, 5)] if kind != "RIGHT" else [])
            if kind != "LEFT":
                rows += [[None, str(b)] for b in range(1, 5)]
            expect(f"SELECT a.id,b.id FROM self_range_rows a {kind} JOIN self_range_rows b ON false;", rows)
        expect("SELECT a.id,b.id FROM self_range_rows a FULL JOIN self_range_rows b ON a.id=b.id AND a.id=1;",
               [["1", "1"]] + [[str(a), None] for a in range(2, 5)] + [[None, str(b)] for b in range(2, 5)])
        expect("SELECT a.payload,b.payload FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.id=1 AND b.id=2;",
               [["a b", None]], ("payload", "payload"), (25, 25))
        expect("SELECT b.*,a.id FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.id=3 AND b.id=4;",
               [["4", "", "3"]], ("id", "payload", "id"), (23, 25, 23))
        for source in ("self_range_rows", "c"):
            prefix = "WITH c AS(SELECT id,payload FROM self_range_rows) " if source == "c" else ""
            expect(prefix + f"SELECT a.id,b.id FROM {source} a CROSS JOIN {source} b WHERE a.id=1 AND b.id=2;", [["1", "2"]])
            expect(prefix + f"SELECT a.id,b.id,d.id FROM {source} a CROSS JOIN {source} b CROSS JOIN {source} d "
                   "WHERE a.id=1 AND b.id=2 AND d.id=3;", [["1", "2", "3"]], ("id", "id", "id"), (23, 23, 23))
            expect(prefix + f"SELECT a.id,b.id,d.id FROM {source} a JOIN {source} b ON a.id=b.id AND a.id=1 "
                   f"JOIN {source} d ON b.id=d.id;", [["1", "1", "1"]], ("id", "id", "id"), (23, 23, 23))
        expect('SELECT "L".id,"R".id FROM self_range_rows "L" CROSS JOIN self_range_rows "R" '
               'WHERE "L".id=1 AND "R".id=2;', [["1", "2"]])
        expect("SELECT a.id,__join_left.id FROM self_range_rows a CROSS JOIN self_range_rows __join_left "
               "WHERE a.id=1 AND __join_left.id=2;", [["1", "2"]])
        expect("SELECT a.id,b.id FROM self_range_rows a CROSS JOIN self_range_rows b "
               "WHERE a . id=1 AND b . id=2;", [["1", "2"]])
        expect("SELECT self_range_rows.id,b.id FROM public.self_range_rows CROSS JOIN public.self_range_rows b "
               "WHERE self_range_rows.id=1 AND b.id=2;", [["1", "2"]])
        expect("SELECT a.id,b.id,x.n FROM self_range_rows a CROSS JOIN self_range_rows b "
               "CROSS JOIN LATERAL(SELECT a.id+b.id AS n)x WHERE a.id=1 AND b.id=2;",
               [["1", "2", "3"]], ("id", "id", "n"), (23, 23, 23))
        for sql in ("CREATE TABLE self_range_bag(id INT,payload TEXT);",
                    "INSERT INTO self_range_bag VALUES(1,'a b'),(1,NULL),(1,'NULL'),(1,'');"):
            assert query(sql)[1] is None, sql
        payloads = ["a b", None, "NULL", ""]
        expect("SELECT a.payload,b.payload FROM self_range_bag a JOIN self_range_bag b ON a.id=b.id;",
               [[a, b] for a in payloads for b in payloads], ("payload", "payload"), (25, 25))
        for sql in ("CREATE TABLE self_range_literals(id INT,payload TEXT);",
                    "INSERT INTO self_range_literals VALUES(1,'a.id'),(2,'b.id');"):
            assert query(sql)[1] is None, sql
        expect("SELECT a.payload,b.payload FROM self_range_literals a CROSS JOIN self_range_literals b "
               "WHERE a.payload='a.id' AND b.payload=$$b.id$$;",
               [["a.id", "b.id"]], ("payload", "payload"), (25, 25))
        assert query("INSERT INTO self_range_literals VALUES(3,'__join_left.id');")[1] is None
        expect("SELECT a.payload,b.payload FROM self_range_literals a CROSS JOIN self_range_literals b "
               "WHERE a.payload='__join_left.id' AND b.id=2;",
               [["__join_left.id", "b.id"]], ("payload", "payload"), (25, 25))
        expect("SELECT a.payload,b.payload FROM self_range_literals a JOIN self_range_literals b "
               "ON a.payload='__join_left.id' AND b.id=2;",
               [["__join_left.id", "b.id"]], ("payload", "payload"), (25, 25))
        expect("SELECT a.id,b.id FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.payload=NULL;", [])
        expect("SELECT a.id,b.id FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.payload='NULL' AND b.id=2;",
               [["3", "2"]])
        expect("SELECT a.id,b.id FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.id BETWEEN 2 AND 3 AND b.id=1;",
               [["2", "1"], ["3", "1"]])
        expect("SELECT a.id,b.id FROM self_range_rows a CROSS JOIN self_range_rows b WHERE a.payload NOT LIKE 'x%' AND b.id=1;",
               [["1", "1"], ["3", "1"], ["4", "1"]])
        assert query('CREATE TABLE self_range_signed(id INT,"-1" INT);')[1] is None
        assert query('INSERT INTO self_range_signed VALUES(-1,999);')[1] is None
        expect("SELECT a.id,b.id FROM self_range_signed a CROSS JOIN self_range_signed b WHERE a.id=-1 AND b.id=-1;",
               [["-1", "-1"]])
        assert query('CREATE TABLE self_range_quoted(id INT,"MixedId" INT,"a.b" INT);')[1] is None
        assert query('INSERT INTO self_range_quoted VALUES(1,11,111),(2,22,222);')[1] is None
        expect('SELECT "L"."MixedId","R"."MixedId" FROM self_range_quoted "L" CROSS JOIN self_range_quoted "R" '
               'WHERE "L"."id"=1 AND "R"."id"=2;', [["11", "22"]], ("MixedId", "MixedId"))
        expect('SELECT a."a.b",b."a.b" FROM self_range_quoted a CROSS JOIN self_range_quoted b '
               'WHERE a."a.b"=111 AND b."a.b"=222;', [["111", "222"]], ("a.b", "a.b"))
        assert query('CREATE TABLE self_range_delimited(id INT,"Value Name" INT,"a""b" INT,"_6964" INT);')[1] is None
        assert query('INSERT INTO self_range_delimited VALUES(1,11,111,1111),(2,22,222,2222);')[1] is None
        expect('SELECT a."Value Name",b."a""b",a."_6964" FROM self_range_delimited a '
               'CROSS JOIN self_range_delimited b WHERE a."Value Name"=11 AND b."a""b"=222;',
               [["11", "222", "1111"]], ("Value Name", 'a"b', "_6964"), (23, 23, 23))
        expect('SELECT b.*,a.id FROM self_range_delimited a CROSS JOIN self_range_delimited b '
               'WHERE a.id=1 AND b."Value Name"=22;', [["2", "22", "222", "2222", "1"]],
               ("id", "Value Name", 'a"b', "_6964", "id"), (23, 23, 23, 23, 23))
        expect('WITH c AS(SELECT * FROM self_range_delimited) SELECT a.id,b.id,d.id FROM c a '
               'JOIN c b ON a.id=b.id AND a."Value Name"=11 JOIN c d ON b.id=d.id;',
               [["1", "1", "1"]], ("id", "id", "id"), (23, 23, 23))
        assert query("INSERT INTO self_range_literals VALUES(4,'__join_left._6964');")[1] is None
        expect("SELECT a.payload,b.payload FROM self_range_literals a CROSS JOIN self_range_literals b "
               "WHERE a.payload='__join_left._6964' AND b.id=2;",
               [["__join_left._6964", "b.id"]], ("payload", "payload"), (25, 25))
        for sql, state in (
                ("SELECT id FROM self_range_rows a CROSS JOIN self_range_rows b;", "42702"),
                ("SELECT __join_left.id FROM self_range_rows a CROSS JOIN self_range_rows b;", "42P01"),
                ("SELECT __join_left.* FROM self_range_rows a CROSS JOIN self_range_rows b;", "42P01"),
                ("SELECT a.id FROM self_range_rows a CROSS JOIN self_range_rows b WHERE __join_right.id=1;", "42P01"),
                ("SELECT 1 FROM self_range_rows a CROSS JOIN self_range_rows a;", "42712"),
                ("SELECT count(*) FROM self_range_rows CROSS JOIN self_range_rows;", "42712")):
            result = query(sql)
            assert result[1] == state and result[0] == [] and result[4] is None, (sql, result)
        print("[SELF JOIN RANGE IDENTITY PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
