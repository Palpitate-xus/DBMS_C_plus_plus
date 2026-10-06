#!/usr/bin/env python3
"""Null-safe ON predicates execute against typed, nullable JOIN ranges."""
import importlib.util
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("null_safe_join_runner", root / "tests/compat/pg_diff_runner.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    server = runner.start_ours(client)

    def query(sql):
        return runner.decode_wire_result(client.simple_query(server["sock"], sql), include_types=True)

    def command(sql):
        result = query(sql)
        assert result[1] is None, (sql, result)

    def expect(sql, rows, types):
        result = query(sql)
        assert result[1] is None and result[0] == rows and result[5] == types, (sql, result)

    def error(sql, state):
        result = query(sql)
        assert result[1] == state and result[0] == [], (sql, result)

    try:
        command("CREATE TABLE null_join_left(id INT,k INT,v TEXT);")
        command("CREATE TABLE null_join_right(id INT,k INT,v TEXT);")
        command("INSERT INTO null_join_left VALUES(1,NULL,NULL),(2,2,''),(3,3,'x'),(4,4,'only-left');")
        command("INSERT INTO null_join_right VALUES(10,NULL,NULL),(20,2,''),(30,3,'x'),(50,5,'only-right');")
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k=r.k ORDER BY l.id,r.id;", [["2", "20"], ["3", "30"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k ORDER BY l.id,r.id;", [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l LEFT JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k ORDER BY l.id,r.id;", [["1", "10"], ["2", "20"], ["3", "30"], ["4", None]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l RIGHT JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k ORDER BY r.id,l.id;", [["1", "10"], ["2", "20"], ["3", "30"], [None, "50"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l LEFT JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k AND r.id=20 ORDER BY l.id,r.id;", [["1", None], ["2", "20"], ["3", None], ["4", None]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l LEFT JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k WHERE r.k IS NULL ORDER BY l.id;", [["1", "10"], ["4", None]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l FULL JOIN null_join_right r ON l.k=r.k AND l.v IS NOT DISTINCT FROM r.v ORDER BY l.id,r.id;", [["1", None], ["2", "20"], ["3", "30"], ["4", None], [None, "10"], [None, "50"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l LEFT JOIN null_join_right r ON l.v IS NOT DISTINCT FROM r.v ORDER BY l.id,r.id;", [["1", "10"], ["2", "20"], ["3", "30"], ["4", None]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.id=2 AND l.k IS DISTINCT FROM r.k ORDER BY r.id;", [["2", "10"], ["2", "30"], ["2", "50"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON CAST(l.k AS TEXT) IS NOT DISTINCT FROM CAST(r.k AS TEXT) ORDER BY l.id;", [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM abs(-r.k) ORDER BY l.id;", [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        command("CREATE FUNCTION null_join_identity(k INT) RETURNS INT STABLE LANGUAGE plpgsql AS $$ BEGIN RETURN k; END; $$;")
        command("CREATE FUNCTION null_join_strict(k INT) RETURNS INT STABLE RETURNS NULL ON NULL INPUT LANGUAGE plpgsql AS $$ BEGIN RETURN k; END; $$;")
        command('CREATE FUNCTION "JoinIdentity"(k INT) RETURNS INT IMMUTABLE LANGUAGE plpgsql AS $$ BEGIN RETURN k; END; $$;')
        for function in ("null_join_identity", "null_join_strict", 'public."JoinIdentity"'):
            expect(f"SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM {function}(r.k) ORDER BY l.id;", [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        command("CREATE FUNCTION null_join_null(k INT) RETURNS INT IMMUTABLE LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$;")
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM null_join_null(r.k) ORDER BY r.id;", [["1", "10"], ["1", "20"], ["1", "30"], ["1", "50"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM CASE WHEN TRUE THEN r.k ELSE CAST('bad' AS INT) END ORDER BY l.id;", [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.id=2 AND l.k IS NOT DISTINCT FROM coalesce(r.k,CAST(NULL AS INT)) ORDER BY r.id;", [["2", "20"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM CASE WHEN r.id=20 THEN 2 ELSE r.k END ORDER BY l.id;", [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON (l.k+1) IS NOT DISTINCT FROM (r.k+1) ORDER BY l.id;", [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k OR (l.id=4 AND r.id=50) ORDER BY l.id;", [["1", "10"], ["2", "20"], ["3", "30"], ["4", "50"]], [23, 23])
        expect("SELECT a.id,b.id FROM null_join_left a JOIN null_join_left b ON a.k IS NOT DISTINCT FROM b.k ORDER BY a.id,b.id;", [["1", "1"], ["2", "2"], ["3", "3"], ["4", "4"]], [23, 23])
        expect("SELECT a.id,b.id,c.id FROM null_join_left a JOIN null_join_left b ON a.k IS NOT DISTINCT FROM b.k JOIN null_join_right c ON b.k IS NOT DISTINCT FROM c.k ORDER BY a.id,b.id,c.id;", [["1", "1", "10"], ["2", "2", "20"], ["3", "3", "30"]], [23, 23, 23])
        expect("SELECT a.id,b.id,c.id FROM null_join_left a LEFT JOIN null_join_left b ON a.k IS NOT DISTINCT FROM b.k LEFT JOIN null_join_right c ON b.k IS NOT DISTINCT FROM c.k ORDER BY a.id,b.id,c.id;", [["1", "1", "10"], ["2", "2", "20"], ["3", "3", "30"], ["4", "4", None]], [23, 23, 23])
        expect("SELECT l.id,r.id,l.k IS NOT DISTINCT FROM r.k AS same FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k ORDER BY l.id;", [["1", "10", "t"], ["2", "20", "t"], ["3", "30", "t"]], [23, 23, 16])
        command('CREATE TABLE null_join_case(id INT,"ID" INT,"a.b" INT);')
        command('INSERT INTO null_join_case VALUES(1,2,3),(2,1,4);')
        expect('SELECT a.id,b.id FROM null_join_case a JOIN null_join_case b ON a."ID" IS NOT DISTINCT FROM b.id ORDER BY a.id;', [["1", "2"], ["2", "1"]], [23, 23])
        expect('SELECT a.id,b.id FROM null_join_case a JOIN null_join_case b ON a."a.b" IS NOT DISTINCT FROM b."a.b" ORDER BY a.id;', [["1", "1"], ["2", "2"]], [23, 23])
        expect('SELECT "A".id,a.id FROM null_join_case "A" JOIN null_join_case a ON "A"."ID" IS NOT DISTINCT FROM a.id ORDER BY "A".id;', [["1", "2"], ["2", "1"]], [23, 23])
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON k IS NOT DISTINCT FROM r.k;", "42702")
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON z.k IS NOT DISTINCT FROM r.k;", "42P01")
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON l.missing IS NOT DISTINCT FROM r.k;", "42703")
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM CAST('bad' AS INT);", "22P02")
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM missing_join_fn(r.k);", "42883")
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM null_join_identity();", "42883")
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM wrong_join_schema.null_join_identity(r.k);", "42883")
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM CASE WHEN TRUE THEN r.k ELSE missing_join_fn(r.k) END;", "42883")
        command("CREATE TABLE null_join_prepare_sink(id INT);")
        command("CREATE FUNCTION null_join_prepare_writer(k INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO null_join_prepare_sink VALUES(k); RETURN k; END; $$;")
        error("SELECT l.id FROM null_join_left l JOIN null_join_right r ON null_join_prepare_writer(l.k) IS NOT DISTINCT FROM missing_join_fn(r.k);", "42883")
        expect("SELECT id FROM null_join_prepare_sink;", [], [23])
        command("CREATE TABLE null_join_full_l(id INT);")
        command("CREATE TABLE null_join_full_r(id INT);")
        command("INSERT INTO null_join_full_l VALUES(1);")
        command("INSERT INTO null_join_full_r VALUES(1);")
        command("CREATE SEQUENCE null_join_full_calls;")
        command("CREATE FUNCTION null_join_full_identity(k INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('null_join_full_calls'); RETURN k; END; $$;")
        expect("SELECT l.id,r.id FROM null_join_full_l l FULL JOIN null_join_full_r r ON l.id=r.id AND l.id IS NOT DISTINCT FROM null_join_full_identity(r.id);", [["1", "1"]], [23, 23])
        expect("SELECT currval('null_join_full_calls');", [["1"]], [20])
        command("INSERT INTO null_join_full_r VALUES(1);")
        expect("SELECT l.id,r.id FROM null_join_full_l l FULL JOIN null_join_full_r r ON l.id=r.id AND l.id IS NOT DISTINCT FROM null_join_full_identity(r.id);", [["1", "1"], ["1", "1"]], [23, 23])
        expect("SELECT currval('null_join_full_calls');", [["3"]], [20])
        expect("SELECT l.id,r.id FROM null_join_full_l l FULL JOIN null_join_full_r r ON l.id=r.id AND l.id IS NOT DISTINCT FROM r.id WHERE l.id=99;", [], [23, 23])
        command("INSERT INTO null_join_full_l VALUES(NULL);")
        command("INSERT INTO null_join_full_r VALUES(NULL);")
        expect("SELECT l.id,r.id FROM null_join_full_l l FULL JOIN null_join_full_r r ON l.id=r.id AND l.id IS NOT DISTINCT FROM r.id ORDER BY l.id,r.id;", [["1", "1"], ["1", "1"], [None, None], [None, None]], [23, 23])
        error("SELECT l.id FROM null_join_left l FULL JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k;", "0A000")
        error("SELECT a.id FROM null_join_left a JOIN null_join_left b ON a.k IS NOT DISTINCT FROM c.k JOIN null_join_right c ON b.k=c.k;", "42P01")
        # A type/cast error must not retain either table's shared lock.
        command("INSERT INTO null_join_left VALUES(6,6,'recovered');")
        expect("SELECT l.id,r.id FROM null_join_left l LEFT JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k WHERE l.id=6;", [["6", None]], [23, 23])
        command("INSERT INTO null_join_right VALUES(11,NULL,NULL);")
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.k IS NOT DISTINCT FROM r.k AND l.id=1 ORDER BY r.id;", [["1", "10"], ["1", "11"]], [23, 23])
        command("INSERT INTO null_join_left VALUES(7,7,'NULL');")
        command("INSERT INTO null_join_right VALUES(70,8,NULL),(71,9,'NULL');")
        expect("SELECT l.id,r.id FROM null_join_left l JOIN null_join_right r ON l.v IS NOT DISTINCT FROM r.v AND l.id=7;", [["7", "71"]], [23, 23])
        print("[NULL SAFE JOIN PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
