#!/usr/bin/env python3
"""Query filters keep COLLATE labels separate from value bindings."""
import importlib.util
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("join_collation_runner", root / "tests/compat/pg_diff_runner.py")
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
        command('CREATE TABLE jc_l(id INT,t TEXT,"C" TEXT);')
        command('CREATE TABLE jc_r(id INT,t TEXT,"C" TEXT);')
        command("INSERT INTO jc_l VALUES(1,'a','value-left'),(2,NULL,'null-left'),(3,'','empty-left');")
        command("INSERT INTO jc_r VALUES(10,'a','value-right'),(20,NULL,'null-right'),(30,'','empty-right');")
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON l.t IS NOT DISTINCT FROM r.t ORDER BY l.id;', [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON (l.t COLLATE "C") IS NOT DISTINCT FROM (r.t COLLATE "C") ORDER BY l.id;', [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect('SELECT l.id,r.id FROM jc_l l LEFT JOIN jc_r r ON (l.t COLLATE "C") IS NOT DISTINCT FROM (r.t COLLATE "C") AND r.id=10 ORDER BY l.id;', [["1", "10"], ["2", None], ["3", None]], [23, 23])
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON (CASE WHEN TRUE THEN l.t ELSE NULL END COLLATE "C") IS NOT DISTINCT FROM (r.t COLLATE "C") ORDER BY l.id;', [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON (l."C" COLLATE "C") IS DISTINCT FROM (r."C" COLLATE "C") AND l.id=1 ORDER BY r.id;', [["1", "10"], ["1", "20"], ["1", "30"]], [23, 23])
        command("CREATE TABLE jc_empty(id INT,t TEXT);")
        for table in ("jc_l", "jc_empty"):
            sql = f'SELECT l.id FROM {table} l JOIN jc_r r ON (l.t COLLATE "missing_join_collation") IS NOT DISTINCT FROM r.t;'
            error(sql, "42704")
        expect('SELECT l.id,r.id FROM jc_l l JOIN jc_r r ON (l.t COLLATE "C") IS NOT DISTINCT FROM (r.t COLLATE "C") ORDER BY l.id;', [["1", "10"], ["2", "20"], ["3", "30"]], [23, 23])

        expect('SELECT id FROM jc_l WHERE t COLLATE "C" = \'a\';', [["1"]], [23])
        expect('SELECT id FROM jc_l WHERE t < \'a\' COLLATE "C";', [["3"]], [23])
        expect('SELECT id FROM jc_l WHERE t COLLATE pg_catalog."C" = \'a\' COLLATE "C";',
               [["1"]], [23])
        expect('SELECT l.id FROM jc_l AS l WHERE l.t IS NULL OR l.t < \'a\' COLLATE "C" ORDER BY l.id;',
               [["2"], ["3"]], [23])
        for table in ("jc_l", "jc_empty"):
            error(f'SELECT id FROM {table} WHERE t COLLATE unavailable = \'x\';', "42704")
            error(f'SELECT id FROM {table} WHERE t COLLATE "C" = \'x\' COLLATE "POSIX";', "42P21")
            error(f'SELECT id FROM {table} WHERE id COLLATE "C" = 1;', "42804")
        error('SELECT id FROM jc_l WHERE FALSE AND t COLLATE unavailable = \'x\' LIMIT 0;', "42704")
        error('SELECT id FROM jc_l WHERE t COLLATE C = \'x\';', "42704")

        command('CREATE SCHEMA jc_rules;')
        command('CREATE COLLATION jc_rules.local_c (provider = libc, locale = \'C\');')
        command('CREATE TABLE jc_declared(id INT, t TEXT COLLATE "POSIX");')
        command("INSERT INTO jc_declared VALUES(1,'a'),(2,'A'),(3,NULL);")
        expect('SELECT id FROM jc_declared WHERE t < \'a\' COLLATE jc_rules.local_c;', [["2"]], [23])
        expect('SELECT id FROM jc_declared WHERE COALESCE(t, \'a\' COLLATE "C") = \'a\' ORDER BY id;',
               [["1"], ["3"]], [23])
        expect('SELECT id FROM jc_declared WHERE (t || \'z\' COLLATE "C") < \'az\';', [["2"]], [23])
        expect('SELECT id FROM jc_declared WHERE GREATEST(t, \'a\' COLLATE "C") = \'a\' ORDER BY id;',
               [["1"], ["2"], ["3"]], [23])
        expect('SELECT id FROM jc_declared WHERE LEAST(t, \'a\' COLLATE "C") = \'A\';', [["2"]], [23])
        expect('SELECT id FROM jc_declared WHERE NULLIF(t, \'a\' COLLATE "C") IS NULL ORDER BY id;',
               [["1"], ["3"]], [23])
        expect('SELECT id FROM jc_l WHERE t COLLATE "C" = \'a\' FOR UPDATE;', [["1"]], [23])
        error('SELECT id FROM jc_l WHERE t COLLATE C = \'a\' FOR UPDATE;', "42704")
        error('SELECT id FROM jc_declared WHERE t COLLATE jc_rules.local_c = \'a\' COLLATE "C";', "42P21")
        result = query('EXPLAIN (ANALYZE, FORMAT JSON, TIMING FALSE) '
                       'SELECT id FROM jc_declared WHERE t < \'a\' COLLATE jc_rules.local_c;')
        assert result[1] is None and result[5] == [114], result
        assert json.loads("\n".join(row[0] for row in result[0]))["actualRows"] == 1
        command('SET search_path TO jc_rules, public;')
        expect('SELECT COALESCE(t COLLATE jc_rules.local_c, \'a\' COLLATE local_c) AS label '
               'FROM jc_declared ORDER BY id LIMIT 3;', [["a"], ["A"], ["a"]], [25])
        expect('SELECT CASE WHEN id <> 3 THEN t COLLATE jc_rules.local_c ELSE \'a\' COLLATE local_c END '
               'AS label FROM jc_declared ORDER BY id LIMIT 3;', [["a"], ["A"], ["a"]], [25])
        command("INSERT INTO jc_declared VALUES(4,'B');")
        expect('SELECT id FROM jc_declared WHERE id = 4 AND '
               'GREATEST(CASE WHEN TRUE THEN t ELSE \'unused\' COLLATE nocase END, \'a\') = \'a\';', [], [23])
        expect('SELECT id FROM jc_declared WHERE id = 4 AND '
               'GREATEST(COALESCE(t, \'z\', \'unused\' COLLATE nocase), \'a\') = \'a\';', [], [23])
        command('CREATE TABLE jc_varying(id INT, t VARCHAR(8));')
        command("INSERT INTO jc_varying VALUES(1,'01'),(2,'1'),(3,'a '),(4,'a'),(5,NULL);")
        expect('SELECT id FROM jc_varying WHERE t COLLATE "C" = \'a \';', [["3"]], [23])
        expect('SELECT id FROM jc_varying WHERE \'01\' COLLATE "C" <> t ORDER BY id;',
               [["2"], ["3"], ["4"]], [23])
        # Implicitly conflicting collations are reported only when a
        # collation-sensitive operation actually evaluates a non-NULL pair.
        command("CREATE COLLATION jc_rules.local_c2 (provider = libc, locale = 'C');")
        command('CREATE TABLE jc_implicit(id INT, c TEXT COLLATE "C", p TEXT COLLATE "POSIX", '
                'd TEXT, a TEXT COLLATE jc_rules.local_c, b TEXT COLLATE jc_rules.local_c2, '
                'v VARCHAR(10) COLLATE jc_rules.local_c, w VARCHAR(10) COLLATE jc_rules.local_c2, '
                'h CHAR(5) COLLATE jc_rules.local_c, j CHAR(5) COLLATE jc_rules.local_c2);')
        command('CREATE TABLE jc_implicit_empty(id INT, c TEXT COLLATE "C", p TEXT COLLATE "POSIX");')
        command('CREATE TABLE jc_implicit_null(id INT, c TEXT COLLATE "C", p TEXT COLLATE "POSIX");')
        command("INSERT INTO jc_implicit VALUES"
                "(1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),"
                "(2,'a','A','A','a','A','a','A','a','A'),"
                "(3,'same','same','same','same','same','same','same','same','same');")
        command("INSERT INTO jc_implicit_null VALUES(1,NULL,NULL),(2,NULL,'x');")

        def expect_wire(sql, rows, state, kinds):
            frames = client.simple_query(server["sock"], sql)
            result = runner.decode_wire_result(frames, include_types=True)
            assert result[0] == rows and result[1] == state and result[5] == [23], (sql, result)
            assert [kind.decode("ascii") for kind, _ in frames] == kinds, (sql, frames)
            assert frames[-1] == (b"Z", b"I"), (sql, frames)
            if state:
                assert result[4] is None, (sql, result)

        for predicate in ("c < p", "p < c", "c = p", "a < b", "b = a",
                          "v = w", "w < v", "h = j", "j < h"):
            expect_wire(f"SELECT id FROM jc_implicit WHERE {predicate};", [], "42P22", ["T", "E", "Z"])
        expect_wire('SELECT id FROM jc_implicit_empty WHERE c < p;', [], None, ["T", "C", "Z"])
        expect_wire('SELECT id FROM jc_implicit_null WHERE c < p;', [], None, ["T", "C", "Z"])
        expect_wire('SELECT id FROM jc_implicit WHERE c < p LIMIT 0;', [], None, ["T", "C", "Z"])
        expect_wire('SELECT id FROM jc_implicit WHERE FALSE AND c < p;', [], None, ["T", "C", "Z"])
        expect_wire('SELECT id FROM jc_implicit WHERE id=1 OR c < p LIMIT 1;', [["1"]], None,
                    ["T", "D", "C", "Z"])
        expect_wire('SELECT id FROM jc_implicit WHERE id=1 OR c < p;', [["1"]], "42P22",
                    ["T", "D", "E", "Z"])
        expect_wire('SELECT id FROM jc_implicit WHERE id=1 OR c < p LIMIT 2;', [["1"]], "42P22",
                    ["T", "D", "E", "Z"])
        expect_wire('SELECT id FROM jc_implicit WHERE CASE c WHEN NULL::text THEN 0 '
                    'WHEN p THEN 1 ELSE 0 END = 1;', [], "42P22", ["T", "E", "Z"])
        expect_wire('SELECT id FROM jc_implicit WHERE CASE c COLLATE "C" WHEN p THEN 1 '
                    'ELSE 0 END = 1;', [], "42P22", ["T", "E", "Z"])
        expect('SELECT id FROM jc_implicit WHERE CASE c WHEN p COLLATE "C" THEN 1 '
               'ELSE 0 END = 1 ORDER BY id;', [["3"]], [23])
        expect('SELECT id FROM jc_implicit WHERE c COLLATE pg_catalog."C" = c COLLATE "C" ORDER BY id;',
               [["2"], ["3"]], [23])
        expect('SELECT id FROM jc_implicit WHERE c = d ORDER BY id;', [["3"]], [23])
        expect('SELECT id FROM jc_implicit WHERE a COLLATE jc_rules.local_c = b ORDER BY id;',
               [["3"]], [23])
        error('SELECT id FROM jc_implicit WHERE a COLLATE jc_rules.local_c = '
              'b COLLATE jc_rules.local_c2;', "42P21")
        command('CREATE TABLE jc_declared_default(id INT, a TEXT COLLATE "C", '
                'd TEXT COLLATE "default");')
        command("INSERT INTO jc_declared_default VALUES (1,'A','a'),(2,'a','A');")
        expect('SELECT id FROM jc_declared_default WHERE a < d ORDER BY id;', [["1"]], [23])

        print("[JOIN COLLATION ROLE PROTOCOL E2E] passed")
    finally:
        runner.stop_ours(server)


if __name__ == "__main__":
    main()
