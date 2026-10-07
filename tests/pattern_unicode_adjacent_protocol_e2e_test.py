#!/usr/bin/env python3
"""SQL pattern classes, repetition, Unicode, and execution-demand controls."""
import importlib.util
import socket
import sys
from pathlib import Path

def main():
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('unicode_adjacent_runner',repo/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host,port,user,database,password = runner._reference_connection_settings()
        sock = socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server = runner.start_ours(client); sock = server['sock']
    cases = [
        ("SELECT 'a.c' SIMILAR TO 'a.c';",None,[['t']]),
        ("SELECT 'a' SIMILAR TO '^a$';",None,[['f']]),
        ("SELECT 'é中' SIMILAR TO '__';",None,[['t']]),
        ("SELECT 'é中' SIMILAR TO '_';",None,[['f']]),
        ("SELECT 'é中' SIMILAR TO '%';",None,[['t']]),
        ("SELECT 'é' SIMILAR TO '[éê]';",None,[['t']]),
        ("SELECT 'ê' SIMILAR TO '[é-ê]';",None,[['t']]),
        ("SELECT 'é' SIMILAR TO '[^é]';",None,[['f']]),
        ("SELECT '中' SIMILAR TO '[[:alpha:]]';",None,[['t']]),
        ("SELECT 'É' SIMILAR TO '[[:upper:]]';",None,[['t']]),
        ("SELECT ']' SIMILAR TO '[]a]';",None,[['t']]),
        ("SELECT '[' SIMILAR TO '[a[b]';",None,[['t']]),
        ("SELECT 'a_' SIMILAR TO '[a[b]_';",None,[['t']]),
        ("SELECT 'aa' SIMILAR TO '[a[b]_';",None,[['f']]),
        ("SELECT 'a%' SIMILAR TO '[a[b]%';",None,[['t']]),
        ("SELECT 'a' SIMILAR TO '[a[b]%';",None,[['f']]),
        ("SELECT 'ab' SIMILAR TO '[a[b].';",None,[['t']]),
        ("SELECT 'a' SIMILAR TO '[a[b]$';",None,[['t']]),
        ("SELECT 'a\"' SIMILAR TO '[a[b]#\"' ESCAPE '#';",None,[['t']]),
        (r"SELECT 'a' SIMILAR TO '[[:<:]]a[[:>:]]';",None,[['t']]),
        (r"SELECT '中' SIMILAR TO '[[:<:]]中[[:>:]]';",None,[['t']]),
        ("SELECT '%' SIMILAR TO '[%_]';",None,[['t']]),
        ("SELECT 'éé' SIMILAR TO '(é|ê){2}';",None,[['t']]),
        ("SELECT 'abc' SIMILAR TO '(ab|a)c';",None,[['t']]),
        ("SELECT '' SIMILAR TO '(a|)';",None,[['t']]),
        ("SELECT 'aaaa' SIMILAR TO 'a{2,4}';",None,[['t']]),
        ("SELECT 'aaaaa' SIMILAR TO 'a{2,4}';",None,[['f']]),
        ("SELECT 'aaaa' SIMILAR TO '(a?)*';",None,[['t']]),
        ("SELECT '' SIMILAR TO '%?';",None,[['t']]),
        ("SELECT 'abbb' SIMILAR TO 'ab+?';",None,[['t']]),
        ("SELECT 'a' SIMILAR TO 'a{0}a';",None,[['t']]),
        ("SELECT '{}' SIMILAR TO '{}';",None,[['t']]),
        (r"SELECT 'a' SIMILAR TO E'a\\';",None,[['t']]),
        (r"SELECT '1' SIMILAR TO '\d';",None,[['t']]),
        (r"SELECT '٣' SIMILAR TO '\d';",None,[['f']]),
        (r"SELECT 'a' SIMILAR TO '\ma\M';",None,[['t']]),
        (r"SELECT 'é' SIMILAR TO '\u00E9';",None,[['t']]),
        (r"SELECT E'\t' SIMILAR TO '\s';",None,[['t']]),
        ("SELECT 'a_' SIMILAR TO 'a中_' ESCAPE '中';",None,[['t']]),
        ("SELECT 'a%' SIMILAR TO 'a中%' ESCAPE '中';",None,[['t']]),
        (r"SELECT 'a\b' SIMILAR TO 'a\b' ESCAPE '';",None,[['t']]),
        (r"SELECT E'a\nb' SIMILAR TO 'a%b';",None,[['t']]),
        (r"SELECT E'\n' SIMILAR TO '[^a]';",None,[['t']]),
        ("SELECT 'abc' SIMILAR TO 'a#\"b#\"c' ESCAPE '#';",None,[['t']]),
        ("SELECT 'aa' SIMILAR TO '#\"a#\"#1' ESCAPE '#';",None,[['t']]),
        ("SELECT 'abb' SIMILAR TO 'a#\"b#\"#1' ESCAPE '#';",None,[['t']]),
        ("SELECT 'ab' SIMILAR TO '#\"a|b#\"#1' ESCAPE '#';",None,[['f']]),
        ("SELECT 'éé' SIMILAR TO '#\"é#\"#1' ESCAPE '#';",None,[['t']]),
        ("SELECT 'a' SIMILAR TO '#\"#\"#\"' ESCAPE '#';",'2200C',[]),
        ("SELECT 'a' SIMILAR TO '[';",'2201B',[]),
        ("SELECT 'a' SIMILAR TO '(';",'2201B',[]),
        ("SELECT 'a' SIMILAR TO '[z-a]';",'2201B',[]),
        ("SELECT 'a' SIMILAR TO '[a-b-c]';",'2201B',[]),
        ("SELECT 'a' SIMILAR TO 'a{3,2}';",'2201B',[]),
        ("SELECT 'a' SIMILAR TO 'a{256}';",'2201B',[]),
        ("SELECT 'a' SIMILAR TO 'a{1x}';",'2201B',[]),
        ("SELECT 'a' SIMILAR TO 'a++';",'2201B',[]),
        (r"SELECT 'a' SIMILAR TO '\q';",'2201B',[]),
        (r"SELECT 'aa' SIMILAR TO '(a)\1';",'2201B',[]),
        (r"SELECT '' SIMILAR TO '\m*';",'2201B',[]),
        (r"SELECT 'a' SIMILAR TO '\A?';",'2201B',[]),
        ("SELECT NULL SIMILAR TO '[';",None,[[None]]),
        ("SELECT NULL SIMILAR TO '[' ESCAPE '#';",None,[[None]]),
        ("SELECT CASE WHEN false THEN 'a' SIMILAR TO '[' ELSE true END;",None,[['t']]),
        ("SELECT 'ẞ' ILIKE 'ß','İ' ILIKE 'i','ΟΣ' ILIKE 'ος','ß' ILIKE 'ss';",None,[['t','t','f','f']]),
        ("SELECT 'É' COLLATE \"C\" ILIKE 'é';",None,[['f']]),
        ("SELECT 'é' COLLATE \"C\" SIMILAR TO '[[:alpha:]]';",None,[['f']]),
        ("SELECT 'É_' ILIKE 'é中_' ESCAPE '中';",None,[['t']]),
        ("SELECT 'É_' ILIKE 'éÉ_' ESCAPE 'É';",None,[['t']]),
        (r"SELECT 'b' LIKE E'a\\';",None,[['f']]),
        (r"SELECT '' LIKE E'%\\';",None,[['f']]),
        (r"SELECT 'a' LIKE E'_\\';",None,[['f']]),
        (r"SELECT 'a' LIKE E'%\\';",'22025',[]),
        (r"SELECT 'a' LIKE E'%_\\';",'22025',[]),
        ("SELECT 'ab' LIKE 'a#' ESCAPE '#';",'22025',[]),
        ("SELECT 'a' LIKE 'a#' ESCAPE '#';",None,[['f']]),
        (r"SELECT CASE WHEN false THEN 'ab' LIKE E'a\\' ELSE true END;",None,[['t']]),
        (r"SELECT NULL NOT LIKE E'a\\';",None,[[None]]),
    ]
    failures = []
    try:
        assert runner.decode_wire_result(client.simple_query(sock,'BEGIN;'))[1] is None
        for sql,state,rows in cases:
            client.simple_query(sock,'SAVEPOINT adjacent_pattern_case;')
            actual = runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
            expected = (rows,state,None if state else [16]*(len(rows[0]) if rows else 1))
            observed = (actual[0],actual[1],None if state else actual[5])
            print('PATTERN_UNICODE_ADJACENT',sql,actual,flush=True)
            if observed != expected: failures.append((sql,observed,expected))
            client.simple_query(sock,'ROLLBACK TO adjacent_pattern_case;')
            client.simple_query(sock,'RELEASE adjacent_pattern_case;')
        print('PATTERN_UNICODE_ADJACENT_FAILURES',failures,flush=True)
        statements = [
            ('CREATE TEMP TABLE unicode_pattern_rows(id INT,v TEXT,flag INT);',[],[], 'CREATE TABLE',None),
            ("INSERT INTO unicode_pattern_rows VALUES(1,'é',0),(2,'É',0),(3,'中',0),(4,'a.c',0),(5,NULL,0);",[],[],'INSERT 0 5',None),
            ("SELECT id,v SIMILAR TO '_',v ILIKE 'é' FROM unicode_pattern_rows ORDER BY id;",
             [['1','t','t'],['2','t','t'],['3','t','f'],['4','f','f'],['5',None,None]], [23,16,16], 'SELECT 5',None),
            ("SELECT id FROM unicode_pattern_rows WHERE v SIMILAR TO '_' ORDER BY id;",[['1'],['2'],['3']],[23],'SELECT 3',None),
            ("UPDATE unicode_pattern_rows SET flag=7 WHERE v ILIKE 'é';",[],[],'UPDATE 2',None),
            ('SELECT id,flag FROM unicode_pattern_rows ORDER BY id;', [['1','7'],['2','7'],['3','0'],['4','0'],['5','0']],[23,23],'SELECT 5',None),
            ("DELETE FROM unicode_pattern_rows WHERE v SIMILAR TO 'a.c' RETURNING id;",[['4']],[23],'DELETE 1',None),
            ('CREATE TEMP TABLE unicode_pattern_demand(id INT,v TEXT,p TEXT);',[],[],'CREATE TABLE',None),
            (r"INSERT INTO unicode_pattern_demand VALUES(1,'a',E'a\\'),(2,'ab',E'a\\'),(3,NULL,E'a\\'),(4,'b',E'a\\');",[],[],'INSERT 0 4',None),
            ('SELECT id,v LIKE p FROM unicode_pattern_demand WHERE id IN(1,3,4) ORDER BY id;', [['1','f'],['3',None],['4','f']],[23,16],'SELECT 3',None),
            ('SELECT v LIKE p FROM unicode_pattern_demand WHERE false;',[],[16],'SELECT 0',None),
            ('SELECT v LIKE p FROM unicode_pattern_demand WHERE id=2;',[],[],None,'22025'),
        ]
        for sql,rows,oids,tag,state in statements:
            client.simple_query(sock,'SAVEPOINT unicode_pattern_statement;')
            actual = runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
            print('PATTERN_UNICODE_STATEMENT',sql,actual,flush=True)
            assert actual[0] == rows and actual[1] == state and actual[4] == tag, (sql,actual,rows,state,tag)
            if state is None: assert actual[5] == oids, (sql,actual,oids)
            else: client.simple_query(sock,'ROLLBACK TO unicode_pattern_statement;')
            client.simple_query(sock,'RELEASE unicode_pattern_statement;')
        assert not failures,failures
    finally:
        try: client.simple_query(sock,'ROLLBACK;')
        finally:
            if server: runner.stop_ours(server)
            else: sock.close()

if __name__ == '__main__': main()
