#!/usr/bin/env python3
"""Strict oracle for the native/storage pattern consumers' exact datums.

This wire matrix does not claim a forced legacy SELECT switch. The companion
native fixture calls query/queryExpr/compareValues/bitmap/JOIN/DML directly.
"""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('legacy_pattern_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server['sock']
    cases = []
    def ids(predicate, expected):
        cases.append((f'SELECT id FROM legacy_pattern_values WHERE {predicate} ORDER BY id;', None,
                      [[str(value)] for value in expected], [23]))
    for predicate, expected in [
        ("v LIKE '_'",[1,2]),("v NOT LIKE '_'",[3,4,5,7,8,9,10]),
        ("v ILIKE 'é'",[1,2]),("v NOT ILIKE 'é'",[3,4,5,7,8,9,10]),
        ("v SIMILAR TO '_'",[1,2]),("v NOT SIMILAR TO '_'",[3,4,5,7,8,9,10]),
        ("v SIMILAR TO 'a.c'",[3]),("v LIKE 'NULL'",[4]),("v LIKE ''",[5]),
        ("v LIKE 'p'",[]),("v NOT LIKE 'a b'",[1,2,3,4,5,7,9,10]),
        ("b LIKE '__'::bytea",[1,2,7]),("c LIKE 'a'",[]),
        ("c LIKE 'a__'",[1,2,3,4,7,8,9,10]),("v LIKE NULL",[]),
        ("v SIMILAR TO 'a_b'",[8,9,10]),("v SIMILAR TO '(é|É)'",[1,2]),
        ("v SIMILAR TO '[éÉ]'",[1,2]),("v LIKE 'a# b' ESCAPE '#'",[8]),
        ("v LIKE '_' ESCAPE NULL",[]),("v LIKE NULL ESCAPE 'xx'",[]),
        ("c LIKE 'a'::char(3)",[]),
        ("v LIKE 'a# b' ESCAPE '#'::char(3)",[8]),
    ]:
        ids(predicate,expected)
    for predicate, state in [
        ("b ILIKE '%'",'42883'),("b SIMILAR TO '%'",'42883'),("id LIKE '%'",'42883'),
        ("v LIKE '_' ESCAPE 'xx'",'22025'),("v SIMILAR TO '['",'2201B'),
        ("v LIKE p ESCAPE id",'42883'),("b LIKE '_'::text",'42883'),
    ]:
        cases.append((f'SELECT id FROM legacy_pattern_values WHERE {predicate};',state,[],None))
    for sql,value in [
        ("SELECT 'a  '::char(3) LIKE 'a  '::char(3);",'f'),
        ("SELECT 'a'::text LIKE 'a  '::char(3);",'t'),
        ("SELECT 'a  '::char(3) ILIKE 'a  '::char(3);",'f'),
        ("SELECT 'a'::char(3) SIMILAR TO 'a  '::char(3);",'f'),
        ("SELECT 'a_' LIKE 'aé_' ESCAPE 'é';",'t'),
        ("SELECT 'a_' LIKE 'aé_' ESCAPE NULL;",None),
        ("SELECT NULL::text LIKE NULL ESCAPE 'xx';",None),
        ("SELECT 'a' LIKE E'a\\\\';",'f'),
        ("SELECT NULL::text LIKE E'a\\\\';",None),
        ("SELECT E'a\\\\' LIKE E'a\\\\' ESCAPE '';",'t'),
        ("SELECT E'\\x01a' LIKE E'\\x01_';",'t'),
        ("SELECT 'É' COLLATE \"C\" ILIKE 'é';",'f'),
        ("SELECT 'É' COLLATE \"C.utf8\" ILIKE 'é';",'t'),
        ("SELECT 'Z' COLLATE \"C.utf8\" < 'a';",'t'),
        ("SELECT 'é' COLLATE \"C.utf8\" > 'z';",'t'),
        ("SELECT 'é' COLLATE \"C.utf8\" SIMILAR TO '[[:alpha:]]';",'t'),
        ("SELECT 'é' COLLATE \"C\" SIMILAR TO '[[:alpha:]]';",'f'),
    ]:
        cases.append((sql,None,[[value]],[16]))
    for sql,state in [
        ("SELECT 'ab' LIKE E'a\\\\';",'22025'),
        ("SELECT NULL::text LIKE '%' ESCAPE 'xx';",'22025'),
        ("SELECT NULL::integer LIKE '%';",'42883'),
        ("SELECT id FROM legacy_pattern_empty WHERE id LIKE '%';",'42883'),
        ("SELECT v FROM legacy_pattern_empty WHERE v LIKE '_' ESCAPE 'xx';",'22025'),
    ]:
        cases.append((sql,state,[],None))
    joined = [[str(value),str(value)] for value in [1,3,4,5,7,8,9,10]]
    for kind in ['JOIN','LEFT JOIN','RIGHT JOIN','FULL JOIN']:
        cases.append((f'SELECT l.id,r.id FROM legacy_pattern_values l {kind} legacy_pattern_rhs r ON l.id=r.id WHERE l.v LIKE r.p ORDER BY l.id,r.id;',None,joined,[23,23]))
    cases.append(('SELECT l.id,r.id FROM legacy_pattern_values l CROSS JOIN legacy_pattern_rhs r WHERE l.id=r.id AND l.v LIKE r.p ORDER BY l.id,r.id;',None,joined,[23,23]))
    cases.append(('SELECT l.id,r.id FROM legacy_pattern_values l LEFT JOIN legacy_pattern_rhs r ON l.id=r.id AND l.v ILIKE r.p ORDER BY l.id;',None,
                  [[str(value),None if value==6 else str(value)] for value in range(1,11)],[23,23]))
    cases.append(("SELECT l.id FROM legacy_pattern_values l JOIN legacy_pattern_rhs r ON l.id=r.id WHERE l.v LIKE 'aé b' ESCAPE r.escape_value;",None,[['8']],[23]))
    cases.append(("SELECT l.id,r.id FROM legacy_pattern_values l LEFT JOIN legacy_pattern_rhs r ON l.id=r.id AND l.v SIMILAR TO 'aé b' ESCAPE r.escape_value ORDER BY l.id;",None,
                  [[str(value),'8' if value==8 else None] for value in range(1,11)],[23,23]))
    cases.extend([
        ("UPDATE legacy_pattern_values SET p='changed' WHERE v ILIKE 'é' RETURNING id;",None, [['1'],['2']],[23]),
        ("DELETE FROM legacy_pattern_values WHERE v SIMILAR TO 'a.c' RETURNING id;",None,[['3']],[23]),
    ])
    setup = [
        'CREATE TABLE legacy_pattern_values(id INT PRIMARY KEY,v TEXT,p TEXT,b BYTEA,c CHAR(3));',
        "INSERT INTO legacy_pattern_values VALUES (1,'é','_','\\xc3a9','a'),(2,'É','_','\\xc3a9','a'),(3,'a.c','_','\\x','a'),(4,'NULL','_','\\x','a'),(5,'','_','\\x',''),(6,NULL,NULL,NULL,NULL),(7,'ab',E'a\\\\','\\x6162','a'),(8,'a b','_','\\x','a'),(9,'a''b','_','\\x','a'),(10,E'a\\nb','_','\\x','a');",
        'CREATE TABLE legacy_pattern_rhs(id INT PRIMARY KEY,p TEXT,escape_value TEXT);',
        "INSERT INTO legacy_pattern_rhs VALUES (1,'_','é'),(2,'é','é'),(3,'a.c','é'),(4,'NULL','é'),(5,'','é'),(6,NULL,'é'),(7,'ab','é'),(8,'a_b','é'),(9,'a''b','é'),(10,'a_b','é');",
        'CREATE TABLE legacy_pattern_empty(id INT,v TEXT);',
        'CREATE INDEX legacy_pattern_v_idx ON legacy_pattern_values(v);',
    ]
    failures=[]
    try:
        assert runner.decode_wire_result(client.simple_query(sock,'BEGIN;'))[1] is None
        for sql in setup:
            result=runner.decode_wire_result(client.simple_query(sock,sql))
            print('LEGACY_PATTERN_SETUP',sql,result,flush=True)
            assert result[1] is None,result
        for sql,state,rows,types in cases:
            assert runner.decode_wire_result(client.simple_query(sock,'SAVEPOINT legacy_pattern_case;'))[1] is None
            actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
            observed=(actual[0],actual[1],None if state else actual[5])
            expected=(rows,state,types)
            print('LEGACY_PATTERN_CASE',sql,actual,flush=True)
            if observed!=expected: failures.append((sql,observed,expected))
            assert runner.decode_wire_result(client.simple_query(sock,'ROLLBACK TO legacy_pattern_case;'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock,'RELEASE legacy_pattern_case;'))[1] is None
        print('LEGACY_PATTERN_FAILURES',failures,flush=True)
        assert not failures,failures
    finally:
        try: client.simple_query(sock,'ROLLBACK;')
        finally:
            if server: runner.stop_ours(server)
            else: sock.close()


if __name__=='__main__': main()
