#!/usr/bin/env python3
"""Typed ARRAY/|| controls, including real SQL NULL and pre-effect errors."""
import argparse
import importlib.util
from pathlib import Path
import socket

CASES = (
    ('integer arrays', 'ARRAY[1,2] || ARRAY[3,4]', '{1,2,3,4}', 1007),
    ('text arrays', "ARRAY['a','b'] || ARRAY['c']", '{a,b,c}', 1009),
    ('integer constructor', 'ARRAY[1,2]', '{1,2}', 1007),
    ('NULL text is not NULL', "ARRAY['NULL',NULL,'', 'a,b', 'a\\b']", '{"NULL",NULL,"","a,b","a\\\\b"}', 1009),
    ('TEXT brace lookalikes', "'{1,2}'::TEXT || '{3,4}'::TEXT", '{1,2}{3,4}', 25),
    ('left SQL NULL array', 'NULL::INT[] || ARRAY[1]', '{1}', 1007),
    ('right SQL NULL array', 'ARRAY[1] || NULL::INT[]', '{1}', 1007),
    ('two SQL NULL arrays', 'NULL::INT[] || NULL::INT[]', None, 1007),
    ('empty typed array', 'ARRAY[]::INT[] || ARRAY[1]', '{1}', 1007),
    ('two empty typed arrays', 'ARRAY[]::INT[] || ARRAY[]::INT[]', '{}', 1007),
    ('untyped empty', 'ARRAY[] || ARRAY[1]', '42P18', None),
    ('unknown array resolves independently', 'ARRAY[NULL] || ARRAY[1]', '42883', None),
    ('integer bigint common type', 'ARRAY[1] || ARRAY[2::BIGINT]', '{1,2}', 1016),
    ('integer numeric common type', 'ARRAY[1] || ARRAY[2.5]', '{1,2.5}', 1231),
    ('array operand mismatch', "ARRAY[1] || ARRAY['bad']", '42883', None),
    ('unknown element input error', "ARRAY[1,'bad']", '22P02', None),
    ('unknown element coercion', "ARRAY[1,'2']", '{1,2}', 1007),
    ('append typed scalar', 'ARRAY[1] || 2', '{1,2}', 1007),
    ('prepend typed scalar', '0 || ARRAY[1]', '{0,1}', 1007),
    ('append NULL scalar', 'ARRAY[1] || NULL::INT', '{1,NULL}', 1007),
    ('unknown chooses array input', "ARRAY[1] || '2'", '22P02', None),
    ('unknown valid array input', "ARRAY[1] || '{2}'", '{1,2}', 1007),
    ('cast constructor before concatenation', 'ARRAY[1,2]::BIGINT[] || ARRAY[3]', '{1,2,3}', 1016),
    ('nested constructors', 'ARRAY[ARRAY[1,2],ARRAY[3,4]] || ARRAY[ARRAY[5,6]]', '{{1,2},{3,4},{5,6}}', 1007),
    ('operator error before nextval', "ARRAY[nextval('array_concat_seq')] || ARRAY[TRUE]", '42883', None),
    ('input error before nextval', "ARRAY[nextval('array_concat_seq'),'bad']", '22P02', None),
    ('wide integer literal', 'ARRAY[2147483648]', '{2147483648}', 1016),
    ('varchar first common type', "ARRAY['a'::VARCHAR,'b'::TEXT]", '{a,b}', 1015),
    ('text first common type', "ARRAY['b'::TEXT,'a'::VARCHAR]", '{b,a}', 1009),
    ('array-cat extra dimension right', 'ARRAY[1,2] || ARRAY[ARRAY[3,4],ARRAY[5,6]]', '{{1,2},{3,4},{5,6}}', 1007),
    ('array-cat extra dimension left', 'ARRAY[ARRAY[1,2],ARRAY[3,4]] || ARRAY[5,6]', '{{1,2},{3,4},{5,6}}', 1007),
    ('multidimensional scalar append invalid', 'ARRAY[ARRAY[1,2]] || 3', '22000', None),
    ('nested typed empty', 'ARRAY[ARRAY[]::INT[],ARRAY[]::INT[]]', '{}', 1007),
    ('mixed scalar array rejected', 'ARRAY[ARRAY[1],2]', '42804', None),
    ('invalid known explicit cast', "ARRAY[DATE '2026-10-06']::INT[]", '42846', None),
    ('array position metadata','array_position(ARRAY[1,2,3],2)','2',23),
    ('array length metadata','array_length(ARRAY[1,2,3],1)','3',23),
    ('array upper metadata','array_upper(ARRAY[1,2,3],1)','3',23),
    ('array lower metadata','array_lower(ARRAY[1,2,3],1)','1',23),
    ('array cat canonical typed value','array_cat(ARRAY[1],ARRAY[2])','{1,2}',1007),
)


def main():
    p = argparse.ArgumentParser(); p.add_argument('--reference18', action='store_true'); args = p.parse_args()
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('array_concat_runner', root/'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r); c = r.load_protocol_client()
    server = None
    if args.reference18:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=15)
        c.startup_reference(sock,user,database,password=password)
    else:
        server = r.start_ours(c); sock = server['sock']
    def query(sql): return r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql); assert result[1] is None,(sql,result); return result
    try:
        if args.reference18:
            assert ok('SHOW server_version_num;')[0]==[['180006']]
            print('ACTUAL REFERENCE VERSION 180006',flush=True)
        ok('BEGIN;'); ok('CREATE TEMP SEQUENCE array_concat_seq;'); ok("SELECT nextval('array_concat_seq');")
        for label, expression, expected, oid in CASES:
            ok('SAVEPOINT array_control;')
            before=int(ok("SELECT currval('array_concat_seq');")[0][0][0])
            result=query('SELECT '+expression+';')
            ok('ROLLBACK TO array_control;')
            calls=int(ok("SELECT currval('array_concat_seq');")[0][0][0])-before
            if oid is None:
                assert result[1]==expected and not result[0],(label,result,expected)
            else:
                assert result[1] is None and result[0]==[[expected]] and result[5]==[oid],(label,result,expected,oid)
            assert calls==0,(label,'unexpected effect count',calls)
            ok('RELEASE array_control;')
            print('ARRAY CONCAT PASS',label,flush=True)
        for label,expression,state in (
            ('mixed shape empty input','ARRAY[ARRAY[1],2]','42804'),
            ('invalid cast empty input',"ARRAY[DATE '2026-10-06']::INT[]",'42846'),
        ):
            ok('SAVEPOINT array_control;')
            result=query('SELECT '+expression+' WHERE FALSE;')
            ok('ROLLBACK TO array_control;');ok('RELEASE array_control;')
            assert result[1]==state and not result[0],(label,result,state)
            print('ARRAY CONCAT PASS',label,flush=True)
        ok('CREATE TEMP TABLE array_concat_rows(id INT,a INT[],b INT[]);')
        ok("INSERT INTO array_concat_rows VALUES(1,'{1,2}','{3}'),(2,NULL,'{}');")
        for label,sql,rows in (
            ('array columns','SELECT a||b FROM array_concat_rows ORDER BY id;',[['{1,2,3}'],['{}']]),
            ('array column and constructor','SELECT a||ARRAY[4] FROM array_concat_rows ORDER BY id;',[['{1,2,4}'],['{4}']]),
            ('array column empty source','SELECT a||ARRAY[4] FROM array_concat_rows WHERE FALSE;',[]),
        ):
            result=query(sql)
            assert result[1] is None and result[0]==rows and result[5]==[1007],(label,result,rows)
            print('ARRAY CONCAT PASS',label,flush=True)
        ok('ROLLBACK;')
        print('[ARRAY CONCAT TYPED] all',len(CASES)+5,'controls passed',flush=True)
    finally:
        if server:r.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
