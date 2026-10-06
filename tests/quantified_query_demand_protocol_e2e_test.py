#!/usr/bin/env python3
"""Typed quantified-query demand; strict 18.6 oracle, no SQL row-limit rewrite."""
import argparse
import importlib.util
import json
from pathlib import Path
import socket

# Keep the original 30 version-checked oracle expectations intact, including
# scan versus hashed test-expression ordering and nontransactional sequences.
CASES = (
    ('ANY decisive first row', 'SELECT 10>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows);', 1, [['t']], None, [16]),
    ('scan volatile left per RHS row', 'SELECT quant_demand_writer(0)>ANY(SELECT id FROM quant_demand_rows);', 3, [['f']], None, [16]),
    ('scan volatile left empty source', 'SELECT quant_demand_writer(0)>ANY(SELECT id FROM quant_demand_rows WHERE FALSE);', 0, [['f']], None, [16]),
    ('scan volatile left empty ALL', 'SELECT quant_demand_writer(0)>ALL(SELECT id FROM quant_demand_rows WHERE FALSE);', 0, [['t']], None, [16]),
    ('array volatile left empty source', 'SELECT quant_demand_writer(0)>ANY(ARRAY[]::INT[]);', 1, [['f']], None, [16]),
    ('hashed left after RHS construction', "SELECT nextval('quant_demand_sequence')-1=ANY(SELECT nextval('quant_demand_sequence') FROM quant_demand_rows);", 4, [['t']], None, [16]),
    ('equality ANY plan demand', 'SELECT 1=ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows);', 3, [['t']], None, [16]),
    ('equality SOME plan demand', 'SELECT 1=SOME(SELECT quant_demand_writer(id) FROM quant_demand_rows);', 3, [['t']], None, [16]),
    ('correlated equality ANY demand', 'SELECT lhs,lhs=ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows WHERE id<=lhs) FROM(VALUES(1),(2))q(lhs);', 3, [['1','t'],['2','t']], None, [23,16]),
    ('ALL decisive first row', 'SELECT 0>ALL(SELECT quant_demand_writer(id) FROM quant_demand_rows);', 1, [['f']], None, [16]),
    ('ANY reads every row', 'SELECT 0>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows);', 3, [['f']], None, [16]),
    ('ALL reads every row', 'SELECT 10>ALL(SELECT quant_demand_writer(id) FROM quant_demand_rows);', 3, [['t']], None, [16]),
    ('ANY materialized incremental resume', 'SELECT lhs,lhs>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows) FROM(VALUES(10),(0))q(lhs);', 3, [['10','t'],['0','f']], None, [23,16]),
    ('ANY materialized early reuse', 'SELECT lhs,lhs>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows) FROM(VALUES(10),(10))q(lhs);', 1, [['10','t'],['10','t']], None, [23,16]),
    ('ALL materialized early reuse', 'SELECT lhs,lhs>ALL(SELECT quant_demand_writer(id) FROM quant_demand_rows) FROM(VALUES(0),(0))q(lhs);', 1, [['0','f'],['0','f']], None, [23,16]),
    ('ANY NULL left evaluates source', 'SELECT NULL::INT>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows);', 3, [[None]], None, [16]),
    ('ALL NULL left evaluates source', 'SELECT NULL::INT>ALL(SELECT quant_demand_writer(id) FROM quant_demand_rows);', 3, [[None]], None, [16]),
    ('ANY volatile sort full keys', 'SELECT 10>ANY(SELECT id FROM quant_demand_rows ORDER BY quant_demand_writer(id));', 3, [['t']], None, [16]),
    ('ANY non-key projection after sort', 'SELECT 10>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows ORDER BY id);', 1, [['t']], None, [16]),
    ('ANY true avoids later cast', "SELECT 10>ANY(SELECT CAST(CASE WHEN id=1 THEN '1' ELSE 'bad' END AS INT) FROM quant_demand_rows);", 0, [['t']], None, [16]),
    ('ALL false avoids later cast', "SELECT 0>ALL(SELECT CAST(CASE WHEN id=1 THEN '1' ELSE 'bad' END AS INT) FROM quant_demand_rows);", 0, [['f']], None, [16]),
    ('ANY needs later cast', "SELECT 0>ANY(SELECT CAST(CASE WHEN id=1 THEN '1' ELSE 'bad' END AS INT) FROM quant_demand_rows);", 0, [], '22P02', [16]),
    ('array constructor evaluated completely', 'SELECT 10>ANY(ARRAY[quant_demand_writer(1),quant_demand_writer(2),quant_demand_writer(3)]);', 3, [['t']], None, [16]),
    ('array wrong operator prepared before writer', 'SELECT 10>ANY(ARRAY[quant_demand_writer(1)::TEXT]);', 0, [], '42883', []),
    ('multi-column query width', 'SELECT 1=ANY(SELECT id,id FROM quant_demand_rows);', 0, [], '42601', []),
    ('writing CTE before invalid width', "WITH ins AS(INSERT INTO quant_demand_sink VALUES(nextval('quant_demand_sequence')) RETURNING id) SELECT 1=ANY(SELECT id,id FROM quant_demand_rows);", 0, [], '42601', []),
    ('SQL NULL arrays', 'SELECT 1=ANY(NULL::INT[]),1=ALL(NULL::INT[]);', 0, [[None,None]], None, [16,16]),
    ('NULL left empty arrays', 'SELECT NULL::INT=ANY(ARRAY[]::INT[]),NULL::INT=ALL(ARRAY[]::INT[]);', 0, [['f','t']], None, [16,16]),
    ('NULL left empty queries', 'SELECT NULL::INT=ANY(SELECT id FROM quant_demand_rows WHERE FALSE),NULL::INT=ALL(SELECT id FROM quant_demand_rows WHERE FALSE);', 0, [['f','t']], None, [16,16]),
    ('ANY and ALL three-valued arrays', 'SELECT 1=ANY(ARRAY[NULL,1]),1=ALL(ARRAY[NULL,1]),1=ANY(ARRAY[NULL,2]),1=ALL(ARRAY[NULL,2]);', 0, [['t',None,None,'f']], None, [16,16,16,16]),
    ('read CTE scan','WITH q AS(SELECT quant_demand_writer(id) AS v FROM quant_demand_rows) SELECT 10>ANY(SELECT v FROM q);',1,[['t']],None,[16]),
    ('read CTE hash','WITH q AS(SELECT quant_demand_writer(id) AS v FROM quant_demand_rows) SELECT 1=ANY(SELECT v FROM q);',3,[['t']],None,[16]),
    ('writing CTE demand',"WITH ins AS(INSERT INTO quant_demand_sink VALUES(nextval('quant_demand_sequence')) RETURNING id) SELECT 100000>ANY(SELECT id FROM ins);",1,[['t']],None,[16]),
    ('writing CTE LIMIT0 completion',"WITH ins AS(INSERT INTO quant_demand_sink VALUES(nextval('quant_demand_sequence')) RETURNING id) SELECT 100000>ANY(SELECT id FROM ins) LIMIT 0;",1,[],None,[16]),
    ('writing CTE operator error beforeeffect',"WITH ins AS(INSERT INTO quant_demand_sink VALUES(nextval('quant_demand_sequence')) RETURNING id) SELECT 1=ANY(ARRAY['bad']);",0,[],'42883',[]),
    ('two genuine query sites','SELECT 10>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows),10>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows);',2,[['t','t']],None,[16,16]),
    ('derived VALUES query','SELECT 10>ANY(SELECT v FROM(VALUES(1),(2),(3))q(v));',0,[['t']],None,[16]),
    ('unknown query NULL','SELECT 1=ANY(SELECT NULL),1=ALL(SELECT NULL);',0,[],'42883',[]),
    ('unknown query input beforeeffect',"SELECT quant_demand_writer(1)=ANY(SELECT 'bad');",0,[],'42883',[]),
    ('wide integral query','SELECT 2147483648=ANY(SELECT 2147483648);',0,[['t']],None,[16]),
    ('array multidimensional','SELECT 3=ANY(ARRAY[ARRAY[1,2],ARRAY[3,4]]),3>ALL(ARRAY[ARRAY[1,2],ARRAY[3,4]]);',0,[['t','f']],None,[16,16]),
    ('typed TEXT numeric-looking',"SELECT '10'::TEXT>ANY(ARRAY['2'::TEXT]),'10'::TEXT=ANY(ARRAY['10'::TEXT]);",0,[['f','t']],None,[16,16]),
    ('TRUE avoids later strict writer','SELECT 10>ANY(SELECT CASE WHEN id=1 THEN 1 ELSE quant_demand_strict(id) END FROM quant_demand_rows);',0,[['t']],None,[16]),
    ('FALSE avoids later strict writer','SELECT 0>ALL(SELECT CASE WHEN id=1 THEN 1 ELSE quant_demand_strict(id) END FROM quant_demand_rows);',0,[['f']],None,[16]),
    ('needed later strict writer','SELECT 0>ANY(SELECT CASE WHEN id=1 THEN 1 ELSE quant_demand_strict(id) END FROM quant_demand_rows);',1,[],'P0002',[16]),
    ('sort keys need later strict writer','SELECT 10>ANY(SELECT id FROM quant_demand_rows ORDER BY CASE WHEN id=1 THEN 1 ELSE quant_demand_strict(id) END);',1,[],'P0002',[16]),
    ('hash needs later strict writer','SELECT 1=ANY(SELECT CASE WHEN id=1 THEN 1 ELSE quant_demand_strict(id) END FROM quant_demand_rows);',1,[],'P0002',[16]),
    ('int8 float8 equality','SELECT 9007199254740993::BIGINT=ANY(SELECT 9007199254740992::DOUBLE PRECISION);',0,[['t']],None,[16]),
    ('float8 int8 equality','SELECT 9007199254740992::DOUBLE PRECISION=ANY(SELECT 9007199254740993::BIGINT);',0,[['t']],None,[16]),
    ('int8 float8 ordering','SELECT 9007199254740993::BIGINT>ANY(SELECT 9007199254740992::DOUBLE PRECISION);',0,[['f']],None,[16]),
    ('float4 float8 equality','SELECT 1e20::REAL=ANY(SELECT 1e20::DOUBLE PRECISION);',0,[['f']],None,[16]),
    ('float4 float8 ordering','SELECT 1e20::REAL>ANY(SELECT 1e20::DOUBLE PRECISION);',0,[['t']],None,[16]),
    ('int4 float4 equality','SELECT 16777217::INT=ANY(SELECT 16777216::REAL);',0,[['f']],None,[16]),
    ('int4 float4 ordering','SELECT 16777217::INT>ANY(SELECT 16777216::REAL);',0,[['t']],None,[16]),
    ('numeric float8 equality','SELECT 9007199254740993::NUMERIC=ANY(SELECT 9007199254740992::DOUBLE PRECISION);',0,[['t']],None,[16]),
    ('int8 float8 scan equality','SELECT 9007199254740993::BIGINT=ALL(SELECT 9007199254740992::DOUBLE PRECISION);',0,[['t']],None,[16]),
    ('float4 float8 array equality','SELECT 1e20::REAL=ANY(ARRAY[1e20::DOUBLE PRECISION]);',0,[['f']],None,[16]),
    ('char text equality',"SELECT 'x '::CHAR(2)=ANY(SELECT 'x'::TEXT);",0,[['t']],None,[16]),
    ('text char equality',"SELECT 'x'::TEXT=ANY(SELECT 'x '::CHAR(2));",0,[['t']],None,[16]),
    ('text trailing space',"SELECT 'x '::TEXT=ANY(SELECT 'x'::CHAR(1));",0,[['f']],None,[16]),
    ('array explicit collation conflict',"SELECT 'x' COLLATE \"C\"=ANY(ARRAY['x' COLLATE \"POSIX\"]);",0,[],'42P21',[]),
    ('interval month equality',"SELECT INTERVAL '1 mon'=ANY(ARRAY[INTERVAL '30 days']);",0,[['t']],None,[16]),
    ('interval month ordering',"SELECT INTERVAL '1 mon'>ALL(ARRAY[INTERVAL '29 days']);",0,[['t']],None,[16]),
    ('interval hour equality',"SELECT INTERVAL '1 day'=ANY(ARRAY[INTERVAL '24 hours']);",0,[['t']],None,[16]),
    ('unnest query equality','SELECT 2=ANY(SELECT unnest(ARRAY[1,2,3]));',0,[['t']],None,[16]),
    ('unnest query NULL','SELECT 2<>ALL(SELECT unnest(ARRAY[NULL,1]));',0,[[None]],None,[16]),
    ('unnest input evaluated completely','SELECT 10>ANY(SELECT unnest(ARRAY[quant_demand_writer(1),quant_demand_writer(2),quant_demand_writer(3)]));',3,[['t']],None,[16]),
    ('unnest LIMIT0 empty child','SELECT 10>ANY(SELECT unnest(ARRAY[quant_demand_writer(1)]) LIMIT 0);',0,[['f']],None,[16]),
    ('unnest WHERE FALSE empty child','SELECT 10>ANY(SELECT unnest(ARRAY[quant_demand_writer(1)]) WHERE FALSE);',0,[['f']],None,[16]),
    ('unnest typed NULL empty child','SELECT 1=ANY(SELECT unnest(NULL::INT[]));',0,[['f']],None,[16]),
    ('unnest multidimensional','SELECT 3=ANY(SELECT unnest(ARRAY[ARRAY[1,2],ARRAY[3,4]]));',0,[['t']],None,[16]),
    ('unnest actual text NULL',"SELECT 'NULL'=ANY(SELECT unnest(ARRAY['NULL']));",0,[['t']],None,[16]),
    ('shared interval simple CASE',"SELECT CASE INTERVAL '1 mon' WHEN INTERVAL '30 days' THEN 1 ELSE 2 END;",0,[['1']],None,[23]),
    ('constant CASE within left quantifier',"SELECT CASE NULL::INT WHEN 1 THEN 1/0 ELSE 10 END>ANY(SELECT quant_demand_writer(id) FROM quant_demand_rows);",1,[['t']],None,[16]),
    ('constant CASE within array',"SELECT 10>ANY(ARRAY[CASE NULL::INT WHEN 1 THEN 1/0 ELSE quant_demand_writer(1) END]);",1,[['t']],None,[16]),
    ('name array numeric-looking',"SELECT '1'::NAME=ANY(ARRAY['01'::NAME]);",0,[['f']],None,[16]),
    ('name query hash numeric-looking',"SELECT '1'::NAME=ANY(SELECT '01'::NAME);",0,[['f']],None,[16]),
    ('name query scan numeric-looking',"SELECT '1'::NAME=ALL(SELECT '01'::NAME);",0,[['f']],None,[16]),
    ('name array lexical ordering',"SELECT '10'::NAME>ANY(ARRAY['2'::NAME]);",0,[['f']],None,[16]),
    ('name text cross operator',"SELECT '1'::NAME=ANY(SELECT '01'::TEXT);",0,[['f']],None,[16]),
    ('text name cross operator',"SELECT '1'::TEXT=ANY(SELECT '01'::NAME);",0,[['f']],None,[16]),
    ('name implicit C array',"SELECT 'B'::NAME<ANY(ARRAY['a'::NAME]);",0,[['t']],None,[16]),
    ('name implicit C cross text',"SELECT 'B'::NAME<ANY(SELECT 'a'::TEXT);",0,[['t']],None,[16]),
    ('text implicit C cross name',"SELECT 'B'::TEXT<ANY(SELECT 'a'::NAME);",0,[['t']],None,[16]),
)

def main():
    p=argparse.ArgumentParser();p.add_argument('--reference18',action='store_true');args=p.parse_args()
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('quant_demand_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    server=None
    if args.reference18:
        host,port,user,database,password=r._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=15);c.startup_reference(sock,user,database,password=password)
    else:server=r.start_ours(c);sock=server['sock']
    def query(sql):return r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def counter():return int(ok("SELECT currval('quant_demand_sequence');")[0][0][0])
    failures=[]
    try:
        if args.reference18:
            assert ok('SHOW server_version_num;')[0]==[['180006']]
            print('ACTUAL REFERENCE VERSION 180006',flush=True)
        ok('BEGIN;');ok('CREATE TEMP TABLE quant_demand_rows(id INT);')
        ok('INSERT INTO quant_demand_rows VALUES(1),(2),(3);')
        ok('CREATE TEMP TABLE quant_demand_sink(id INT);');ok('CREATE TEMP SEQUENCE quant_demand_sequence;')
        # PG keeps this owned routine in the temporary schema. The candidate
        # fixture uses its established unqualified CREATE FUNCTION consumer;
        # this is not a claim to repair the independent qualified-DDL gap.
        routine=('pg_temp.' if args.reference18 else '')+'quant_demand_writer'
        ok('CREATE FUNCTION '+routine+"(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('quant_demand_sequence'); RETURN arg; END; $$;")
        strict=('pg_temp.' if args.reference18 else '')+'quant_demand_strict'
        ok('CREATE FUNCTION '+strict+"(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE n INT; BEGIN INSERT INTO quant_demand_sink VALUES(arg); PERFORM nextval('quant_demand_sequence'); SELECT id INTO STRICT n FROM quant_demand_rows WHERE id=99; RETURN arg; END; $$;")
        ok("SELECT nextval('quant_demand_sequence');")
        for label,sql,calls,rows,state,oids in CASES:
            if args.reference18:sql=sql.replace('quant_demand_writer(',routine+'(').replace('quant_demand_strict(',strict+'(')
            ok('SAVEPOINT quant_control;');before=counter();result=query(sql)
            ok('ROLLBACK TO quant_control;');actual_calls=counter()-before
            actual=(actual_calls,result[0],result[1],result[5]);expected=(calls,rows,state,oids)
            print('QUANT DEMAND',label,'EXPECTED',expected,'ACTUAL',actual,flush=True)
            if actual!=expected:failures.append((label,actual,expected))
            assert ok('SELECT COUNT(*) FROM quant_demand_sink;')[0]==[['0']]
            ok('RELEASE quant_control;')
        # An ordinary result receiver must still consume all rows.
        ok('SAVEPOINT quant_control;');before=counter()
        result=ok('SELECT '+routine+'(id) FROM quant_demand_rows;')
        assert result[0]==[['1'],['2'],['3']] and result[5]==[23] and counter()-before==3,result
        ok('ROLLBACK TO quant_control;');ok('RELEASE quant_control;')
        # EXPLAIN's published telemetry belongs to the actual demand tree:
        # plain metadata never calls a writer, ANALYZE calls it only for the
        # demanded tuples, and failed executions publish no partial plan.
        for format in ('TEXT','JSON'):
            for analyze in (False,True):
                for op,calls in (('10>',1),('1=',3)):
                    sql='EXPLAIN ('+('ANALYZE,' if analyze else '')+'FORMAT '+format+') SELECT '+op+'ANY(SELECT '+routine+'(id) FROM quant_demand_rows);'
                    ok('SAVEPOINT quant_control;');before=counter();result=query(sql)
                    assert result[1] is None,(sql,result)
                    assert counter()-before==(calls if analyze else 0),(sql,result,counter()-before)
                    text='\n'.join(row[0] for row in result[0])
                    assert text and result[4] is not None,(sql,result)
                    if format=='JSON':
                        plan=json.loads(text)
                        if args.reference18:plan=plan[0]['Plan']
                        if analyze:assert plan['Actual Rows' if args.reference18 else 'actualRows']==1,(sql,plan)
                    elif analyze and not args.reference18:assert 'Actual rows: 1' in text,(sql,text)
                    ok('ROLLBACK TO quant_control;');ok('RELEASE quant_control;')
            for state,child in (
                ('P0002','CASE WHEN id=1 THEN 1 ELSE '+strict+'(id) END'),
                ('22P02',"CAST(CASE WHEN id=1 THEN "+routine+"(1)::TEXT ELSE 'bad' END AS INT)"),
            ):
                ok('SAVEPOINT quant_control;');before=counter()
                sql='EXPLAIN (ANALYZE,FORMAT '+format+') SELECT 0>ANY(SELECT '+child+' FROM quant_demand_rows);'
                result=query(sql)
                assert result[1]==state and result[0]==[] and result[4] is None,(sql,result)
                ok('ROLLBACK TO quant_control;')
                assert counter()-before==1,(sql,counter()-before)
                assert ok('SELECT COUNT(*) FROM quant_demand_sink;')[0]==[['0']]
                ok('RELEASE quant_control;')
        ok('ROLLBACK;')
        print('QUANT DEMAND FAILURES',failures,flush=True)
        assert not failures,failures
        print('[QUANTIFIED QUERY DEMAND] all',len(CASES),'controls and ordinary receiver passed',flush=True)
    finally:
        if server:r.stop_ours(server)
        else:sock.close()

if __name__=='__main__':main()
