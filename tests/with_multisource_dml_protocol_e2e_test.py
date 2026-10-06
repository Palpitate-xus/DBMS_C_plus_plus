#!/usr/bin/env python3
"""Retained WITH primary UPDATE FROM / DELETE USING, strict PG18 reference."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('multi_dml_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password);server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql,ready=None):
        messages=client.simple_query(server['sock'],sql)
        result=runner.decode_wire_result(messages,include_types=True)
        print('WITH_MULTI_DML',sql,result,flush=True)
        if ready is not None and messages[-1]!=(b'Z',ready):failures.append(('ready',sql,messages[-1],ready))
        return result
    def setup(sql):
        result=query(sql);assert result[1] is None,(sql,result)
    def ordered(rows):return sorted(rows,key=lambda row:tuple(str(cell) for cell in row))
    def check(label,sql,rows=None,state=None,tag=None,types=None,ready=None):
        result=query(sql,ready);good=result[1]==state
        if rows is not None:good=good and ordered(result[0])==ordered(rows)
        if tag is not None:good=good and result[4]==tag
        if types is not None:good=good and result[5]==types
        if state:good=good and not result[0] and result[4] is None
        if not good:failures.append((label,rows,state,tag,types,result))
        print('WITH_MULTI_CONTROL',label,'PASS' if good else 'FAIL',flush=True)
        return result
    def reset():
        for table in ('multi_target','multi_source','multi_extra','multi_audit'):setup('DELETE FROM '+table)
        setup("INSERT INTO multi_target VALUES(1,1,101,'old1'),(2,2,102,'old2'),(3,3,103,'old3'),(4,4,104,'old4')")
        setup("INSERT INTO multi_source VALUES(1,10,NULL),(2,20,''),(3,NULL,'NULL')")
        setup('INSERT INTO multi_extra VALUES(1,7),(2,NULL),(4,9)')
    try:
        if reference:runner.verify_reference_version(client,sock)
        for sql in ('CREATE TEMP TABLE multi_target(id INT PRIMARY KEY,a INT,b INT,t TEXT)',
                    'CREATE TEMP TABLE multi_source(id INT,a INT,t TEXT)',
                    'CREATE TEMP TABLE multi_extra(id INT,f INT)',
                    'CREATE TEMP TABLE multi_audit(id INT)',
                    'CREATE TEMP TABLE multi_duplicate(id INT,a INT,t TEXT)',
                    'CREATE TEMP SEQUENCE multi_set_seq','CREATE TEMP SEQUENCE multi_tuple_seq',
                    'CREATE TEMP SEQUENCE multi_static_seq','CREATE TEMP SEQUENCE multi_late_seq'):
            setup(sql)
        reset()
        check('physical-target-logical-name-shadow',
              'WITH multi_target AS(SELECT 999 AS id) UPDATE multi_target AS t SET a=s.a,b=t.a FROM multi_source AS s '
              'WHERE t.id=s.id AND t.id=1 RETURNING t.id,t.a,t.b,s.a,s.t',
              [['1','10','1','10',None]],tag='UPDATE 1',types=[23,23,23,23,25])
        reset()
        check('nullable-source-cells-and-source-returning',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET a=s.a,t=s.t FROM multi_source s WHERE t.id=s.id RETURNING t.id,t.a,t.t,s.t',
              [['1','10',None,None],['2','20','',''],['3',None,'NULL','NULL']],tag='UPDATE 3',types=[23,23,25,25])
        reset()
        check('same-physical-table-distinct-occurrences',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET a=s.b FROM multi_target s WHERE t.id=s.id AND t.id=1 RETURNING t.a,s.a,s.b',
              [['101','1','101']],tag='UPDATE 1')
        reset()
        check('quoted-source-alias',
              'WITH "Source Range"("Id","Amount") AS(VALUES(1,77)) UPDATE multi_target AS "T" '
              'SET a="s.a"."Amount" FROM "Source Range" AS "s.a" WHERE "T".id="s.a"."Id" RETURNING "T".a,"s.a"."Amount"',
              [['77','77']],tag='UPDATE 1')
        reset()
        check('inner-join-typed-on',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET a=s.a,b=e.f FROM multi_source s JOIN multi_extra e '
              'ON s.id=e.id AND COALESCE(e.f,0)>0 WHERE t.id=s.id RETURNING t.id,t.a,t.b,e.f',
              [['1','10','7','7']],tag='UPDATE 1')
        reset()
        check('left-join-real-null-extension',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET b=COALESCE(e.f,-1) FROM multi_source s LEFT JOIN multi_extra e '
              'ON s.id=e.id WHERE t.id=s.id RETURNING t.id,t.b,e.f',
              [['1','7','7'],['2','-1',None],['3','-1',None]],tag='UPDATE 3')
        reset()
        check('right-join-real-null-extension',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET b=e.f FROM multi_source s RIGHT JOIN multi_extra e '
              'ON s.id=e.id WHERE t.id=e.id RETURNING t.id,t.b,s.id',
              [['1','7','1'],['2',None,'2'],['4','9',None]],tag='UPDATE 3')
        reset()
        check('full-join-using-merged-ordinal',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET b=COALESCE(e.f,-1) FROM multi_source s FULL JOIN multi_extra e '
              'USING(id) WHERE t.id=COALESCE(s.id,e.id) RETURNING t.id,s.id,e.id,t.b',
              [['1','1','1','7'],['2','2','2','-1'],['3','3',None,'-1'],['4',None,'4','9']],tag='UPDATE 4')
        reset()
        check('correlated-set-and-returning-child',
              'WITH s(id,a) AS(VALUES(1,7)) UPDATE multi_target t SET a=(SELECT s.a+t.b) FROM s '
              'WHERE (SELECT t.id=s.id) RETURNING t.a,(SELECT s.a+t.id)',[['108','8']],tag='UPDATE 1')
        reset()
        check('derived-source-retained-ast',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET a=q.v FROM(SELECT id,a+5 AS v FROM multi_source)q '
              'WHERE t.id=q.id RETURNING t.id,t.a', [['1','15'],['2','25'],['3',None]],tag='UPDATE 3')
        reset()
        check('delete-using-logical-cte',
              'WITH multi_target AS(VALUES(1),(3)) DELETE FROM multi_target t USING multi_target s WHERE t.id=s.column1 RETURNING t.id,s.column1',
              [['1','1'],['3','3']],tag='DELETE 2')
        reset()
        check('delete-using-join-nullable',
              'WITH p AS(SELECT 1) DELETE FROM multi_target t USING multi_source s LEFT JOIN multi_extra e ON s.id=e.id '
              'WHERE t.id=s.id AND e.f IS NULL RETURNING t.id,s.t,e.f',
              [['2','',None],['3','NULL',None]],tag='DELETE 2')
        reset()
        check('delete-using-self-join',
              'WITH p AS(SELECT 1) DELETE FROM multi_target t USING multi_target s WHERE t.id=s.id AND s.id=1 RETURNING t.id,s.a',
              [['1','1']],tag='DELETE 1')
        reset()
        check('delete-correlated-predicate',
              'WITH s(id,a) AS(VALUES(1,7)) DELETE FROM multi_target t USING s WHERE(SELECT t.id=s.id) RETURNING t.id,(SELECT s.a)',
              [['1','7']],tag='DELETE 1')
        reset()
        check('returning-bare-star-full-source-namespace',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET a=s.a FROM multi_source s WHERE t.id=s.id AND t.id=1 RETURNING *',
              [['1','10','101','old1','1','10',None]],tag='UPDATE 1',types=[23,23,23,25,23,23,25])
        reset()
        check('returning-qualified-star-and-transition-provenance',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET a=s.a FROM multi_source s WHERE t.id=s.id AND t.id=1 '
              'RETURNING old.a,new.a,t.*,s.*',
              [['1','10','1','10','101','old1','1','10',None]],tag='UPDATE 1',types=[23,23,23,23,23,25,23,23,25])
        reset()
        check('using-bare-star-skips-hidden-original-key',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET b=COALESCE(e.f,-1) FROM multi_source s FULL JOIN multi_extra e '
              'USING(id) WHERE t.id=COALESCE(s.id,e.id) AND t.id=1 RETURNING *',
              [['1','1','7','old1','1','10',None,'7']],tag='UPDATE 1',types=[23,23,23,25,23,23,25,23])
        reset()
        check('duplicate-output-labels-have-distinct-ordinals',
              'WITH s AS(SELECT 1 AS id,2 AS id) UPDATE multi_target t SET a=7 FROM s WHERE t.id=1 RETURNING t.a,s.*',
              [['7','1','2']],tag='UPDATE 1',types=[23,23,23])
        reset()
        check('delete-source-and-transition-star',
              'WITH p AS(SELECT 1) DELETE FROM multi_target t USING multi_source s WHERE t.id=s.id AND t.id=1 RETURNING old.*,new.*,s.*',
              [['1','1','101','old1',None,None,None,None,'1','10',None]],tag='DELETE 1',types=[23,23,23,25,23,23,23,25,23,23,25])
        # All source rows must pass the join and WHERE, but a physical target
        # is mutated only once. The valid chosen source is unspecified by PG.
        reset();setup('INSERT INTO multi_source VALUES(1,11,\'other\')')
        multiple=check('multi-match-target-mutated-once',
              "WITH p AS(SELECT 1) UPDATE multi_target t SET a=s.a,b=nextval('multi_set_seq') FROM multi_source s "
              'WHERE t.id=s.id AND t.id=1 RETURNING t.a,t.b,s.a',tag='UPDATE 1',types=[23,23,23])
        if multiple[1] is None:
            good=len(multiple[0])==1 and multiple[0][0][0] in ('10','11') and multiple[0][0][2]==multiple[0][0][0]
            if not good:failures.append(('nondeterministic valid source provenance',multiple))
        check('all-qualified-set-projection-effects',"SELECT currval('multi_set_seq')",[['2']])
        reset();setup('INSERT INTO multi_duplicate VALUES(1,0,NULL),(1,0,NULL)')
        check('identical-nullable-target-tuples-have-distinct-rids',
              "WITH p AS(SELECT 1) UPDATE multi_duplicate t SET a=nextval('multi_tuple_seq') FROM multi_source s WHERE t.id=s.id RETURNING t.a,s.a,t.t",
              [['1','10',None],['2','10',None]],tag='UPDATE 2')
        check('two-physical-tuples-two-projections',"SELECT currval('multi_tuple_seq')",[['2']])
        reset();setup('DELETE FROM multi_source')
        check('empty-source-command-tag',
              'WITH p AS(SELECT 1) UPDATE multi_target t SET a=s.a FROM multi_source s WHERE t.id=s.id RETURNING t.id',[],tag='UPDATE 0',types=[23])
        prefix="WITH writer AS(INSERT INTO multi_audit VALUES(nextval('multi_static_seq')) RETURNING id) "
        for label,command,state in (
            ('target-alias-hides-physical-name','UPDATE multi_target t SET a=s.a FROM multi_source s WHERE multi_target.id=s.id','42P01'),
            ('unaliased-target-repeat','UPDATE multi_target SET a=1 FROM multi_target','42712'),
            ('alias-collision-with-target','UPDATE multi_target t SET a=1 FROM multi_source t','42712'),
            ('unknown-source-column-empty','UPDATE multi_target t SET a=s.missing FROM multi_source s WHERE false','42703'),
            ('ambiguous-column-empty','UPDATE multi_target t SET a=id FROM multi_source s WHERE false','42702'),
            ('unknown-short-circuit-callee-empty','UPDATE multi_target t SET a=CASE WHEN false THEN no_such_multi_fn() ELSE 1 END FROM multi_source s WHERE false','42883'),
            ('join-on-boolean-static','UPDATE multi_target t SET a=1 FROM multi_source s JOIN multi_extra e ON 1 WHERE false','42804'),
            ('join-on-target-not-visible','UPDATE multi_target t SET a=1 FROM multi_source s JOIN multi_extra e ON t.id=e.id WHERE false','42P01'),
            ('delete-target-repeat','DELETE FROM multi_target USING multi_target WHERE false','42712'),
            ('delete-missing-source-column','DELETE FROM multi_target t USING multi_source s WHERE s.missing=1','42703'),
        ):check(label,prefix+command,state=state)
        check('all-static-errors-before-writer',"SELECT currval('multi_static_seq')",state='55000')
        check('static-errors-write-no-audit','SELECT id FROM multi_audit',[])
        reset()
        check('writing-ctes-base-source-fixed-cid',
              'WITH changed AS(UPDATE multi_source SET a=99 WHERE id=1 RETURNING id),removed AS(DELETE FROM multi_source WHERE id=2 RETURNING id) '
              'UPDATE multi_target t SET a=s.a FROM multi_source s WHERE t.id=s.id RETURNING t.id,t.a',
              [['1','10'],['2','20'],['3',None]],tag='UPDATE 3')
        check('writes-visible-next-command','SELECT id,a FROM multi_source ORDER BY id',[['1','99'],['3',None]])
        reset()
        check('returning-write-cte-is-real-source',
              'WITH w AS(INSERT INTO multi_source VALUES(4,44,\'new\') RETURNING id,a) UPDATE multi_target t SET a=w.a FROM w '
              'WHERE t.id=w.id RETURNING t.id,t.a,(SELECT a FROM multi_source WHERE id=4)',
              [['4','44',None]],tag='UPDATE 1')
        reset();setup("UPDATE multi_source SET t=CASE WHEN id=1 THEN '11' ELSE 'bad' END")
        check('late-set-cast-rolls-back-whole-unit',
              "WITH w AS(INSERT INTO multi_audit VALUES(9) RETURNING id) UPDATE multi_target t SET a=CAST(s.t AS INT),b=nextval('multi_late_seq') "
              'FROM multi_source s,w WHERE t.id=s.id AND s.id<=2 AND w.id>0',state='22P02')
        check('late-error-original-targets','SELECT id,a FROM multi_target ORDER BY id',[['1','1'],['2','2'],['3','3'],['4','4']])
        check('late-error-cte-writes-rolled-back','SELECT id FROM multi_audit',[])
        check('one-real-prefix-projection-effect-survives-error',"SELECT currval('multi_late_seq')",[['1']])
        reset();setup('BEGIN');setup('INSERT INTO multi_audit VALUES(90)');setup('SAVEPOINT keep_multi')
        check('explicit-transaction-failure',
              'WITH s(id,a) AS(VALUES(1,1),(2,1)) UPDATE multi_target t SET id=s.a FROM s WHERE t.id=s.id',state='23505',ready=b'E')
        check('failed-user-block','SELECT 1',state='25P02',ready=b'E')
        setup('ROLLBACK TO keep_multi')
        check('prior-success-retained','SELECT id FROM multi_audit',[['90']],ready=b'T')
        check('failed-update-recovered','SELECT id FROM multi_target ORDER BY id',[['1'],['2'],['3'],['4']],ready=b'T')
        setup('ROLLBACK')
        assert not failures,failures
        print('[WITH MULTISOURCE DML] passed',flush=True)
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
