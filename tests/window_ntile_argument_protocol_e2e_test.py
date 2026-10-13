#!/usr/bin/env python3
"""R06: typed NTILE arguments and lazy initialization per partition."""
import importlib.util
import os
import socket
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('ntile_argument_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,db,password=r._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=r.wire_timeout())
        c.startup_reference(sock,user,db,password);r.verify_reference_version(c,sock)
    else:
        server=r.start_ours(c);sock=server['sock']
    failures=[];checks=0
    def query(sql):return r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def control(sql,rows=None,state=None,headers=None,oids=None):
        nonlocal checks
        result=query(sql);checks+=1
        good=result[1]==state
        if state:good=good and result[0]==[] and result[4] is None
        else:good=good and result[0]==rows and result[3]==headers and result[5]==oids and result[4]=='SELECT '+str(len(rows))
        print('NTILE_ARGUMENT',sql,result,'PASS',good,flush=True)
        if not good:failures.append((sql,result,rows,state))
    try:
        ok('CREATE TEMP TABLE tile_inputs(id INT,p TEXT,n INT)')
        ok("INSERT INTO tile_inputs VALUES(1,'a',NULL),(2,'a',3),(3,'a',0),(4,'a',-1),"
           "(5,'a',8),(6,'a',NULL),(7,'b',NULL),(8,'b',NULL),(9,'b',2)")
        for legacy in [False,True]:
            extra=',nth_value(id,1) OVER(PARTITION BY p ORDER BY id) AS first' if legacy else ''
            headers=['id','bucket']+(['first'] if legacy else []);oids=[23,23]+([23] if legacy else [])
            def expected(buckets):
                return [[str(i),value]+([str(1 if i<=6 else 7)] if legacy else []) for i,value in enumerate(buckets,1)]
            for argument,values,state in [
                ('0',None,'22014'),('-1',None,'22014'),('-2147483648',None,'22014'),
                ('NULL',[None]*9,None),('NULL::integer',[None]*9,None),
                ('2',['1','1','1','2','2','2','1','1','2'],None),
                ("'2'",['1','1','1','2','2','2','1','1','2'],None),
                ("'0'",None,'22014'),("'bad'",None,'22P02'),
                ('2147483648',None,'42883'),('1::bigint',None,'42883'),('TRUE',None,'42883'),
                ('n',[None,'1','1','2','2','3',None,None,'1'],None),
                ('n+0',[None,'1','1','2','2','3',None,None,'1'],None),
                ('abs(-2)',['1','1','1','2','2','2','1','1','2'],None),
                ('9/(9-id)',['1','1','1','1','1','1','1','2','3'],None),
            ]:
                sql=f'SELECT id,ntile({argument}) OVER(PARTITION BY p ORDER BY id) AS bucket{extra} FROM tile_inputs ORDER BY id'
                control(sql,None if state else expected(values),state,headers,oids)
            for suffix in ['WHERE false','LIMIT 0']:
                for argument,state in [('0',None),('-1',None),('NULL',None),("'bad'",'22P02')]:
                    sql=f'SELECT id,ntile({argument}) OVER(PARTITION BY p ORDER BY id) AS bucket{extra} FROM tile_inputs {suffix}'
                    control(sql,[],state,headers,oids)
            for predicate in ['id>=3','id>=4',"p='b' OR id=3"]:
                control(f'SELECT id,ntile(n) OVER(PARTITION BY p ORDER BY id) AS bucket{extra} FROM tile_inputs WHERE {predicate}',
                    state='22014')
            statement='r06_ntile_legacy' if legacy else 'r06_ntile_structured'
            ok(f'PREPARE {statement}(integer) AS SELECT id,ntile($1) OVER(PARTITION BY p ORDER BY id) AS bucket{extra} FROM tile_inputs ORDER BY id')
            try:
                for argument,state,values in [('0','22014',None),('-1','22014',None),
                    ('NULL',None,[None]*9),('2',None,['1','1','1','2','2','2','1','1','2'])]:
                    control(f'EXECUTE {statement}({argument})',None if state else expected(values),state,headers,oids)
            finally:ok('DEALLOCATE '+statement)
        sequence='r06_tile_'+uuid.uuid4().hex[:12];ok('CREATE SEQUENCE '+sequence)
        try:
            for argument,values,calls in [
                (f"nextval('{sequence}')::integer",['1','1','1','1','1','1','1','1','2'],'2'),
                (f"CASE WHEN n IS NULL THEN NULL ELSE nextval('{sequence}')::integer END",
                 [None,'1','1','2','2','3',None,None,'1'],'4')]:
                control(f'SELECT id,ntile({argument}) OVER(PARTITION BY p ORDER BY id) AS bucket FROM tile_inputs ORDER BY id',
                    [[str(i),v] for i,v in enumerate(values,1)],headers=['id','bucket'],oids=[23,23])
                control(f"SELECT currval('{sequence}')",[[calls]],headers=['currval'],oids=[20])
            control(f"SELECT id,ntile(nextval('{sequence}')::integer) OVER(PARTITION BY p ORDER BY id) AS bucket FROM tile_inputs LIMIT 0",
                [],headers=['id','bucket'],oids=[23,23])
            control(f"SELECT currval('{sequence}')",[['4']],headers=['currval'],oids=[20])
            control(f"SELECT id,ntile(NULLIF(nextval('{sequence}')::integer,nextval('{sequence}')::integer-1)) OVER(PARTITION BY p ORDER BY id) AS bucket FROM tile_inputs ORDER BY id",
                [[str(i),None] for i in range(1,10)],headers=['id','bucket'],oids=[23,23])
            control(f"SELECT currval('{sequence}')",[['22']],headers=['currval'],oids=[20])
            control(f"SELECT id,ntile(CASE WHEN id=1 THEN nextval('{sequence}')::integer ELSE 1/0 END) OVER(PARTITION BY p ORDER BY id) AS bucket FROM tile_inputs",
                state='22012')
            control(f"SELECT currval('{sequence}')",[['22']],headers=['currval'],oids=[20])
        finally:ok('DROP SEQUENCE '+sequence)
        print('NTILE_ARGUMENT_PROTOCOL_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks==72 and not failures,failures
    finally:
        if server:r.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
