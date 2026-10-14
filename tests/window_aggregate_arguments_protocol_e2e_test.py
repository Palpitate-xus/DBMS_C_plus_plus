#!/usr/bin/env python3
"""Window aggregate expression arguments retain typed values and demand."""
import importlib.util
import socket
import sys
import uuid
from decimal import Decimal
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('window_aggregate_arguments_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,db,password=r._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=r.wire_timeout())
        c.startup_reference(sock,user,db,password);r.verify_reference_version(c,sock)
    else:server=r.start_ours(c);sock=server['sock']
    sequence='r45_window_seq_'+uuid.uuid4().hex[:16];created=False;checks=0;failures=[]
    def query(sql):return r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def control(sql,rows=None,headers=None,oids=None,state=None,error_prefix=None):
        nonlocal checks
        result=query(sql);checks+=1
        passed=result[1]==state
        if state:
            # Validate any successful prefix before a runtime error without
            # prescribing buffering/flush timing. There is no successful
            # complete result. Pure analysis errors must expose no rows.
            allowed=[] if error_prefix is None else error_prefix
            passed=passed and len(result[0])<=len(allowed) and result[0]==allowed[:len(result[0])] and result[4] is None
        else:passed=passed and result[0]==rows and result[3]==headers and result[5]==oids and result[4]=='SELECT '+str(len(rows))
        print('WINDOW_AGGREGATE_ARGUMENT',sql,result,'PASS',passed,flush=True)
        if not passed:failures.append((sql,result,rows,headers,oids))
    try:
        ok('CREATE TEMP TABLE window_aggregate_argument_rows(id INT)')
        ok('INSERT INTO window_aggregate_argument_rows VALUES(1),(2),(3)')
        ok('CREATE SEQUENCE '+sequence);created=True
        expression="nextval('"+sequence+"')"
        for ordered in (False,True):
            ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
            call='sum('+expression+') OVER(ORDER BY id)'
            control('SELECT '+call+' AS x,'+call+' AS x FROM window_aggregate_argument_rows'+(' ORDER BY x' if ordered else ''),
                    [['1','1'],['3','3'],['6','6']] if ordered else [['1','2'],['4','6'],['9','12']],
                    ['x','x'],[1700,1700])
            control('SELECT '+expression,[['4' if ordered else '7']],['nextval'],[20])
        ok('CREATE TEMP TABLE window_aggregate_expression_rows(id INT,k INT,p TEXT,v INT,word TEXT)')
        ok("INSERT INTO window_aggregate_expression_rows VALUES(1,1,'a',10,'z'),(2,1,'a',NULL,NULL),(3,2,'b',30,'a'),(4,3,'b',50,'m')")
        values=[11,None,31,51];keys=[1,1,2,3];groups=[0,0,1,2]
        definitions=[('ubp',0,'cur',0),('cur',0,'ubf',0),('ubp',0,'ubf',0),
                     ('pre',1,'pre',1),('pre',2,'pre',1),('cur',0,'cur',0),
                     ('cur',0,'fol',2),('fol',1,'fol',2),('fol',2,'fol',1),
                     ('pre',3,'pre',5),('ubp',0,'pre',1),('fol',2,'ubf',0)]
        def bound(kind,n):
            return {'ubp':'UNBOUNDED PRECEDING','ubf':'UNBOUNDED FOLLOWING','cur':'CURRENT ROW'}.get(
                kind,str(n)+(' PRECEDING' if kind=='pre' else ' FOLLOWING'))
        def endpoint(kind,n,current):
            if kind=='ubp':return float('-inf')
            if kind=='ubf':return float('inf')
            return current+(-n if kind=='pre' else n if kind=='fol' else 0)
        def aggregate(function,members):
            data=[values[j] for j in members if values[j] is not None]
            if function=='count':return str(len(data))
            if not data:return None
            if function=='sum':return str(sum(data))
            if function=='min':return str(min(data))
            if function=='max':return str(max(data))
            return format(Decimal(sum(data))/Decimal(len(data)),'.16f')
        for mode in ('ROWS','GROUPS','RANGE'):
            coordinates=list(range(4)) if mode=='ROWS' else groups if mode=='GROUPS' else keys
            for start,so,end,eo in definitions:
                frame=mode+' BETWEEN '+bound(start,so)+' AND '+bound(end,eo)
                for function in ('sum','count','min','max','avg'):
                    rows=[]
                    for i in range(4):
                        lo=endpoint(start,so,coordinates[i]);hi=endpoint(end,eo,coordinates[i])
                        rows.append([str(i+1),aggregate(function,[j for j,c in enumerate(coordinates) if lo<=c<=hi])])
                    control('SELECT id,'+function+'(v+1) OVER(ORDER BY k '+frame+
                            ') AS x FROM window_aggregate_expression_rows ORDER BY id',
                            rows,['id','x'],[23,1700 if function=='avg' else 23 if function in ('min','max') else 20])
            for exclusion in ('NO OTHERS','CURRENT ROW','GROUP','TIES'):
                for function in ('sum','count','min','max','avg'):
                    rows=[]
                    for i in range(4):
                        members=[j for j,c in enumerate(coordinates) if c<=coordinates[i]]
                        if exclusion=='CURRENT ROW':members=[j for j in members if j!=i]
                        if exclusion=='GROUP':members=[j for j in members if keys[j]!=keys[i]]
                        if exclusion=='TIES':members=[j for j in members if j==i or keys[j]!=keys[i]]
                        rows.append([str(i+1),aggregate(function,members)])
                    control('SELECT id,'+function+'(v+1) OVER(ORDER BY k '+mode+
                            ' BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE '+exclusion+
                            ') AS x FROM window_aggregate_expression_rows ORDER BY id',
                            rows,['id','x'],[23,1700 if function=='avg' else 23 if function in ('min','max') else 20])
        for argument,function,result,oid in [
                ('NULL','count',['0']*4,20),("'NULL'",'count',['1','2','3','4'],20),
                ('1','sum',['1','2','3','4'],20),('NULL::integer','sum',[None]*4,20),
                ("'NULL'::text",'min',['NULL']*4,25),
                ("coalesce(word,'')",'count',['1','2','3','4'],20),
                ("coalesce(word,'')",'min',['z','','',''],25)]:
            control('SELECT id,'+function+'('+argument+') OVER(ORDER BY id) AS x FROM window_aggregate_expression_rows ORDER BY id',
                    [[str(i+1),value] for i,value in enumerate(result)],['id','x'],[23,oid])
        # Reset/reuse counts are observable: volatile moving aggregates may
        # re-evaluate overlapping members, while stable frame heads append.
        for frame,suffix,expected,next_value,order in [
                ('','',['1','3','6','10'],'5','id'),
                ('ROWS BETWEEN 1 PRECEDING AND CURRENT ROW','',['1','3','7','11'],'7','id'),
                ('ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING','',[None,'1','2','3'],'4','id'),
                ('ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW','',[None,'1','5','15'],'7','id'),
                ('GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW','',['3','3','6','9'],'6','k'),
                ('RANGE BETWEEN 1 PRECEDING AND CURRENT ROW','',['3','3','6','9'],'6','k'),
                ('',' LIMIT 1',['1'],'2','id'),('',' LIMIT 1 OFFSET 1',['3'],'3','id'),
                ('ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING',' LIMIT 1',['3'],'3','id'),
                ('',' ORDER BY x DESC LIMIT 1',['10'],'5','id')]:
            ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
            control('SELECT sum('+expression+') OVER(ORDER BY '+order+' '+frame+
                    ') AS x FROM window_aggregate_expression_rows'+suffix,[[v] for v in expected],['x'],[1700])
            control('SELECT '+expression,[[next_value]],['nextval'],[20])
        for shared in (False,True):
            ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
            call='sum('+expression+') OVER(ORDER BY k)'
            control('SELECT '+call+' AS '+('x' if shared else 'a')+','+call+' AS '+('x' if shared else 'b')+
                    ' FROM window_aggregate_expression_rows'+(' ORDER BY x' if shared else ''),
                    [['3','3'],['3','3'],['6','6'],['10','10']] if shared else [['4','6'],['4','6'],['9','12'],['16','20']],
                    ['x','x'] if shared else ['a','b'],[1700,1700])
            control('SELECT '+expression,[['5' if shared else '9']],['nextval'],[20])
        for suffix in (' WHERE FALSE',' LIMIT 0'):
            ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
            control('SELECT sum('+expression+') OVER(ORDER BY id) AS x FROM window_aggregate_expression_rows'+suffix,[],['x'],[1700])
            control('SELECT '+expression,[['1']],['nextval'],[20])
        base='SELECT sum(10/(3-id)) OVER(ORDER BY id) AS x FROM window_aggregate_expression_rows'
        control(base,state='22012',error_prefix=[['5'],['15']])
        control(base+' LIMIT 1',[['5']],['x'],[20])
        control(base+' WHERE FALSE',[],['x'],[20]);control(base+' LIMIT 0',[],['x'],[20])
        control('SELECT sum(10/(4-id)) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS x FROM window_aggregate_expression_rows',
                [[None],['3'],['5'],['10']],['x'],[20])
        for suffix in ('',' WHERE FALSE',' LIMIT 0'):
            control('SELECT sum(missing+1) OVER(ORDER BY id) AS x FROM window_aggregate_expression_rows'+suffix,state='42703')
        control('SELECT id,sum(v+1) OVER(ORDER BY id) AS x,count(*) OVER(ORDER BY id) AS n FROM window_aggregate_expression_rows ORDER BY id',
                [['1','11','1'],['2','11','2'],['3','42','3'],['4','93','4']],['id','x','n'],[23,20,20])
        control('SELECT id,sum(v+1) OVER(PARTITION BY p ORDER BY id) AS x FROM window_aggregate_expression_rows ORDER BY id',
                [['1','11'],['2','11'],['3','31'],['4','82']],['id','x'],[23,20])
        ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
        control('SELECT sum('+expression+') OVER(PARTITION BY p ORDER BY id) AS x FROM window_aggregate_expression_rows',
                [['1'],['3'],['3'],['7']],['x'],[1700])
        control('SELECT '+expression,[['5']],['nextval'],[20])
        control('SELECT id,sum((SELECT t.v+1)) OVER(ORDER BY id) AS x FROM window_aggregate_expression_rows t ORDER BY id',
                [['1','11'],['2','11'],['3','42'],['4','93']],['id','x'],[23,20])
        ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
        control('SELECT sum((SELECT '+expression+')) OVER(ORDER BY id) AS x FROM window_aggregate_expression_rows',
                [['1'],['2'],['3'],['4']],['x'],[1700])
        control('SELECT '+expression,[['2']],['nextval'],[20])
        ok('CREATE TEMP TABLE window_aggregate_float_rows(id INT,r REAL,d DOUBLE PRECISION)')
        ok('INSERT INTO window_aggregate_float_rows VALUES(1,0.1,0.1),(2,0.2,0.2)')
        for function,argument,expected,oid in [
                ('sum','r+0::real',['0.1','0.3'],700),
                ('avg','r+0::real',['0.10000000149011612','0.15000000223517418'],701),
                ('min','r+0::real',['0.1','0.1'],700),('max','r+0::real',['0.1','0.2'],700),
                ('sum','d+0::double precision',['0.1','0.30000000000000004'],701),
                ('avg','d+0::double precision',['0.1','0.15000000000000002'],701)]:
            control('SELECT '+function+'('+argument+') OVER(ORDER BY id) AS x FROM window_aggregate_float_rows',
                    [[value] for value in expected],['x'],[oid])
        print('WINDOW_AGGREGATE_ARGUMENT_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks==300 and not failures,failures
    finally:
        if created:ok('DROP SEQUENCE '+sequence)
        if server:r.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
