#!/usr/bin/env python3
"""R07: signed per-row window offsets and lazy typed defaults."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('signed_offset_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,db,password=r._reference_connection_settings();sock=socket.create_connection((host,port),timeout=r.wire_timeout())
        c.startup_reference(sock,user,db,password);r.verify_reference_version(c,sock)
    else:server=r.start_ours(c);sock=server['sock']
    failures=[];checks=0
    numbers=['9223372036854775807','9223372036854775806',None,'-9223372036854775808','-9223372036854775807',None]
    texts=['','NULL',None,'a b',' spaced ',None];offsets=[0,1,-1,None,1,-1]
    def query(sql):return r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def control(sql,rows=None,headers=None,oids=None,state=None):
        nonlocal checks
        result=query(sql);checks+=1;good=result[1]==state
        if state:good=good and result[0]==[] and result[4] is None
        else:good=good and result[0]==rows and result[3]==headers and result[5]==oids and result[4]=='SELECT '+str(len(rows))
        print('SIGNED_WINDOW_OFFSET',sql,result,'PASS',good,flush=True)
        if not good:failures.append((sql,result,rows,state))
    def expected(function,values,offset,fallback,legacy):
        result=[]
        for i in range(6):
            distance=offset[i];target=None if distance is None else i+(-distance if function=='lag' else distance)
            value=None if target is None else values[target] if 0<=target<6 and target//3==i//3 else fallback
            result.append([str(i+1),value]+([str(1 if i<3 else 4)] if legacy else []))
        return result
    try:
        ok('CREATE TEMP TABLE offset_inputs(id INT,p TEXT,n INT,v BIGINT,payload TEXT)')
        ok("INSERT INTO offset_inputs VALUES(1,'a',0,9223372036854775807,''),(2,'a',1,9223372036854775806,'NULL'),"
           "(3,'a',-1,NULL,NULL),(4,'b',NULL,-9223372036854775808,'a b'),(5,'b',1,-9223372036854775807,' spaced '),(6,'b',-1,NULL,NULL)")
        cases=[('0',[0]*6),('1',[1]*6),('-1',[-1]*6),('2147483647',[2147483647]*6),
            ('-2147483648',[-2147483648]*6),('NULL',[None]*6),('n',offsets),
            ('-n',[None if n is None else -n for n in offsets]),('abs(n)',[None if n is None else abs(n) for n in offsets])]
        for legacy in [False,True]:
            extra=',nth_value(id,1) OVER(PARTITION BY p ORDER BY id) AS first' if legacy else ''
            headers=['id','shifted']+(['first'] if legacy else []);baseoids=[23,None]+([23] if legacy else [])
            for function in ['lag','lead']:
                for column,values,default,oid in [('v',numbers,'99',20),('payload',texts,"'NULL'",25)]:
                    for expression,offset in cases:
                        sql=f'SELECT id,{function}({column},{expression},{default}) OVER(PARTITION BY p ORDER BY id) AS shifted{extra} FROM offset_inputs ORDER BY id'
                        oids=baseoids.copy();oids[1]=oid
                        control(sql,expected(function,values,offset,'99' if column=='v' else 'NULL',legacy),headers,oids)
                for offset in [0,1,-1]:
                    sql=f'SELECT id,{function}(payload,{offset}) OVER(PARTITION BY p ORDER BY id) AS shifted{extra} FROM offset_inputs ORDER BY id'
                    oids=baseoids.copy();oids[1]=25
                    control(sql,expected(function,texts,[offset]*6,None,legacy),headers,oids)
                for suffix in ['WHERE false','LIMIT 0']:
                    control(f'SELECT id,{function}(v,0) OVER(ORDER BY id) AS shifted{extra} FROM offset_inputs {suffix}',[],headers,[23,20]+([23] if legacy else []))
                statement='r07_'+function+('_legacy' if legacy else '_structured')
                ok(f'PREPARE {statement}(integer) AS SELECT id,{function}(v,$1,99) OVER(PARTITION BY p ORDER BY id) AS shifted{extra} FROM offset_inputs ORDER BY id')
                try:
                    for argument,offset in [('0',0),('-1',-1),('NULL',None)]:
                        control(f'EXECUTE {statement}({argument})',expected(function,numbers,[offset]*6,'99',legacy),headers,[23,20]+([23] if legacy else []))
                finally:ok('DEALLOCATE '+statement)
        for function in ['lag','lead']:
            for default in ["''","NULL","'NULL'","payload","'a''b'",'"payload"']:
                fallback=texts if default in ['payload','"payload"'] else [
                    '' if default=="''" else None if default=='NULL' else "a'b" if default=="'a''b'" else 'NULL']*6
                control(f'SELECT id,{function}(payload,99,{default}) OVER(PARTITION BY p ORDER BY id) AS shifted FROM offset_inputs ORDER BY id',
                    [[str(i+1),fallback[i]] for i in range(6)],['id','shifted'],[23,25])
            control(f'SELECT id,{function}(id,0,2147483648::bigint) OVER(ORDER BY id) AS shifted FROM offset_inputs ORDER BY id',
                [[str(i),str(i)] for i in range(1,7)],['id','shifted'],[23,20])
            for bad,state in [("'bad'",'22P02'),('TRUE','42883'),('2147483648','42883'),('1::bigint','42883')]:
                control(f'SELECT id,{function}(v,{bad}) OVER(ORDER BY id) AS shifted FROM offset_inputs LIMIT 0',state=state)
            control(f"SELECT id,{function}(id,0,'bad') OVER(ORDER BY id) AS shifted FROM offset_inputs LIMIT 0",state='22P02')
            for default in ['TRUE',"'x'::text"]:
                control(f'SELECT id,{function}(id,0,{default}) OVER(ORDER BY id) AS shifted FROM offset_inputs LIMIT 0',state='42883')
            control(f'SELECT id,{function}(NULL,0) OVER(ORDER BY id) AS shifted FROM offset_inputs ORDER BY id',
                state='42804')
            for arguments in ['NULL::text,0','NULL,0,NULL']:
                control(f'SELECT id,{function}({arguments}) OVER(ORDER BY id) AS shifted FROM offset_inputs ORDER BY id',
                    [[str(i),None] for i in range(1,7)],['id','shifted'],[23,25])
        sequence='r07_offset_'+uuid.uuid4().hex[:12];ok('CREATE SEQUENCE '+sequence)
        try:
            control(f"SELECT id,lag(v,0,nextval('{sequence}')) OVER(PARTITION BY p ORDER BY id) AS shifted FROM offset_inputs ORDER BY id",
                [[str(i+1),v] for i,v in enumerate(numbers)],['id','shifted'],[23,20])
            control(f"SELECT currval('{sequence}')",state='55000')
            for function,distance,first in [('lag',1,1),('lead',-1,3)]:
                rows=expected(function,numbers,[distance]*6,None,False);rows[0][1]=str(first);rows[3][1]=str(first+1)
                control(f"SELECT id,{function}(v,{distance},nextval('{sequence}')) OVER(PARTITION BY p ORDER BY id) AS shifted FROM offset_inputs ORDER BY id",rows,['id','shifted'],[23,20])
                control(f"SELECT currval('{sequence}')",[[str(first+1)]],['currval'],[20])
            control(f"SELECT id,lag(v,NULL,nextval('{sequence}')) OVER(PARTITION BY p ORDER BY id) AS shifted FROM offset_inputs ORDER BY id",
                [[str(i),None] for i in range(1,7)],['id','shifted'],[23,20])
            control(f"SELECT currval('{sequence}')",[['4']],['currval'],[20])
            control(f"SELECT id,lag(v,99,nextval('{sequence}')) OVER(PARTITION BY p ORDER BY id) AS shifted FROM offset_inputs ORDER BY id",
                [[str(i),str(i+4)] for i in range(1,7)],['id','shifted'],[23,20])
            control(f"SELECT currval('{sequence}')",[['10']],['currval'],[20])
            control(f"SELECT id,lag(nextval('{sequence}'),0) OVER(PARTITION BY p ORDER BY id) AS shifted FROM offset_inputs ORDER BY id",
                [[str(i),str(i+10)] for i in range(1,7)],['id','shifted'],[23,20])
            control(f"SELECT currval('{sequence}')",[['16']],['currval'],[20])
            control(f"SELECT id,lag(nextval('{sequence}'),NULL) OVER(PARTITION BY p ORDER BY id) AS shifted FROM offset_inputs ORDER BY id",
                [[str(i),None] for i in range(1,7)],['id','shifted'],[23,20])
            control(f"SELECT currval('{sequence}')",[['16']],['currval'],[20])
        finally:ok('DROP SEQUENCE '+sequence)
        print('SIGNED_WINDOW_OFFSET_PROTOCOL_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks==152 and not failures,failures
    finally:
        if server:r.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
