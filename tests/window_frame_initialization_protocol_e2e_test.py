#!/usr/bin/env python3
"""Window frame offsets initialize once per parsed window, not per function/row."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('frame_init_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,db,password=r._reference_connection_settings();sock=socket.create_connection((host,port),timeout=r.wire_timeout())
        c.startup_reference(sock,user,db,password);r.verify_reference_version(c,sock)
    else:server=r.start_ours(c);sock=server['sock']
    suffix_id=uuid.uuid4().hex[:16]
    sequence='r04_frame_seq_'+suffix_id;function='r04_frame_offset_'+suffix_id
    created=False;function_created=False;checks=0;failures=[]
    def query(sql):return r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def control(label,sql,rows,headers,oids):
        nonlocal checks
        result=query(sql);checks+=1
        good=result[1] is None and result[0]==rows and result[3]==headers and result[5]==oids and result[4]=='SELECT '+str(len(rows))
        print('WINDOW_FRAME_INIT',label,sql,result,'PASS',good,flush=True)
        if not good:failures.append((label,sql,result,rows,headers,oids))
    try:
        ok('CREATE TEMP TABLE window_frame_offset_rows(id INT,p TEXT,v INT)')
        ok("INSERT INTO window_frame_offset_rows VALUES(1,'a',10),(2,'a',20),(3,'a',30),(4,'b',40),(5,'b',50),(6,'b',60)")
        ok('CREATE SEQUENCE '+sequence);created=True
        # Observe exact calls without prescribing which of two distinct
        # WindowAgg definitions PostgreSQL chooses to initialize first.
        ok('CREATE FUNCTION '+function+'() RETURNS BIGINT LANGUAGE plpgsql AS $$ DECLARE observed BIGINT; BEGIN SELECT nextval(\''+
           sequence+'\') INTO observed; RETURN 1; END; $$')
        function_created=True
        value=function+'()'
        base='PARTITION BY p ORDER BY id ROWS '+value+' PRECEDING'
        child='(SELECT '+value+' FROM window_frame_offset_rows q WHERE id=1)'
        cases=[
            ('same',base,base,1,1,1),
            ('whitespace',base,' PARTITION  BY  p ORDER  BY  id ROWS '+value+' PRECEDING ',1,1,1),
            ('no_others',base,base+' EXCLUDE NO OTHERS',1,1,1),
            ('explicit_cast',base,'PARTITION BY p ORDER BY id ROWS CAST('+value+' AS bigint) PRECEDING',2,1,1),
            ('explicit_direction',base,base.replace('ORDER BY id','ORDER BY id ASC'),2,1,1),
            ('explicit_nulls',base,base.replace('ORDER BY id','ORDER BY id NULLS LAST'),2,1,1),
            ('explicit_between',base,'PARTITION BY p ORDER BY id ROWS BETWEEN '+value+' PRECEDING AND CURRENT ROW',2,1,1),
            ('qualified_key',base,base.replace('ORDER BY id','ORDER BY t.id'),2,1,1),
            ('qualified_routine',base,base.replace(value,'public.'+value),2,1,1),
            ('same_child','PARTITION BY p ORDER BY id ROWS (SELECT '+value+') PRECEDING',
             'PARTITION BY p ORDER BY id ROWS (SELECT '+value+') PRECEDING',1,1,1),
            ('same_table_child','PARTITION BY p ORDER BY id ROWS '+child+' PRECEDING',
             'PARTITION BY p ORDER BY id ROWS '+child+' PRECEDING',1,1,1),
            ('qualified_child_column','PARTITION BY p ORDER BY id ROWS '+child+' PRECEDING',
             'PARTITION BY p ORDER BY id ROWS '+child.replace('WHERE id=','WHERE q.id=')+' PRECEDING',2,1,1)
        ]
        values=[10,20,30,40,50,60]
        for name,left,right,calls,left_distance,right_distance in cases:
            for suffix in ('',' WHERE FALSE',' LIMIT 0'):
                ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
                expected=[]
                if not suffix:
                    for i in range(6):
                        first=0 if i<3 else 3
                        expected.append([str(i+1),str(sum(values[max(first,i-left_distance):i+1])),
                                         str(i-max(first,i-right_distance)+1)])
                sql='SELECT id,sum(v) OVER('+left+') AS total,count(v) OVER('+right+') AS n FROM window_frame_offset_rows AS t'+suffix
                if not suffix:sql+=' ORDER BY id'
                control(name+suffix,sql,expected,['id','total','n'],[23,20,20])
                control(name+suffix+' counter','SELECT nextval(\''+sequence+'\')',
                        [[str(1 if suffix==' LIMIT 0' else calls+1)]],['nextval'],[20])
        for suffix in ('',' WHERE FALSE',' LIMIT 0'):
            ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
            frame='PARTITION BY p ORDER BY id ROWS BETWEEN '+value+' PRECEDING AND '+value+' FOLLOWING'
            expected=[]
            if not suffix:
                for i in range(6):
                    first=0 if i<3 else 3;last=3 if i<3 else 6
                    data=values[max(first,i-1):min(last,i+2)]
                    expected.append([str(i+1),str(sum(data)),str(len(data))])
            sql='SELECT id,sum(v) OVER('+frame+') AS total,count(v) OVER('+frame+') AS n FROM window_frame_offset_rows AS t'+suffix
            if not suffix:sql+=' ORDER BY id'
            control('two_bounds'+suffix,sql,expected,['id','total','n'],[23,20,20])
            control('two_bounds'+suffix+' counter','SELECT nextval(\''+sequence+'\')',
                    [['1' if suffix==' LIMIT 0' else '3']],['nextval'],[20])
        print('WINDOW_FRAME_INIT_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks==78 and not failures,failures
    finally:
        if function_created:
            result=query('DROP FUNCTION '+function+'()')
            assert result[1] is None,result
        if created:
            result=query('DROP SEQUENCE '+sequence)
            assert result[1] is None,result
        if server:r.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
