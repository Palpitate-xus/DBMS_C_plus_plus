#!/usr/bin/env python3
"""R44: window result slots cannot overwrite real source column names."""
import importlib.util
import socket
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('window_namespace_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,db,password=r._reference_connection_settings();sock=socket.create_connection((host,port),timeout=r.wire_timeout())
        c.startup_reference(sock,user,db,password);r.verify_reference_version(c,sock)
    else:server=r.start_ours(c);sock=server['sock']
    checks=0;failures=[]
    def query(sql):return r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def control(sql,rows,headers,oids):
        nonlocal checks
        result=query(sql);checks+=1
        def typed(values):
            return [[Decimal(v) if v is not None and oids[i]==1700 else v for i,v in enumerate(row)] for row in values]
        try:
            good=result[1] is None and result[3]==headers and result[5]==oids and result[4]=='SELECT '+str(len(rows)) and typed(result[0])==typed(rows)
        except InvalidOperation:good=False
        print('WINDOW_INTERNAL_NAMESPACE',sql,result,'PASS',good,flush=True)
        if not good:failures.append((sql,result,rows,headers,oids))
    try:
        ok('CREATE TEMP TABLE r44_source(id INT,_win_0 INT)')
        ok('INSERT INTO r44_source VALUES(1,10),(2,20)')
        # Preserve all three original discovery SQL controls and exact rows.
        control('SELECT id,row_number() OVER(ORDER BY id) AS rn,_win_0 FROM r44_source ORDER BY id',
            [['1','1','10'],['2','2','20']],['id','rn','_win_0'],[23,20,23])
        control('SELECT id,row_number() OVER(ORDER BY id) AS rn,nth_value(_win_0,1) OVER(ORDER BY id) AS first,_win_0 FROM r44_source ORDER BY id',
            [['1','1','10','10'],['2','2','10','20']],['id','rn','first','_win_0'],[23,20,23,23])
        control('SELECT id,sum(_win_0) OVER() AS total,nth_value(_win_0,1) OVER(ORDER BY id) AS first,_win_0 FROM r44_source ORDER BY id',
            [['1','30','10','10'],['2','30','10','20']],['id','total','first','_win_0'],[23,20,23,23])
        for suffix,declaration,oid,values in [
            ('bigint','BIGINT',20,['9223372036854775807','9223372036854775806',None,None,'-9223372036854775808','-9223372036854775807']),
            ('text','TEXT COLLATE "C"',25,['','01','NULL',None,'a b',' spaced ']),
            ('numeric','NUMERIC',1700,['1.00','2.50','9007199254740993.01',None,'-0.00','12345678901234567890.01'])]:
            table='r44_'+suffix;ok(f'CREATE TEMP TABLE {table}(id INT,p TEXT,_win_0 {declaration},_win_1 INT,_win_2 INT)')
            def literal(value):
                if value is None:return 'NULL'
                return "'"+value.replace("'","''")+"'" if oid==25 else value
            ok('INSERT INTO '+table+' VALUES'+','.join(
                f"({i+1},'{('a' if i<3 else 'b')}',{literal(v)},{(i+1)*10},{(i+1)*1000})" for i,v in enumerate(values)))
            window='PARTITION BY p ORDER BY id'
            physical=['_win_0','_win_1','_win_2'];sourceoids=[oid,23,23]
            originals=[[str(i+1),values[i],str((i+1)*10),str((i+1)*1000)] for i in range(6)]
            native=f'SELECT id,row_number() OVER({window}) AS rn,_win_0,_win_1,_win_2 FROM {table} ORDER BY id'
            control(native,[[row[0],str(i%3+1)]+row[1:] for i,row in enumerate(originals)],['id','rn']+physical,[23,20]+sourceoids)
            body=f'SELECT id,row_number() OVER({window}) AS rn,rank() OVER({window}) AS rank,nth_value(_win_0,1) OVER({window}) AS first,_win_0,_win_1,_win_2 FROM {table}'
            rows=[[str(i+1),str(i%3+1),str(i%3+1),values[0 if i<3 else 3]]+originals[i][1:] for i in range(6)]
            positive=[i for i in range(6) if values[i] is not None];nulls=[i for i in range(6) if values[i] is None]
            key=lambda i: values[i] if oid==25 else Decimal(values[i])
            orders=[('ORDER BY id',list(range(6))),('ORDER BY _win_1 DESC,id',list(range(5,-1,-1))),
                ('ORDER BY _win_0 NULLS FIRST,id',nulls+sorted(positive,key=key)),
                ('ORDER BY _win_0 DESC NULLS LAST,id',sorted(positive,key=key,reverse=True)+nulls),
                ('ORDER BY id LIMIT 2 OFFSET 1',[1,2]),('ORDER BY id LIMIT 0',[])]
            for order,indices in orders:
                control(body+' '+order,[rows[i] for i in indices],['id','rn','rank','first']+physical,[23,20,20,oid]+sourceoids)
            control(f'SELECT id,nth_value(_win_0,1) OVER({window}) AS first,_win_0 FROM {table} ORDER BY id',
                [[str(i+1),values[0 if i<3 else 3],v] for i,v in enumerate(values)],['id','first','_win_0'],[23,oid,oid])
            control(f'SELECT id,row_number() OVER({window}) AS rn,lag(_win_0,0) OVER({window}) AS shifted,nth_value(_win_1,1) OVER({window}) AS first FROM {table} ORDER BY id',
                [[str(i+1),str(i%3+1),v,'10' if i<3 else '40'] for i,v in enumerate(values)],['id','rn','shifted','first'],[23,20,oid,23])
            control(f'SELECT id,row_number() OVER({window}) AS rn,rank() OVER({window}) AS rank,ntile(_win_1) OVER({window}) AS bucket,nth_value(id,1) OVER({window}) AS first,_win_1,_win_2 FROM {table} ORDER BY id',
                [[str(i+1),str(i%3+1),str(i%3+1),str(i%3+1),'1' if i<3 else '4',str((i+1)*10),str((i+1)*1000)] for i in range(6)],
                ['id','rn','rank','bucket','first','_win_1','_win_2'],[23,20,20,23,23,23,23])
            control(f'SELECT DISTINCT nth_value(_win_0,1) OVER({window}) AS first FROM {table} ORDER BY 1',[[values[0]],[None]],['first'],[oid])
            control(f'SELECT id,_win_0,_win_1,_win_2 FROM {table} ORDER BY id',originals,['id']+physical,[23]+sourceoids)
        print('WINDOW_INTERNAL_NAMESPACE_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks==39 and not failures,failures
    finally:
        if server:r.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
