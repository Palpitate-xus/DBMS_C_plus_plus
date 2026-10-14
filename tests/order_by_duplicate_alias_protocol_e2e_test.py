#!/usr/bin/env python3
"""ORDER BY output-name ambiguity uses bound expressions, never row values."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('duplicate_alias_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,db,password=r._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=r.wire_timeout())
        c.startup_reference(sock,user,db,password);r.verify_reference_version(c,sock)
    else:server=r.start_ours(c);sock=server['sock']
    sequence='o05_alias_seq_'+uuid.uuid4().hex[:16];created=False
    checks=0;failures=[]
    def query(sql):return r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def control(sql,rows=None,headers=None,oids=None,state=None):
        nonlocal checks
        result=query(sql);checks+=1
        good=result[1]==state
        if state:good=good and result[0]==[] and result[4] is None
        else:good=good and result[0]==rows and result[3]==headers and result[5]==oids and result[4]=='SELECT '+str(len(rows))
        print('ORDER_ALIAS',sql,result,'PASS',good,flush=True)
        if not good:failures.append((sql,result,rows,headers,oids,state))
    try:
        ok('CREATE TEMP TABLE alias_rows(id INT,v INT,word TEXT)')
        ok("INSERT INTO alias_rows VALUES(1,30,'B'),(2,10,'a'),(3,20,NULL)")
        for target,key in [('id AS x,v AS x','x'),('id AS id,v AS id','id'),
                           ('id,v AS id','id'),('1 AS x,2 AS x','x'),
                           ('1.0 AS x,1.00 AS x','x'),('id AS x,id+0 AS x','x'),
                           ('id AS x,id::bigint AS x','x'),('sum(id) AS x,sum(v) AS x','x'),
                           ('row_number() OVER(ORDER BY id) AS x,row_number() OVER(ORDER BY v) AS x','x'),
                           ('row_number() OVER(ORDER BY id) AS x,row_number() OVER(ORDER BY t.id) AS x','x'),
                           ('(SELECT 1) AS x,(SELECT 2) AS x','x'),('id AS X,v AS "x"','x'),
                           ('t.*,v AS id','id'),
                           ("'a' AS x,'a'::text AS x",'x'),('NULL AS x,NULL::text AS x','x'),
                           ("'NULL' AS x,NULL AS x",'x'),
                           ('(SELECT 1 AS one) AS x,(SELECT 1 AS two) AS x','x'),
                           ('(SELECT 1) AS x,(SELECT 1::integer) AS x','x'),
                           ('1 AS x,1::numeric AS x','x'),('1+1 AS x,2 AS x','x'),
                           ('1e0 AS x,1.0 AS x','x')]:
            for suffix in ('',' WHERE FALSE',' LIMIT 0'):
                # LIMIT belongs after ORDER BY, WHERE before it.
                sql='SELECT '+target+' FROM alias_rows t'+(' WHERE FALSE' if suffix==' WHERE FALSE' else '')+' ORDER BY '+key
                if suffix==' LIMIT 0':sql+=suffix
                control(sql,state='42702')
        for target,rows,oids in [
                ('id AS x,t.id AS x',[[str(i),str(i)] for i in (1,2,3)],[23,23]),
                ('id AS x,id::integer AS x',[[str(i),str(i)] for i in (1,2,3)],[23,23]),
                ('1 AS x,01 AS x',[['1','1']]*3,[23,23]),
                ("1 AS x,'01'::integer AS x",[['1','1']]*3,[23,23]),
                ('1.00 AS x,01.00 AS x',[['1.00','1.00']]*3,[1700,1700]),
                ('lower(word) AS x,pg_catalog.lower(word) AS x',[['a','a'],['b','b'],[None,None]],[25,25]),
                ('(SELECT 1) AS x,(SELECT 01) AS x',[['1','1']]*3,[23,23]),
                ('row_number() OVER(ORDER BY id) AS x,row_number() OVER(ORDER BY id) AS x',
                 [[str(i),str(i)] for i in (1,2,3)],[20,20])]:
            for empty in (False,True):
                control('SELECT '+target+' FROM alias_rows t'+(' WHERE FALSE' if empty else '')+' ORDER BY x',
                        [] if empty else rows,['x','x'],oids)
        control('SELECT id AS "X",v AS x FROM alias_rows ORDER BY "X"',
                [['1','30'],['2','10'],['3','20']],['X','x'],[23,23])
        control('SELECT id AS x,v AS x FROM alias_rows ORDER BY 2',
                [['2','10'],['3','20'],['1','30']],['x','x'],[23,23])
        control('SELECT id AS x,v AS x FROM alias_rows ORDER BY x+0',state='42703')
        control('SELECT word AS x,word AS x FROM alias_rows ORDER BY x COLLATE "C"',state='42703')
        control('SELECT a.id AS x,b.id AS x FROM alias_rows a JOIN alias_rows b ON a.id=b.id ORDER BY x',state='42702')
        # Equal current row values do not make distinct source occurrences
        # equivalent. Conversely true duplicate outputs may share a volatile
        # producer only when that output name is referenced by ORDER BY.
        ok('CREATE SEQUENCE '+sequence);created=True
        expression="nextval('"+sequence+"')"
        for suffix in ('',' WHERE FALSE',' LIMIT 0'):
            ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
            sql='SELECT '+expression+' AS x,('+expression+'+0) AS x FROM alias_rows'+(' WHERE FALSE' if suffix==' WHERE FALSE' else '')+' ORDER BY x'
            if suffix==' LIMIT 0':sql+=suffix
            control(sql,state='42702')
            control('SELECT '+expression,[['1']],['nextval'],[20])
        ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
        control('SELECT '+expression+' AS x,'+expression+' AS x FROM alias_rows ORDER BY x',
                [['1','1'],['2','2'],['3','3']],['x','x'],[20,20])
        control('SELECT '+expression,[['4']],['nextval'],[20])
        ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
        control('SELECT '+expression+' AS x,'+expression+' AS x FROM alias_rows ORDER BY x LIMIT 1',
                [['1','1']],['x','x'],[20,20])
        control('SELECT '+expression,[['4']],['nextval'],[20])
        ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
        control('SELECT '+expression+' AS x,'+expression+' AS x FROM alias_rows',
                [['1','2'],['3','4'],['5','6']],['x','x'],[20,20])
        control('SELECT '+expression,[['7']],['nextval'],[20])
        for function,oid,independent,shared,independent_next,shared_next in [
                ('lag',20,[[None,None],['1','2'],['3','4']],[['1','1'],['2','2'],[None,None]],'5','3')]:
            for ordered in (False,True):
                ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
                call=function+'('+expression+') OVER(ORDER BY id)'
                control('SELECT '+call+' AS x,'+call+' AS x FROM alias_rows'+(' ORDER BY x' if ordered else ''),
                        shared if ordered else independent,['x','x'],[oid,oid])
                control('SELECT '+expression,[[shared_next if ordered else independent_next]],['nextval'],[20])
        for left,right in [('row_number() OVER w','row_number() OVER(ORDER BY id)'),
                           ('row_number() OVER w','row_number() OVER w')]:
            control('SELECT '+left+' AS x,'+right+' AS x FROM alias_rows WINDOW w AS(ORDER BY id) ORDER BY x',
                    [['1','1'],['2','2'],['3','3']],['x','x'],[20,20])
        ok('ALTER SEQUENCE '+sequence+' RESTART WITH 1')
        frame='ORDER BY id ROWS '+expression+' PRECEDING'
        control('SELECT sum(v) OVER w AS x,sum(v) OVER('+frame+') AS x FROM alias_rows WINDOW w AS('+frame+') ORDER BY x',
                [['30','30'],['30','30'],['40','40']],['x','x'],[20,20])
        control('SELECT '+expression,[['2']],['nextval'],[20])
        for suffix in ('',' WHERE FALSE',' LIMIT 0'):
            where=' WHERE FALSE' if suffix==' WHERE FALSE' else ''
            limit=suffix if suffix==' LIMIT 0' else ''
            for offset in ('(WITH q AS(SELECT t.id AS n) SELECT n FROM q)',
                           '(SELECT q.n FROM(SELECT t.id AS n) q)'):
                control('SELECT sum(v) OVER w AS x,sum(v) OVER w AS x FROM alias_rows t'+where+
                        ' WINDOW w AS(ORDER BY id ROWS '+offset+' PRECEDING) ORDER BY x'+limit,state='42P10')
            control('SELECT row_number() OVER absent AS x,row_number() OVER absent AS x FROM alias_rows'+where+
                    ' ORDER BY x'+limit,state='42704')
            control('SELECT row_number() OVER w AS x,row_number() OVER w AS x FROM alias_rows'+where+
                    ' WINDOW w AS(),w AS() ORDER BY x'+limit,state='42P20')
        control('SELECT 1 AS healthy',[['1']],['healthy'],[23])
        print('ORDER_ALIAS_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks==117 and not failures,failures
    finally:
        if created:ok('DROP SEQUENCE '+sequence)
        if server:r.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
