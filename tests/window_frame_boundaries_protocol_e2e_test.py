#!/usr/bin/env python3
"""Directional ROWS/GROUPS/RANGE bounds, empty frames and error phases."""
import importlib.util
import socket
import sys
from decimal import Decimal
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('window_frames_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner);client=runner.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,db,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,db,password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server['sock']
    checks=0;failures=[]
    def query(sql):return runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    def control(sql,rows=None,headers=None,oids=None,state=None):
        nonlocal checks
        result=query(sql);checks+=1
        passed=result[1]==state
        if state:passed=passed and result[0]==[] and result[4] is None
        else:passed=passed and result[0]==rows and result[3]==headers and result[5]==oids and result[4]=='SELECT '+str(len(rows))
        print('WINDOW_FRAME',sql,result,'PASS',passed,flush=True)
        if not passed:failures.append((sql,result,rows,headers,oids,state))
    def bound(kind,offset):
        if kind=='ubp':return 'UNBOUNDED PRECEDING'
        if kind=='ubf':return 'UNBOUNDED FOLLOWING'
        if kind=='cur':return 'CURRENT ROW'
        return str(offset)+(' PRECEDING' if kind=='pre' else ' FOLLOWING')
    definitions=[('ubp',0,'cur',0),('cur',0,'ubf',0),('ubp',0,'ubf',0),
                 ('pre',1,'pre',1),('pre',2,'pre',1),('cur',0,'cur',0),
                 ('cur',0,'fol',2),('fol',1,'fol',2),('fol',2,'fol',1),
                 ('pre',3,'pre',5),('ubp',0,'pre',1),('fol',2,'ubf',0)]
    keys=[1,1,3,3,5,8];values=[10,10,30,30,50,80];groups=[0,0,1,1,2,3]
    def endpoint(kind,offset,current):
        if kind=='ubp':return Decimal('-Infinity')
        if kind=='ubf':return Decimal('Infinity')
        return current+(offset if kind=='fol' else -offset if kind=='pre' else 0)
    try:
        ok('CREATE TEMP TABLE window_frame_rows(id INT,k INT,v INT)')
        ok('INSERT INTO window_frame_rows VALUES'+','.join(f'({i+1},{keys[i]},{values[i]})' for i in range(6)))
        for mode in ('ROWS','GROUPS','RANGE'):
            coordinates=list(range(6)) if mode=='ROWS' else groups if mode=='GROUPS' else keys
            for start,so,end,eo in definitions:
                frame=mode+' BETWEEN '+bound(start,so)+' AND '+bound(end,eo)
                expected=[]
                for i in range(6):
                    lo=endpoint(start,so,coordinates[i]);hi=endpoint(end,eo,coordinates[i])
                    members=[j for j,c in enumerate(coordinates) if lo<=c<=hi]
                    data=[values[j] for j in members]
                    expected.append([str(i+1),str(sum(data)) if data else None,
                                     str(data[0]) if data else None,str(data[-1]) if data else None,
                                     str(data[1]) if len(data)>=2 else None])
                over='OVER(ORDER BY k '+frame+')'
                control('SELECT id,sum(v) '+over+' AS total FROM window_frame_rows ORDER BY id',
                        [row[:2] for row in expected],['id','total'],[23,20])
                control('SELECT id,nth_value(v,2) '+over+' AS second FROM window_frame_rows ORDER BY id',
                        [[row[0],row[4]] for row in expected],['id','second'],[23,23])
                # NTH requires the legacy adapter. All four functions must
                # consume the same correctly empty/directional frame there.
                control('SELECT id,sum(v) '+over+' AS total,first_value(v) '+over+' AS first,last_value(v) '+over+
                        ' AS last,nth_value(v,2) '+over+' AS second FROM window_frame_rows ORDER BY id',
                        expected,['id','total','first','last','second'],[23,20,23,23,23])
        # Exclusion also applies when CURRENT ROW itself is outside the frame.
        for mode in ('ROWS','GROUPS','RANGE'):
            coordinates=list(range(6)) if mode=='ROWS' else groups if mode=='GROUPS' else keys
            for start,so,end,eo in [('pre',1,'pre',1),('cur',0,'cur',0),('ubp',0,'cur',0)]:
                for exclusion in ('NO OTHERS','CURRENT ROW','GROUP','TIES'):
                    frame=mode+' BETWEEN '+bound(start,so)+' AND '+bound(end,eo)+' EXCLUDE '+exclusion
                    expected=[]
                    for i in range(6):
                        lo=endpoint(start,so,coordinates[i]);hi=endpoint(end,eo,coordinates[i])
                        members=[j for j,c in enumerate(coordinates) if lo<=c<=hi]
                        if exclusion=='CURRENT ROW':members=[j for j in members if j!=i]
                        if exclusion=='GROUP':members=[j for j in members if keys[j]!=keys[i]]
                        if exclusion=='TIES':members=[j for j in members if j==i or keys[j]!=keys[i]]
                        data=[values[j] for j in members]
                        expected.append([str(i+1),str(sum(data)) if data else None,
                                         str(data[0]) if data else None,str(data[-1]) if data else None,
                                         str(data[1]) if len(data)>=2 else None])
                    over='OVER(ORDER BY k '+frame+')'
                    control('SELECT id,sum(v) '+over+' AS total,first_value(v) '+over+' AS first,last_value(v) '+over+
                            ' AS last,nth_value(v,2) '+over+' AS second FROM window_frame_rows ORDER BY id',
                            expected,['id','total','first','last','second'],[23,20,23,23,23])
        numeric_keys=[Decimal('9007199254740993.00'),Decimal('9007199254740993.00'),
                      Decimal('9007199254740993.01'),None,Decimal('9007199254740993.03'),Decimal('9007199254740993.05')]
        numeric_values=[None,None,30,40,50,60]
        ok('CREATE TEMP TABLE window_frame_numeric(id INT,k NUMERIC,v INT)')
        ok('INSERT INTO window_frame_numeric VALUES'+','.join(
            '('+str(i+1)+','+('NULL' if numeric_keys[i] is None else str(numeric_keys[i]))+','+
            ('NULL' if numeric_values[i] is None else str(numeric_values[i]))+')' for i in range(6)))
        decimal_definitions=[('pre',Decimal('0.01'),'fol',Decimal('0.01')),
                             ('pre',Decimal('0.02'),'pre',Decimal('0.01')),
                             ('fol',Decimal('0.01'),'fol',Decimal('0.03')),
                             ('fol',Decimal('0.03'),'fol',Decimal('0.01')),
                             ('ubp',0,'cur',0),('cur',0,'ubf',0),('cur',0,'cur',0)]
        for ascending in (True,False):
            for nulls_first in (True,False):
                nonnull=sorted([i for i,k in enumerate(numeric_keys) if k is not None],
                               key=lambda i:numeric_keys[i],reverse=not ascending)
                nulls=[i for i,k in enumerate(numeric_keys) if k is None]
                order=nulls+nonnull if nulls_first else nonnull+nulls
                def compare(a,b):
                    if a is None or b is None:
                        return 0 if a is b else (-1 if (a is None)==nulls_first else 1)
                    return ((a>b)-(a<b))*(1 if ascending else -1)
                def threshold(kind,distance,current):
                    if current is None:return None
                    return current+(distance if (kind=='fol')==ascending else -distance) if kind in ('pre','fol') else current
                for start,so,end,eo in decimal_definitions:
                    for exclusion in ('NO OTHERS','CURRENT ROW','GROUP','TIES'):
                        frame='RANGE BETWEEN '+bound(start,so)+' AND '+bound(end,eo)+' EXCLUDE '+exclusion
                        over='OVER(ORDER BY k '+('ASC' if ascending else 'DESC')+' NULLS '+('FIRST' if nulls_first else 'LAST')+' '+frame+')'
                        expected=[]
                        for i,current in enumerate(numeric_keys):
                            lo=threshold(start,so,current);hi=threshold(end,eo,current)
                            members=[j for j in order if (start=='ubp' or compare(numeric_keys[j],lo)>=0) and
                                     (end=='ubf' or compare(numeric_keys[j],hi)<=0)]
                            if exclusion=='CURRENT ROW':members=[j for j in members if j!=i]
                            if exclusion=='GROUP':members=[j for j in members if numeric_keys[j]!=current]
                            if exclusion=='TIES':members=[j for j in members if j==i or numeric_keys[j]!=current]
                            data=[numeric_values[j] for j in members];numbers=[v for v in data if v is not None]
                            text=lambda v:None if v is None else str(v)
                            expected.append([str(i+1),text(sum(numbers)) if numbers else None,
                                             text(data[0]) if data else None,text(data[-1]) if data else None,
                                             text(data[1]) if len(data)>=2 else None])
                        control('SELECT id,sum(v) '+over+' AS total,first_value(v) '+over+' AS first,last_value(v) '+over+
                                ' AS last,nth_value(v,2) '+over+' AS second FROM window_frame_numeric ORDER BY id',
                                expected,['id','total','first','last','second'],[23,20,23,23,23])
        ok('CREATE TEMP TABLE window_frame_words(id INT,rows INT,range INT,groups INT,v INT)')
        ok('INSERT INTO window_frame_words VALUES(1,2,2,2,20),(2,1,1,1,10)')
        for column in ('rows','range','groups'):
            for frame in ('ROWS BETWEEN 1 PRECEDING AND CURRENT ROW','GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW',''):
                over='OVER(ORDER BY '+column+' '+frame+')'
                control('SELECT id,sum(v) '+over+' AS total,nth_value(v,2) '+over+
                        ' AS second FROM window_frame_words ORDER BY id',
                        [['1','30','20'],['2','10',None]],['id','total','second'],[23,20,23])
        for frame,state,limit_state in [
            ('ROWS BETWEEN CURRENT ROW AND 1 PRECEDING','42P20','42P20'),
            ('ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW','42P20','42P20'),
            ('ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING','42P20','42P20'),
            ('ROWS BETWEEN NULL PRECEDING AND CURRENT ROW','22004',None),
            ('ROWS BETWEEN -1 PRECEDING AND CURRENT ROW','22013',None),
            ('GROUPS BETWEEN NULL PRECEDING AND CURRENT ROW','22004',None),
            ('GROUPS BETWEEN -1 PRECEDING AND CURRENT ROW','22013',None),
            ('RANGE BETWEEN NULL PRECEDING AND CURRENT ROW','22004',None),
            ('RANGE BETWEEN -1 PRECEDING AND CURRENT ROW','22013',None),
            ('RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW','0A000','0A000')]:
            base='SELECT sum(v) OVER(ORDER BY k '+frame+') AS total FROM window_frame_rows'
            for suffix,error in [('',state),(' WHERE FALSE',state),(' LIMIT 0',limit_state)]:
                # RANGE rejects a negative distance in the in_range call,
                # which is not reached for an empty input. NULL is still an
                # initialization error, unlike this negative-distance case.
                if suffix==' WHERE FALSE' and frame.startswith('RANGE BETWEEN -1'):
                    error=None
                control(base+suffix,[] if error is None else None,['total'] if error is None else None,
                        [20] if error is None else None,error)
        # ROWS/GROUPS acquire bigint context; 1.5 rounds to two, strings
        # acquire that input type, and int64 offsets do not go through stoi.
        for token,offset in [('1.5',2),("'2'",2),('2::bigint',2),('9223372036854775807',9223372036854775807)]:
            expected=[[str(i+1),str(sum(values[max(0,i-offset):i])) if i else None] for i in range(6)]
            control('SELECT id,sum(v) OVER(ORDER BY id ROWS BETWEEN '+token+
                    ' PRECEDING AND 1 PRECEDING) AS total FROM window_frame_rows ORDER BY id',
                    expected,['id','total'],[23,20])
        # These references are hidden in a CTE/derived-source child, but
        # still belong to the current window's source. Reject during pure
        # binding, including before an empty child or parent LIMIT 0.
        for offset in ('(WITH q AS(SELECT t.id AS x) SELECT x FROM q)',
                       '(SELECT q.x FROM(SELECT t.id AS x) q)'):
            for suffix in ('',' WHERE FALSE',' LIMIT 0'):
                control('SELECT sum(v) OVER(ORDER BY id ROWS '+offset+
                        ' PRECEDING) AS total FROM window_frame_rows t'+suffix,state='42P10')
        for offset in ('(WITH q AS(SELECT 1 AS x) SELECT x FROM q)',
                       '(SELECT q.x FROM(SELECT 1 AS x) q)'):
            control('SELECT id,sum(v) OVER(ORDER BY id ROWS '+offset+
                    ' PRECEDING) AS total FROM window_frame_rows t ORDER BY id',
                    [[str(i+1),str(sum(values[max(0,i-1):i+1]))] for i in range(6)],
                    ['id','total'],[23,20])
        print('WINDOW_FRAME_COMPLETE',checks,'FAILURES',len(failures),flush=True)
        assert checks==307 and not failures,failures
    finally:
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
