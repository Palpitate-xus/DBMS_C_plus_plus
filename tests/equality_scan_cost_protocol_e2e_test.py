#!/usr/bin/env python3
"""Ordinary literal scan costing changes actual paths, preserving full rows."""
import importlib.util
import json
import os
import socket
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('equality_scan_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();server=runner.start_ours(client)
    other=None
    trace={'complete':False,'commands':[],'server_pid':server['process'].pid,
           'actual_exe':str(Path('/proc/'+str(server['process'].pid)+'/exe').resolve())}
    evidence=Path(os.environ['DBMS_TEST_EVIDENCE_DIR'])if os.environ.get('DBMS_TEST_EVIDENCE_DIR')else None
    def query(sql,sock=None,ready=b'I'):
        messages=client.simple_query(sock or server['sock'],sql)
        rows,state,message,headers,tag=runner.decode_wire_result(messages)
        trace['commands'].append({'sql':sql,'rows':rows,'state':state,'tag':tag,
                                  'raw':[{'kind':k.decode(),'body_hex':b.hex()}for k,b in messages]})
        assert state is None,(sql,state,message)
        assert messages[-1]==(b'Z',ready)
        return rows
    def nodes(node):
        yield node
        for child in node.get('children',[]):yield from nodes(child)
    def explain(sql,kind):
        document=json.loads('\n'.join(row[0]for row in query('EXPLAIN (FORMAT JSON) '+sql)))
        actual=list(nodes(document['plan']));assert any(n['nodeType']==kind for n in actual),(sql,document)
        if kind=='TableScan':assert not any(n['nodeType']=='IndexScan'for n in actual),(sql,document)
        return document
    rows=[[str(i),None if i<128 else'7'if i<928 else str(i+1000),
           None if i%17==0 else'payload 界'if i%7 else'']for i in range(1024)]
    def literal(v):return 'NULL'if v is None else "'"+v.replace("'","''")+"'"
    try:
        query('ALTER SYSTEM SET auto_analyze=off')
        query('CREATE TABLE equality_scan_wire(id integer PRIMARY KEY,k integer,pad text)')
        query('INSERT INTO equality_scan_wire VALUES'+','.join('('+','.join(literal(v)for v in rows[(i*37)%1024])+')'for i in range(1024)))
        query('CREATE INDEX equality_scan_wire_k ON equality_scan_wire(k)')
        query('ANALYZE equality_scan_wire')
        path=Path(server['dir'])/'info';before={n:(path/n).read_bytes()for n in['.stats','.analyzed_statistics']}
        def select(value):return 'SELECT id,k,pad FROM equality_scan_wire WHERE k='+value+' ORDER BY id'
        hot=explain(select('7'),'TableScan');assert hot['totalRows']==800,hot
        for value in['7','0007','2023','42']:
            explain(select(value),'TableScan'if int(value)==7 else'IndexScan')
            wanted=sorted((r for r in rows if r[1]is not None and int(r[1])==int(value)),key=lambda r:int(r[0]))
            assert query(select(value))==wanted,(value,'all values and NULL population')
        query('SET random_page_cost=0.000001')
        indexed=explain(select('7'),'IndexScan');assert indexed['totalRows']==800,indexed
        assert query(select('7'))==[r for r in rows if r[1]=='7']
        # These two costs differ beyond six decimal places and straddle
        # the actual ordinary path boundary. Reuse the identical SQL text.
        query('SET seq_page_cost=0')
        query('SET random_page_cost=0.00780000001')
        explain(select('7'),'TableScan')
        query('SET random_page_cost=0.00779999999')
        explain(select('7'),'IndexScan')
        query('SET seq_page_cost=1')
        query('SET random_page_cost=4')
        explain(select('7'),'TableScan')
        zero=query('SELECT id FROM equality_scan_wire WHERE k IS NULL ORDER BY id')
        assert zero==[[str(i)]for i in range(128)]
        # EXPLAIN and ORDER BY alone do not prove that ordinary queries use
        # the chosen physical scan. Check real access counters and all rows.
        def scans():
            entry=next(r for r in query('SELECT * FROM pg_catalog.pg_stat_tables')
                       if r[0]=='equality_scan_wire')
            return [int(v)for v in entry[1:5]]
        access=[]
        for value in ['7','2023','42']:
            previous=scans()
            actual=query('SELECT id,k,pad FROM equality_scan_wire WHERE k='+value)
            wanted=[r for r in rows if r[1]is not None and int(r[1])==int(value)]
            assert sorted(actual,key=lambda r:int(r[0]))==wanted,(value,'unordered full population')
            current=scans()
            delta=[b-a for a,b in zip(previous,current)]
            if value=='7':
                assert delta==[1,1024,0,0],(value,delta)
            else:
                assert delta==[0,0,1,len(wanted)],(value,delta)
            access.append({'value':value,'before':previous,'after':current,'delta':delta})
        assert all((path/n).read_bytes()==raw for n,raw in before.items()),'planning/read execution never rewrites ANALYZE images'
        query('CREATE TABLE equality_or_wire(id integer,v text)')
        query("INSERT INTO equality_or_wire VALUES(1,'same'),(2,'same'),(3,'NULL'),(4,'line\nbreak'),(5,NULL),(6,''),(7,' 界 ')")
        or_values=[['same'],['same'],['NULL'],['line\nbreak'],[None],[''],[' 界 ']]
        assert query('SELECT v FROM equality_or_wire WHERE id<=4 OR id>=2')==or_values
        assert query('SELECT id,v FROM equality_or_wire WHERE id<=4 OR id>=2 ORDER BY id DESC')==[
            [str(i+1),v[0]]for i,v in reversed(list(enumerate(or_values)))]
        query('CREATE TABLE equality_snapshot_wire(id integer PRIMARY KEY,k integer,tag varchar(40))')
        query("INSERT INTO equality_snapshot_wire VALUES(1,7,'aaa'),(2,7,'bbb'),(3,8,NULL),(4,NULL,'ccc')")
        query('CREATE INDEX equality_snapshot_hash ON equality_snapshot_wire USING hash(k)')
        query('CREATE INDEX equality_snapshot_gist ON equality_snapshot_wire USING gist(tag)')
        paths=[('id=2 AND k=7','BitmapHeapScan'),
               ('id=2 OR k=99','BitmapOrHeapScan'),
               ("tag>='bbb' AND tag<='bbb'",'GiSTScan')]
        def snapshot_scans(ready=b'I'):
            entry=next(r for r in query('SELECT * FROM pg_catalog.pg_stat_tables',ready=ready)
                       if r[0]=='equality_snapshot_wire')
            return [int(v)for v in entry[1:5]]
        initial_access=[]
        for predicate,kind in paths:
            sql='SELECT id,k,tag FROM equality_snapshot_wire WHERE '+predicate
            if kind in ('BitmapOrHeapScan','GiSTScan'):
                # Existing disjunction/GiST EXPLAIN renderers omit the actual
                # access node. Preserve that observation and prove execution
                # using counters rather than the rendered text.
                query('EXPLAIN (FORMAT JSON) '+sql)
            else:explain(sql,kind)
            previous=snapshot_scans()
            assert query('SELECT id,k,tag FROM equality_snapshot_wire WHERE '+predicate)==[['2','7','bbb']]
            current=snapshot_scans()
            delta=[b-a for a,b in zip(previous,current)]
            assert delta==[0,0,1,1],(kind,'current snapshot uses actual index',delta)
            initial_access.append({'node':kind,'delta':delta})
        other=socket.create_connection(('127.0.0.1',server['port']),timeout=runner.wire_timeout())
        client.startup(other,'alice','info')
        query('BEGIN ISOLATION LEVEL REPEATABLE READ',ready=b'T')
        assert query('SELECT id,k,tag FROM equality_snapshot_wire WHERE id=2',ready=b'T')==[['2','7','bbb']]
        query("UPDATE equality_snapshot_wire SET id=20,k=70,tag='zzz' WHERE id=2",sock=other)
        snapshots=[]
        for predicate,kind in paths:
            previous=snapshot_scans(b'T')
            actual=query('SELECT id,k,tag FROM equality_snapshot_wire WHERE '+predicate,ready=b'T')
            assert actual==[['2','7','bbb']],(kind,'old indexed key remains visible',actual)
            current=snapshot_scans(b'T')
            delta=[b-a for a,b in zip(previous,current)]
            assert delta==[1,4,0,0],(kind,'historical snapshot uses complete heap',delta)
            snapshots.append({'node':kind,'delta':delta,'rows':actual})
        assert query('SELECT id,k,tag FROM equality_snapshot_wire WHERE id=20',ready=b'T')==[]
        assert query('SELECT id,k,tag FROM equality_snapshot_wire WHERE id=2 OR k=0',ready=b'T')==[['2','7','bbb']]
        assert query('SELECT id,k,tag FROM equality_snapshot_wire WHERE id=4 AND k=0',ready=b'T')==[]
        assert query("SELECT id,k,tag FROM equality_snapshot_wire WHERE tag>='' ORDER BY id",ready=b'T')==[
            ['1','7','aaa'],['2','7','bbb'],['4',None,'ccc']]
        query('COMMIT')
        assert query('SELECT id,k,tag FROM equality_snapshot_wire WHERE id=20')==[['20','70','zzz']]
        trace.update(complete=True,full_source_rows=1024,hot_rows=800,null_rows=128,
                     unordered_actual_access=access,
                     bitmap_AND_OR_GiST_current_access=initial_access,
                     bitmap_AND_OR_GiST_historical_snapshot=snapshots,
                     unindexed_OR_duplicate_rows_NULL_newline_empty_UTF8_preserved=True,
                     primitive_types_NULL_empty_UTF8_and_order_checked=True)
    finally:
        if other is not None:other.close()
        runner.stop_ours(server);trace['server_exit']=server['process'].returncode
        if evidence:
            evidence.mkdir(parents=True,exist_ok=True);(evidence/'trace-v1.json').write_text(json.dumps(trace,ensure_ascii=False,indent=2)+'\n')
        assert trace['server_exit']==0
    print('EQUALITY_SCAN_COST_PROTOCOL commands='+str(len(trace['commands']))+' full_rows_NULL_empty_UTF8=1 GUC_actual_access=1')


if __name__=='__main__':main()
