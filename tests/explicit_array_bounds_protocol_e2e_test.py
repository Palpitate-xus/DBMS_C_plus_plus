#!/usr/bin/env python3
"""Explicit SQL array bounds retain values, metadata and atomicity."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('array_bounds_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,database,password=r._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=r.wire_timeout())
        c.startup_reference(sock,user,database,password=password);r.verify_reference_version(c,sock)
    else:server=r.start_ours(c);sock=server['sock']
    failures=[]
    def query(sql,rows=None,oids=None,state=None,ready=b'I'):
        messages=c.simple_query(sock,sql);actual=r.decode_wire_result(messages,include_types=True)
        statuses=[body for kind,body in messages if kind==b'Z']
        print('ARRAY_BOUNDS',sql,actual,'READY',statuses,flush=True)
        good=actual[1]==state and statuses==[ready]
        if rows is not None:good=good and actual[0]==rows
        if oids is not None:good=good and actual[5]==oids
        if not good:failures.append((sql,actual,statuses,rows,oids,state,ready))
        return actual
    one="'[0:2]={1,NULL,3}'::INT[]"
    two="'[-2:-1][3:4]={{1,NULL},{3,4}}'::INT[]"
    original=[['1','[0:2]={1,NULL,3}'],['2','[-2:-1][3:4]={{1,NULL},{3,4}}'],['3',None],['4','{}']]
    try:
        query(f'SELECT {one},{two},NULL::INT[],\'{{}}\'::INT[];',rows=[[original[0][1],original[1][1],None,'{}']],oids=[1007]*4)
        query("SELECT '[+01:+02]={01,02}'::INT[],'[2]={1,2}'::INT[],'{{},{}}'::INT[];",rows=[['{1,2}','{1,2}','{}']],oids=[1007]*3)
        query("SELECT '[0:1]={abcd,éééé}'::VARCHAR(3)[],'[0:1]={a,é}'::CHAR(3)[],'[-1:0]={1.234,NULL}'::NUMERIC(4,1)[];",rows=[['[0:1]={abc,ééé}','[0:1]={\"a  \",\"é  \"}','[-1:0]={1.2,NULL}']],oids=[1015,1014,1231])
        query(f'SELECT array_dims({one}),array_lower({one},1),array_upper({one},1),array_length({one},1),array_ndims({one}),cardinality({one});',rows=[['[0:2]','0','2','3','1','3']],oids=[25,23,23,23,23,23])
        query(f'SELECT array_dims({two}),array_lower({two},1),array_upper({two},1),array_lower({two},2),array_upper({two},2),array_length({two},2),array_ndims({two}),cardinality({two});',rows=[['[-2:-1][3:4]','-2','-1','3','4','2','2','4']],oids=[25]+[23]*7)
        query(f'SELECT ({one})[0],({one})[1],({one})[-1],({one})[3],({two})[-2][3],({two})[-2][4],({two})[-1][4],({two})[-2];',rows=[['1',None,None,None,'1',None,'4',None]],oids=[23]*8)
        query(f'SELECT ({one})[0:1],array_position({one},NULL),array_position({one},3,1),3=ANY({one});',rows=[['{1,NULL}','1','2','t']],oids=[1007,23,23,16])
        query(f'SELECT unnest({one});',rows=[['1'],[None],['3']],oids=[23])
        query(f'SELECT array_append({one},4),array_prepend(4,{one}),{one}||ARRAY[4],4||{one};',rows=[['[0:3]={1,NULL,3,4}','[0:3]={4,1,NULL,3}']*2],oids=[1007]*4)
        query("SELECT '[0:1]={1,2}'::INT[]||'[5:6]={3,4}'::INT[];",rows=[['[0:3]={1,2,3,4}']],oids=[1007])
        query("SELECT ARRAY['[0:1]={1,2}'::INT[],'[0:1]={3,4}'::INT[]];",rows=[['[1:2][0:1]={{1,2},{3,4}}']],oids=[1007])
        query("SELECT '[0:1]={1,2}'::INT[]='[1:2]={1,2}'::INT[],'[0:1]={1,2}'::INT[]<ARRAY[1,2];",rows=[['f','t']],oids=[16,16])
        for literal,state in [('[0:1]={1}','22P02'),('[0:1][2:3]={1,2}','22P02'),('[2:1]={}','2202E'),('[2147483648:2147483648]={1}','54000'),('[2147483647:2147483647]={1}','54000'),('[-2147483648:0]={1}','54000'),('[0 :1]={1,2}','22P02'),('[0:1]{1,2}','22P02'),('[0:1]={{1,2},{3}}','22P02'),('[1][1][1][1][1][1][1]={{{{{{{1}}}}}}}','54000')]:
            query("SELECT '"+literal+"'::INT[];",rows=[],state=state)
        query("SELECT '[0:1]={1,2147483648}'::INT[];",rows=[],state='22003')
        query("SELECT array_append('[2147483646:2147483646]={1}'::INT[],2);",rows=[],state='54000')
        query("SELECT ARRAY['[0:1]={1,2}'::INT[],ARRAY[3,4]];",rows=[],state='2202E')
        query('CREATE TABLE explicit_bounds_rows(id INT PRIMARY KEY,a INT[]);')
        query(f'INSERT INTO explicit_bounds_rows VALUES(1,{one}),(2,{two}),(3,NULL),(4,ARRAY[]::INT[]);')
        query('CREATE INDEX explicit_bounds_idx ON explicit_bounds_rows(a);')
        query('SELECT id,a FROM explicit_bounds_rows ORDER BY id;',rows=original,oids=[23,1007])
        query(f'SELECT id,a FROM explicit_bounds_rows WHERE a={one};',rows=[original[0]],oids=[23,1007])
        query("INSERT INTO explicit_bounds_rows VALUES(5,'[0:1]={8,9}'::INT[]),(6,'[0:1]={1}'::INT[]);",rows=[],state='22P02')
        query('SELECT id,a FROM explicit_bounds_rows ORDER BY id;',rows=original,oids=[23,1007])
        query('BEGIN;',ready=b'T');query('SAVEPOINT bounds_parent;',ready=b'T')
        query('ALTER TABLE explicit_bounds_rows ALTER COLUMN a TYPE BIGINT[];',ready=b'T')
        query('SELECT id,a FROM explicit_bounds_rows ORDER BY id;',rows=original,oids=[23,1016],ready=b'T')
        query('ROLLBACK TO bounds_parent;',ready=b'T')
        query('SELECT id,a FROM explicit_bounds_rows ORDER BY id;',rows=original,oids=[23,1007],ready=b'T')
        query('COMMIT;')
        query('ALTER TABLE explicit_bounds_rows ALTER COLUMN a TYPE BIGINT[];')
        query("INSERT INTO explicit_bounds_rows VALUES(5,'[-1:0]={1,2147483648}'::BIGINT[]);")
        query('ALTER TABLE explicit_bounds_rows ALTER COLUMN a TYPE INT[];',rows=[],state='22003')
        query('SELECT id,a FROM explicit_bounds_rows ORDER BY id;',rows=original+[['5','[-1:0]={1,2147483648}']],oids=[23,1016])
        print('ARRAY_BOUNDS_FAILURES',failures,flush=True);assert not failures,failures
    finally:
        try:c.simple_query(sock,'ROLLBACK;');c.simple_query(sock,'DROP TABLE IF EXISTS explicit_bounds_rows;')
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
