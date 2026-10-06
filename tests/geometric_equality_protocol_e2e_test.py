#!/usr/bin/env python3
"""Genuine geometric equality signatures, typed values and runtime demand."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid
CASES=(
    ('path','[(0,0),(1,1)]','[(7,8),(9,10)]',1),
    ('path','[(0,0),(1,1)]','((7,8),(9,10))',1),
    ('path','[(0,0),(1,1)]','[(0,0)]',2),
    ('circle','<(0,0),3>','<(9,8),3>',1),
    ('circle','<(0,0),3>','<(0,0),4>',2),
    ('circle','<(0,0),1>','<(7,8),1.00000001>',1),
    ('circle','<(0,0),NaN>','<(0,0),NaN>',2),
    ('circle','<(0,0),Infinity>','<(1,1),Infinity>',1),
    ('line','{1,2,3}','{2,4,6}',1),('line','{1,2,3}','{-2,-4,-6}',1),
    ('line','{1,2,3}','{1,2,4}',2),('line','{1,2,3}','{1,2,3.0000001}',1),
    ('line','{NaN,2,3}','{NaN,2,3}',1),('line','{NaN,2,3}','{NaN,2,4}',2),
    ('lseg','[(0,0),(1,1)]','[(3,3),(4,4)]',2),
    ('lseg','[(0,0),(1,1)]','[(1,1),(0,0)]',2),
    ('lseg','[(0,0),(1,1)]','[(0,0),(1.0000001,1)]',1),
    ('lseg','[(NaN,0),(1,1)]','[(NaN,0),(1,1)]',1))
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('geometry_equality_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120);client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        if reference18:client.simple_query(server['sock'],'SAVEPOINT geometry_case;')
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        if reference18:
            if result[1] is not None:client.simple_query(server['sock'],'ROLLBACK TO SAVEPOINT geometry_case;')
            client.simple_query(server['sock'],'RELEASE SAVEPOINT geometry_case;')
        print('GEOMETRY_EQUALITY',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('GEOMETRY_EQUALITY_FAILURE',failures[-1],flush=True)
    def rows(sql,expected,oid=23):
        result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],expected);check(sql+' OID',result[5],[oid])
    try:
        if reference18:
            runner.verify_reference_version(client,sock)
            schema='geometry_equality_'+uuid.uuid4().hex
            setup=runner.decode_wire_result(client.simple_query(sock,'BEGIN; CREATE SCHEMA '+schema+'; SET search_path='+schema+',public;'),include_types=True)
            assert setup[1] is None,setup
        for kind,left,right,value in CASES:
            for cast in (False,True):
                a="CAST('"+left+"' AS "+kind+")" if cast else kind+" '"+left+"'"
                b="CAST('"+right+"' AS "+kind+")" if cast else kind+" '"+right+"'"
                expression='CASE '+a+' WHEN '+b+' THEN 1 ELSE 2 END'
                rows('SELECT '+expression+';',[[str(value)]])
                rows('VALUES('+expression+');',[[str(value)]])
                rows('SELECT '+expression+' WHERE false;',[])
        for kind,left,right,value in CASES:
            rows("SELECT CASE WHEN "+kind+" '"+left+"' = "+kind+" '"+right+"' THEN 1 ELSE 2 END;",[[str(value)]])
        rows("SELECT CASE WHEN CIRCLE '<(0,0),NaN>' <> CIRCLE '<(0,0),NaN>' THEN 1 ELSE 2 END;",[['2']])
        rows("SELECT CASE WHEN CIRCLE '<(0,0),3>' <> CIRCLE '<(7,8),3>' THEN 1 ELSE 2 END;",[['2']])
        rows("SELECT CASE WHEN NULLIF(PATH '[(0,0),(1,1)]',PATH '[(2,2),(3,3)]') IS NULL THEN 1 ELSE 2 END;",[['1']])
        for value in ('1e308','1e-200'):
            check('area range '+value,query("SELECT CASE CIRCLE '<(0,0),"+value+">' WHEN CIRCLE '<(1,1),"+value+">' THEN 1 ELSE 2 END WHERE false;")[1],'22003')
        for sql in ('CREATE TEMP TABLE geometry_eq_rows(p PATH,c CIRCLE,ln LINE,ls LSEG);',
                    "INSERT INTO geometry_eq_rows VALUES('[(0,0),(1,1)]','<(0,0),3>','{1,2,3}','[(0,0),(1,1)]'),(NULL,NULL,NULL,NULL);"):
            check(sql,query(sql)[1],None)
        rows("SELECT CASE p WHEN PATH '[(9,9),(8,8)]' THEN 1 ELSE 2 END FROM geometry_eq_rows;",[['1'],['2']])
        rows("SELECT CASE c WHEN CIRCLE '<(7,8),3>' THEN 1 ELSE 2 END FROM geometry_eq_rows;",[['1'],['2']])
        rows("SELECT CASE ln WHEN LINE '{2,4,6}' THEN 1 ELSE 2 END FROM geometry_eq_rows;",[['1'],['2']])
        rows("SELECT CASE ls WHEN LSEG '[(0,0),(1.0000001,1)]' THEN 1 ELSE 2 END FROM geometry_eq_rows;",[['1'],['2']])
        check('operator before zero rows',query("SELECT CASE PATH '[(0,0)]' WHEN CIRCLE '<(0,0),1>' THEN 1 ELSE 2 END WHERE false;")[1],'42883')
        for sql in ('CREATE TEMP SEQUENCE geometry_eq_calls;',
                    "CREATE FUNCTION geometry_eq_tick() RETURNS PATH AS $$DECLARE tick BIGINT;BEGIN tick:=nextval('geometry_eq_calls');RETURN CAST('[(9,9),(8,8)]' AS PATH);END$$ LANGUAGE plpgsql;"):
            check(sql,query(sql)[1],None)
        rows("SELECT CASE geometry_eq_tick() WHEN PATH '[(0,0)]' THEN 1 WHEN PATH '[(0,0),(1,1)]' THEN 2 ELSE 3 END;",[['2']])
        rows("SELECT currval('geometry_eq_calls');",[['1']],20)
        rows("SELECT CASE p WHEN geometry_eq_tick() THEN 1 ELSE 2 END FROM geometry_eq_rows;",[['1'],['2']])
        rows("SELECT currval('geometry_eq_calls');",[['3']],20)
        rows("SELECT CASE CAST(NULL AS PATH) WHEN geometry_eq_tick() THEN 1 ELSE 2 END;",[['2']])
        rows("SELECT currval('geometry_eq_calls');",[['3']],20)
        assert not failures,'%d geometric equality failures: %r'%(len(failures),failures)
        print('[GEOMETRY EQUALITY '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:
            client.simple_query(sock,'ROLLBACK;');sock.close()
        else:runner.stop_ours(server)
if __name__=='__main__':main()
