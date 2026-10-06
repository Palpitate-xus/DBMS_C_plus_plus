#!/usr/bin/env python3
"""Original geometric DISTINCT controls and NULL/effect demand boundaries."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('distinct_eq_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    cases_spec=importlib.util.spec_from_file_location('geometry_cases',root/'tests/geometric_equality_protocol_e2e_test.py')
    geometry=importlib.util.module_from_spec(cases_spec);cases_spec.loader.exec_module(geometry)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120);client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('DISTINCT_EQUALITY',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('DISTINCT_EQUALITY_FAILURE',failures[-1],flush=True)
    def rows(sql,expected,oid=23):
        result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],expected);check(sql+' OID',result[5],[oid])
    try:
        if reference18:
            runner.verify_reference_version(client,sock)
            schema='distinct_equality_'+uuid.uuid4().hex
            setup=runner.decode_wire_result(client.simple_query(sock,'BEGIN; CREATE SCHEMA '+schema+'; SET search_path='+schema+',public;'),include_types=True)
            assert setup[1] is None,setup
        for kind,left,right,value in geometry.CASES:
            # These are the exact eighteen original full-matrix queries.
            rows("SELECT CASE WHEN "+kind+" '"+left+"' IS NOT DISTINCT FROM "+kind+" '"+right+"' THEN 1 ELSE 2 END;",[[str(value)]])
            expression="CASE WHEN "+kind+" '"+left+"' IS DISTINCT FROM "+kind+" '"+right+"' THEN true ELSE false END"
            rows('SELECT '+expression+';',[['f' if value==1 else 't']],16)
            rows('VALUES('+expression+');',[['f' if value==1 else 't']],16)
            rows('SELECT '+expression+' WHERE false;',[],16)
        for kind in ('path','circle','line','lseg'):
            value={'path':'[(0,0)]','circle':'<(0,0),1>','line':'{1,2,3}','lseg':'[(0,0),(1,1)]'}[kind]
            rows('SELECT CASE WHEN CAST(NULL AS '+kind+') IS NOT DISTINCT FROM CAST(NULL AS '+kind+') THEN 1 ELSE 2 END;',[['1']])
            rows("SELECT CASE WHEN CAST(NULL AS "+kind+") IS DISTINCT FROM "+kind+" '"+value+"' THEN 1 ELSE 2 END;",[['1']])
            rows("SELECT CASE WHEN "+kind+" '"+value+"' IS DISTINCT FROM CAST(NULL AS "+kind+") THEN 1 ELSE 2 END;",[['1']])
        check('source fixture',query("CREATE TEMP TABLE distinct_geom_rows(p PATH);INSERT INTO distinct_geom_rows VALUES('[(0,0),(1,1)]'),(NULL);")[1],None)
        rows("SELECT CASE WHEN p IS NOT DISTINCT FROM PATH '[(2,2),(3,3)]' THEN 1 ELSE 2 END FROM distinct_geom_rows;",[['1'],['2']])
        for sql in ('CREATE TEMP SEQUENCE distinct_eq_calls;',
                    "CREATE FUNCTION distinct_eq_tick() RETURNS PATH AS $$DECLARE tick BIGINT;BEGIN tick:=nextval('distinct_eq_calls');RETURN CAST('[(2,2),(3,3)]' AS PATH);END$$ LANGUAGE plpgsql;"):
            check(sql,query(sql)[1],None)
        rows('SELECT CASE WHEN p IS NOT DISTINCT FROM distinct_eq_tick() THEN 1 ELSE 2 END FROM distinct_geom_rows;',[['1'],['2']])
        rows("SELECT currval('distinct_eq_calls');",[['2']],20)
        rows('SELECT CASE WHEN CAST(NULL AS PATH) IS DISTINCT FROM distinct_eq_tick() THEN 1 ELSE 2 END;',[['1']])
        rows("SELECT currval('distinct_eq_calls');",[['3']],20)
        rows('SELECT CASE WHEN p IS DISTINCT FROM distinct_eq_tick() THEN 1 ELSE 2 END FROM distinct_geom_rows WHERE false;',[])
        rows("SELECT currval('distinct_eq_calls');",[['3']],20)
        assert not failures,'%d distinct equality failures: %r'%(len(failures),failures)
        print('[DISTINCT EQUALITY '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:
            client.simple_query(sock,'ROLLBACK;');sock.close()
        else:runner.stop_ours(server)
if __name__=='__main__':main()
