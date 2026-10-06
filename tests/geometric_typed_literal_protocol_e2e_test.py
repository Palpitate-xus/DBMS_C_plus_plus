#!/usr/bin/env python3
"""Original geometric typed-literal SQL and analysis-time input controls."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('geometry_literal_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference18='--reference18' in sys.argv[1:]
    if reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120);client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('GEOMETRY_LITERAL',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('GEOMETRY_LITERAL_FAILURE',failures[-1],flush=True)
    try:
        if reference18:runner.verify_reference_version(client,sock)
        for kind,value,oid in (
            ('point','(1,2)',600),('line','{1,2,3}',628),('lseg','[(0,0),(1,2)]',601),
            ('box','(2,3),(0,0)',603),('path','[(0,0),(1,2)]',602),
            ('polygon','((0,0),(1,0),(0,1))',604),('circle','<(1,2),3>',718)):
            expression="CASE true WHEN true THEN "+kind+" '"+value+"' ELSE NULL END"
            for sql in ('SELECT '+expression+';','VALUES('+expression+');'):
                result=query(sql);check(sql+' state',result[1],None);check(sql+' rows',result[0],[[value]]);check(sql+' OID',result[5],[oid])
            sql='SELECT '+expression+' WHERE false;';result=query(sql)
            check(sql+' state',result[1],None);check(sql+' rows',result[0],[]);check(sql+' OID',result[5],[oid])
            check(kind+' invalid discarded input',query("SELECT CASE true WHEN true THEN 1 ELSE CASE "+kind+" 'bad' WHEN NULL THEN 2 ELSE 3 END END WHERE false;")[1],'22P02')
        for sql in ('CREATE TEMP SEQUENCE geometry_literal_preflight;','CREATE TEMP TABLE geometry_literal_rows(id BIGINT);'):
            check(sql,query(sql)[1],None)
        sql="WITH w AS (INSERT INTO geometry_literal_rows VALUES(nextval('geometry_literal_preflight')) RETURNING id) SELECT CASE PATH 'bad' WHEN NULL THEN 1 ELSE 2 END FROM w;"
        check('input before writing CTE',query(sql)[1],'22P02')
        check('input preflight effects',query("SELECT currval('geometry_literal_preflight');")[1],'55000')
        assert not failures,'%d geometry literal failures: %r'%(len(failures),failures)
        print('[GEOMETRY TYPED LITERAL '+('PG18.6 REFERENCE' if reference18 else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference18:sock.close()
        else:runner.stop_ours(server)
if __name__=='__main__':main()
