#!/usr/bin/env python3
"""Geometric CAST input must fail in analysis and in genuine dynamic execution."""
import importlib.util
import socket
import sys
from pathlib import Path

SHAPES = (
    ('point','(1,2)',600), ('line','{1,2,3}',628), ('lseg','[(0,0),(1,2)]',601),
    ('box','(2,3),(0,0)',603), ('path','[(0,0),(1,2)]',602),
    ('polygon','((0,0),(1,0),(0,1))',604), ('circle','<(1,2),3>',718),
)


def main():
    directory=Path(__file__).resolve().parent
    spec=importlib.util.spec_from_file_location('geometric_cast_runner',directory/'compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference=sys.argv[1:]==['--reference18']
    assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    failures=[]
    def query(sql,state=None,rows=None,types=None):
        actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print('GEOMETRIC_CAST',sql,actual,flush=True)
        if actual[1]!=state:failures.append((sql,'state',actual[1],state))
        if rows is not None and actual[0]!=rows:failures.append((sql,'rows',actual[0],rows))
        if types is not None and actual[5]!=types:failures.append((sql,'types',actual[5],types))
        if state and (actual[0] or actual[4] is not None):failures.append((sql,'partial publication',actual))
        return actual
    try:
        for sql in ('BEGIN','CREATE TEMP TABLE geometry_cast_rows(id INT,v TEXT)',
                    "INSERT INTO geometry_cast_rows VALUES(1,'bad'),(2,NULL)",
                    'CREATE TEMP SEQUENCE geometry_cast_effects'):
            query(sql+';');assert not failures,failures
        counter=1
        def invalid(sql):
            nonlocal counter
            query('SAVEPOINT geometry_cast_case;')
            query(sql+';','22P02')
            query('ROLLBACK TO geometry_cast_case;')
            query("SELECT nextval('geometry_cast_effects');",rows=[[str(counter)]],types=[20])
            counter+=1
        for kind,value,oid in SHAPES:
            invalid("SELECT CAST('bad' AS "+kind+") WHERE false")
            invalid("SELECT 'bad'::"+kind+" WHERE false")
            invalid("SELECT CAST('bad' AS pg_catalog.\""+kind+"\") WHERE false")
            invalid("SELECT CASE WHEN false THEN CAST('bad' AS "+kind+") ELSE NULL::"+kind+" END")
            invalid('SELECT CAST(v AS '+kind+') FROM geometry_cast_rows WHERE id=1')
            query("SELECT CAST('"+value+"' AS "+kind+');',rows=[[value]],types=[oid])
            query("SELECT CAST('"+value+"' AS pg_catalog.\""+kind+'\");',rows=[[value]],types=[oid])
            query("SELECT '"+value+"'::"+kind+';',rows=[[value]],types=[oid])
            query('SELECT CAST(v AS '+kind+') FROM geometry_cast_rows WHERE id=2;',rows=[[None]],types=[oid])
        invalid("SELECT CASE NULL::PATH WHEN CAST('bad' AS PATH) THEN 1 ELSE 2 END")
        invalid("SELECT CASE NULL::PATH WHEN CAST('bad' AS PATH) THEN 1 ELSE 2 END WHERE false")
        invalid("WITH w AS(INSERT INTO geometry_cast_rows VALUES(nextval('geometry_cast_effects'),'x') RETURNING id) "
                "SELECT CASE NULL::PATH WHEN CAST('bad' AS PATH) THEN 1 ELSE 2 END FROM w")
        query('SELECT id,v FROM geometry_cast_rows ORDER BY id;',rows=[['1','bad'],['2',None]],types=[23,25])
        query('ROLLBACK;');print('GEOMETRIC_CAST_FAILURES',failures,flush=True)
        assert not failures,failures
    finally:
        try:client.simple_query(sock,'ROLLBACK;')
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
