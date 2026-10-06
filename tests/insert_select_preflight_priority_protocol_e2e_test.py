#!/usr/bin/env python3
"""Whole INSERT SELECT analysis retains input-versus-WHERE error order."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('insert_priority_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference18='--reference18' in sys.argv[1:]
    if reference18 and '--reference' in sys.argv[1:]:
        raise ValueError('choose PostgreSQL18.6 reference or PG17.2 diagnostic, not both')
    reference=reference18 or '--reference' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password); server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('INSERT_PREFLIGHT',sql,result,flush=True); return result
    def check(label,actual,expected):
        if actual!=expected:
            failures.append((label,actual,expected)); print('INSERT_PREFLIGHT_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        if reference18:
            runner.verify_reference_version(client,sock)
        elif reference:
            assert query('SHOW server_version_num;')[0]==[['170002']]
        setup('CREATE TEMP TABLE insert_priority(id INT,v INTERVAL);')
        for label,sql,state in (
            ('typed-cast-before-where',"INSERT INTO insert_priority SELECT 1,CAST('2147483648 months' AS INTERVAL) WHERE missing_priority_fn(1)=1;",'22015'),
            ('typed-literal-before-where',"INSERT INTO insert_priority SELECT 1,INTERVAL '1 fortnight' WHERE missing_priority_fn(1)=1;",'22007'),
            ('unknown-input-after-where',"INSERT INTO insert_priority SELECT 1,'2147483648 months' WHERE missing_priority_fn(1)=1;",'42883'),
            ('valid-input-missing-where',"INSERT INTO insert_priority SELECT 1,'1 day' WHERE missing_priority_fn(1)=1;",'42883'),
            ('select-path-unchanged','SELECT 1 WHERE missing_priority_fn(1)=1;','42883'),
            ('update-path-unchanged',"UPDATE insert_priority SET v='1 day' WHERE missing_priority_fn(1)=1;",'42883'),
            ('delete-path-unchanged','DELETE FROM insert_priority WHERE missing_priority_fn(1)=1;','42883'),
            ('raw-with-not-exempt','WITH input_rows AS(SELECT 1 AS id) SELECT id FROM input_rows WHERE missing_priority_fn(1)=1;','42883'),
            ('values-path-not-exempt',"INSERT INTO insert_priority VALUES(1,CAST('1 fortnight' AS INTERVAL));",'22007'),
        ):
            check(label,query(sql)[1],state)
            check(label+'-no-write',query('SELECT id FROM insert_priority;')[0],[])
        result=query("INSERT INTO insert_priority SELECT 1,INTERVAL '1 day' WHERE false;")
        check('valid-empty-command',(result[1],result[4]),(None,'INSERT 0 0'))
        setup("INSERT INTO insert_priority SELECT 1,'1 us';")
        result=query('SELECT id,v FROM insert_priority;')
        check('valid-values',result[0],[['1','00:00:00.000001']])
        check('valid-types',result[5],[23,1186])
        setup('BEGIN;')
        check('transaction-input',query("INSERT INTO insert_priority SELECT 2,CAST('2147483648 months' AS INTERVAL) WHERE missing_priority_fn(1)=1;")[1],'22015')
        check('transaction-aborted',query('SELECT 1;')[1],'25P02')
        setup('ROLLBACK;')
        check('transaction-original-rows',query('SELECT id FROM insert_priority;')[0],[['1']])
        assert not failures, '%d INSERT SELECT preflight assertions failed: %r' % (len(failures),failures)
        print('[INSERT SELECT PREFLIGHT '+('PG18.6 REFERENCE' if reference18 else 'PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__=='__main__': main()
