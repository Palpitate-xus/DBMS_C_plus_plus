#!/usr/bin/env python3
"""Malformed VALUES rows are rejected before an inserted prefix or input coercion."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('insert_syntax_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference='--reference' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password)
        server={'sock':sock}
    else:
        server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('INSERT_SYNTAX',sql,result,flush=True)
        return result
    def check(label,actual,expected):
        if actual!=expected:
            failures.append((label,actual,expected)); print('INSERT_SYNTAX_FAILURE',failures[-1],flush=True)
    try:
        if reference: assert query('SHOW server_version_num;')[0]==[['170002']]
        assert query('CREATE TEMP TABLE insert_syntax_rows(id INT PRIMARY KEY,v INTERVAL);')[1] is None
        assert query("INSERT INTO insert_syntax_rows VALUES(1,'1 day');")[1] is None
        for values in (
            "(3,'1 day')(4,'2 days')", "(3 '1 day')", "(3,'1 day' bogus)",
            "(3,'2147483648 months'),(4,'1 day' bogus)", "(3,'1 day'),",
            "(3,'1 day'", "(3,)", "(,3)", "()", "",
        ):
            result=query('INSERT INTO insert_syntax_rows VALUES%s;' % values)
            check(values,result[1],'42601')
            check('unchanged-'+values,query('SELECT id,v FROM insert_syntax_rows;')[0],[['1','1 day']])
        assert query("INSERT INTO insert_syntax_rows VALUES((3),'1 day' /* ) VALUES ( */),(4,'2 days') ON CONFLICT(id) DO NOTHING;")[1] is None
        check('positive-rows',query('SELECT id,v FROM insert_syntax_rows ORDER BY id;')[0],
              [['1','1 day'],['3','1 day'],['4','2 days']])
        assert not failures, '%s VALUES syntax assertions failed: %r' % (len(failures),failures)
        print('[INSERT VALUES SYNTAX '+('PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__=='__main__': main()
