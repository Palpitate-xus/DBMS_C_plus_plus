#!/usr/bin/env python3
"""Legacy scalar projection retains exact 21000 and a usable connection."""
import argparse
import importlib.util
from pathlib import Path
import socket


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--reference18',action='store_true');args=parser.parse_args()
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('scalar_projection_state_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner);client=runner.load_protocol_client()
    server=None
    if args.reference18:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=15)
        client.startup_reference(sock,user,database,password=password)
    else:
        server=runner.start_ours(client);sock=server['sock']
    def query(sql):return runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
    def ok(sql):
        result=query(sql);assert result[1] is None,(sql,result);return result
    try:
        if args.reference18:assert ok('SHOW server_version_num')[0]==[['180006']]
        ok('BEGIN;')
        for sql in (
            'CREATE TEMP TABLE sub_outer(id INT);','CREATE TEMP TABLE sub_inner(id INT,enabled INT);',
            'INSERT INTO sub_outer VALUES(1),(2),(3),(4);','INSERT INTO sub_inner VALUES(2,1),(3,1);',
            'INSERT INTO sub_inner(enabled) VALUES(1);','CREATE TEMP TABLE sub_text(id INT,value TEXT);',
            "INSERT INTO sub_text VALUES(1,NULL),(2,''),(3,'NULL'),(4,NULL);",
        ):ok(sql)
        # The first query is the exact original full-protocol SQL at line2504;
        # both NULL rows and non-NULL rows count toward scalar cardinality.
        for sql in (
            'SELECT id, (SELECT id FROM sub_inner WHERE enabled = 1) FROM sub_outer',
            'SELECT (SELECT id FROM sub_inner WHERE enabled = 1), id FROM sub_outer',
            'SELECT id, (SELECT value FROM sub_text WHERE value IS NULL) FROM sub_outer',
        ):
            ok('SAVEPOINT scalar_control');result=query(sql)
            assert result[1]=='21000' and result[0]==[] and result[4] is None,(sql,result)
            ok('ROLLBACK TO scalar_control');ok('RELEASE scalar_control')
            assert ok('SELECT 1')[0]==[['1']]
            print('SCALAR CARDINALITY 21000',sql,flush=True)
        for child,value in (
            ('SELECT id FROM sub_inner WHERE id=2','2'),
            ('SELECT id FROM sub_inner WHERE id=99',None),
            ('SELECT value FROM sub_text WHERE id=1',None),
            ('SELECT value FROM sub_text WHERE id=2',''),
            ('SELECT value FROM sub_text WHERE id=3','NULL'),
        ):
            result=ok('SELECT id, ('+child+') FROM sub_outer')
            assert result[0]==[[str(i),value] for i in range(1,5)],(child,result)
            assert result[5]==[23,23 if 'FROM sub_inner' in child else 25],(child,result)
        ok('ROLLBACK;')
        # Autocommit rejection must also recover, without lowering a failed
        # transaction to success or disguising SQL NULL as empty text.
        ok('CREATE TEMP TABLE scalar_autocommit_outer(id INT)');ok('INSERT INTO scalar_autocommit_outer VALUES(1)')
        ok('CREATE TEMP TABLE scalar_autocommit_inner(id INT)');ok('INSERT INTO scalar_autocommit_inner VALUES(1),(2)')
        result=query('SELECT id,(SELECT id FROM scalar_autocommit_inner) FROM scalar_autocommit_outer')
        assert result[1]=='21000' and result[0]==[] and result[4] is None,result
        assert ok('SELECT 1')[0]==[['1']]
        ok('DROP TABLE scalar_autocommit_inner,scalar_autocommit_outer')
        print('[SCALAR PROJECTION CARDINALITY SQLSTATE] passed',flush=True)
    finally:
        if server:runner.stop_ours(server)
        else:sock.close()


if __name__=='__main__':main()
