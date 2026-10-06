#!/usr/bin/env python3
"""PG18 controls for source qualification demand and genuine USING types."""
import importlib.util
from pathlib import Path
import socket
import sys

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('multi_boundary_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password);server={'sock':sock}
    else:server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('MULTI_BOUNDARY',sql,result,flush=True);return result
    def setup(sql):
        result=query(sql);assert result[1] is None,(sql,result)
    def check(label,sql,rows=None,state=None,tag=None,types=None):
        result=query(sql)
        valid=result[1]==state and (rows is None or result[0]==rows) and (tag is None or result[4]==tag) and (types is None or result[5]==types)
        if not valid:failures.append((label,rows,state,tag,types,result))
        print('MULTI_BOUNDARY_CONTROL',label,'PASS' if valid else 'FAIL',flush=True)
    try:
        if reference:runner.verify_reference_version(client,sock)
        for sql in ('CREATE TEMP TABLE boundary_target(id INT,v BIGINT)',
                    'CREATE TEMP TABLE boundary_left(k INT)',
                    'CREATE TEMP TABLE boundary_right(k BIGINT)',
                    'CREATE TEMP SEQUENCE boundary_on_false_seq',
                    'CREATE TEMP SEQUENCE boundary_on_null_seq',
                    'INSERT INTO boundary_target VALUES(1,0)',
                    'INSERT INTO boundary_left VALUES(1)',
                    'INSERT INTO boundary_right VALUES(1),(2147483648)'):
            setup(sql)
        check('where-false-does-not-open-volatile-on',
              "WITH p AS(SELECT 1) UPDATE boundary_target t SET v=1 FROM boundary_left l JOIN boundary_right r "
              "ON nextval('boundary_on_false_seq')>0 WHERE false RETURNING t.id",[],tag='UPDATE 0',types=[23])
        check('false-on-never-called',"SELECT currval('boundary_on_false_seq')",state='55000')
        check('where-null-does-not-open-volatile-on',
              "WITH p AS(SELECT 1) DELETE FROM boundary_target t USING boundary_left l JOIN boundary_right r "
              "ON nextval('boundary_on_null_seq')>0 WHERE NULL RETURNING t.id",[],tag='DELETE 0',types=[23])
        check('null-on-never-called',"SELECT currval('boundary_on_null_seq')",state='55000')
        check('using-common-type-wide-unmatched-value',
              'WITH p AS(SELECT 1) UPDATE boundary_target t SET v=k FROM boundary_left l FULL JOIN boundary_right r '
              'USING(k) WHERE k=2147483648 RETURNING k,k+0,t.v',
              [['2147483648','2147483648','2147483648']],tag='UPDATE 1',types=[20,20,20])
        assert not failures,failures
        print('[WITH MULTISOURCE BOUNDARY] passed',flush=True)
    finally:
        if reference:sock.close()
        else:runner.stop_ours(server)

if __name__=='__main__':main()
