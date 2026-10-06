#!/usr/bin/env python3
"""WITH-final-DML retains target-input errors before CTE effects (independent gate)."""
import importlib.util
from pathlib import Path
import socket
import sys


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('interval_with_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference='--reference' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password)
        server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('INTERVAL_WITH',sql,result,flush=True)
        return result
    def check(label,actual,expected):
        if actual!=expected:
            failures.append((label,actual,expected)); print('INTERVAL_WITH_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        if reference: assert query('SHOW server_version_num;')[0]==[['170002']]
        setup('CREATE TEMP TABLE interval_state_rows(id INT PRIMARY KEY,v INTERVAL);')
        setup("INSERT INTO interval_state_rows VALUES(1,'1 day'),(2,NULL);")
        original="WITH input_constants AS (SELECT 3 AS id) INSERT INTO interval_state_rows VALUES(3,'2147483648 months');"
        # This is the unchanged original full-matrix WITH failure, not a skip
        # switch in the independent direct INSERT/UPDATE classification gate.
        check('original-with-input-error',query(original)[1],'22015')
        expected=[['1','1 day'],['2',None]]
        check('original-with-no-writes',query('SELECT id,v FROM interval_state_rows ORDER BY id;')[0],expected)
        setup('CREATE TEMP TABLE interval_with_effects(id INT);')
        setup('CREATE TEMP SEQUENCE interval_with_seq;')
        sql="WITH effects AS (INSERT INTO interval_with_effects VALUES(nextval('interval_with_seq')) RETURNING id) INSERT INTO interval_state_rows VALUES(3,'2147483648 months');"
        check('writing-cte-input-error',query(sql)[1],'22015')
        check('writing-cte-no-writes',query('SELECT id FROM interval_with_effects;')[0],[])
        check('writing-cte-no-sequence-effect',query("SELECT currval('interval_with_seq');")[1],'55000')
        setup('BEGIN;')
        check('transaction-input-error',query(original)[1],'22015')
        check('transaction-aborted',query('SELECT 1;')[1],'25P02')
        setup('ROLLBACK;')
        check('transaction-restored',query('SELECT id,v FROM interval_state_rows ORDER BY id;')[0],expected)
        assert not failures,'%s WITH input assertions failed: %r' % (len(failures),failures)
        print('[INTERVAL WITH DML INPUT '+('PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)


if __name__=='__main__': main()
