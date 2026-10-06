#!/usr/bin/env python3
"""Reached PL SQL preparation has a real, isolated EXCLUDED namespace."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('conflict_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client(); reference18='--reference18' in sys.argv[1:]
    reference=reference18 or '--reference' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password); server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('CONFLICT_BIND',sql,result,flush=True); return result
    def check(label,actual,expected):
        if actual!=expected: failures.append((label,actual,expected)); print('CONFLICT_BIND_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        if reference18: runner.verify_reference_version(client,sock)
        elif reference: assert query('SHOW server_version_num;')[0]==[['170002']]
        setup('BEGIN;')
        setup('CREATE TEMP TABLE conflict_rows(id INT PRIMARY KEY,v INT);')
        setup('CREATE TEMP SEQUENCE conflict_sequence;')
        setup('CREATE FUNCTION conflict_bound_call() RETURNS INT AS $$ BEGIN INSERT INTO conflict_rows VALUES(1,10) ON CONFLICT(id) DO UPDATE SET v=excluded.v+1 WHERE conflict_rows.id=excluded.id; RETURN 7; END; $$ LANGUAGE plpgsql;')
        for index in range(2):
            setup('SAVEPOINT conflict_positive;')
            result=query('SELECT conflict_bound_call();')
            check('call-'+str(index),(result[0],result[1],result[5]),([['7']],None,[23]))
            if result[1]: setup('ROLLBACK TO SAVEPOINT conflict_positive;')
            setup('RELEASE SAVEPOINT conflict_positive;')
        check('upsert-values',query('SELECT id,v FROM conflict_rows;')[0],[['1','11']])
        setup("CREATE FUNCTION conflict_bound_bad() RETURNS INT AS $$ BEGIN INSERT INTO conflict_rows VALUES(nextval('conflict_sequence'),12) ON CONFLICT(id) DO UPDATE SET v=excluded.missing; RETURN 7; END; $$ LANGUAGE plpgsql;")
        setup('SAVEPOINT conflict_error;')
        check('missing-transition-column',query('SELECT conflict_bound_bad();')[1],'42703')
        setup('ROLLBACK TO SAVEPOINT conflict_error;')
        setup('RELEASE SAVEPOINT conflict_error;')
        setup('SAVEPOINT conflict_effect;')
        check('not-executed',query("SELECT currval('conflict_sequence');")[1],'55000')
        setup('ROLLBACK TO SAVEPOINT conflict_effect;')
        setup('RELEASE SAVEPOINT conflict_effect;')
        check('unchanged-after-error',query('SELECT id,v FROM conflict_rows;')[0],[['1','11']])
        setup('CREATE FUNCTION conflict_bound_ambiguous() RETURNS INT AS $$ BEGIN INSERT INTO conflict_rows VALUES(1,20) ON CONFLICT(id) DO UPDATE SET v=v+1; RETURN 7; END; $$ LANGUAGE plpgsql;')
        setup('SAVEPOINT conflict_ambiguous;')
        check('unqualified-column-ambiguous',query('SELECT conflict_bound_ambiguous();')[1],'42702')
        setup('ROLLBACK TO SAVEPOINT conflict_ambiguous;')
        setup('RELEASE SAVEPOINT conflict_ambiguous;')
        check('unchanged-after-ambiguity',query('SELECT id,v FROM conflict_rows;')[0],[['1','11']])
        for index,suffix in enumerate(('ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING excluded.v',
                       'ON CONFLICT(id) DO NOTHING RETURNING excluded.v','RETURNING excluded.v')):
            function='conflict_returning_'+str(index)
            setup('CREATE FUNCTION '+function+'() RETURNS INT AS $$ BEGIN INSERT INTO conflict_rows VALUES(2,20) '+suffix+'; RETURN 7; END; $$ LANGUAGE plpgsql;')
            setup('SAVEPOINT conflict_returning;')
            check('no-returning-leak-'+suffix,query('SELECT '+function+'();')[1],'42P01')
            setup('ROLLBACK TO SAVEPOINT conflict_returning;')
            setup('RELEASE SAVEPOINT conflict_returning;')
        setup('ROLLBACK;')
        assert not failures,'%d conflict binding assertions failed: %r'%(len(failures),failures)
        print('[INSERT CONFLICT BINDING '+('PG18.6 REFERENCE' if reference18 else 'PG17.2 DIAGNOSTIC' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)
if __name__=='__main__': main()
