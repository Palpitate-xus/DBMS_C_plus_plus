#!/usr/bin/env python3
"""PG18 RETURNING transition scopes prepare before reached PL effects."""
import importlib.util
from pathlib import Path
import socket
import sys
def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('returning_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client=runner.load_protocol_client(); reference='--reference18' in sys.argv[1:]
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=120)
        client.startup_reference(sock,user,database,password); runner.verify_reference_version(client,sock); server={'sock':sock}
    else: server=runner.start_ours(client)
    failures=[]
    def query(sql):
        result=runner.decode_wire_result(client.simple_query(server['sock'],sql),include_types=True)
        print('RETURNING_BIND',sql,result,flush=True); return result
    def check(label,actual,expected):
        if actual!=expected: failures.append((label,actual,expected)); print('RETURNING_BIND_FAILURE',failures[-1],flush=True)
    def setup(sql):
        result=query(sql); assert result[1] is None,(sql,result)
    try:
        setup('BEGIN;')
        setup('CREATE TEMP TABLE returning_rows(id INT PRIMARY KEY,v INT);')
        setup('CREATE TEMP SEQUENCE returning_sequence;')
        setup('CREATE FUNCTION returning_bound_call() RETURNS INT AS $$ BEGIN INSERT INTO returning_rows VALUES(1,10) RETURNING old.id,new.id,id; RETURN 7; END; $$ LANGUAGE plpgsql;')
        # PL requires a destination for a valid RETURNING query; the static
        # namespace must resolve first. PERFORM is not a DML RETURNING sink.
        setup('SAVEPOINT returning_positive;')
        check('resolved-then-destination',query('SELECT returning_bound_call();')[1],'42601')
        setup('ROLLBACK TO SAVEPOINT returning_positive;')
        setup('RELEASE SAVEPOINT returning_positive;')
        for index,suffix,state in (
            (0,'RETURNING old.missing','42703'),
            (1,'RETURNING WITH(OLD AS before_row) old.id','42P01'),
            (2,'RETURNING WITH(OLD AS same_row,NEW AS same_row) same_row.id','42712'),
        ):
            function='returning_bound_bad_'+str(index)
            setup("CREATE FUNCTION "+function+"() RETURNS INT AS $$ BEGIN INSERT INTO returning_rows VALUES(nextval('returning_sequence'),10) "+suffix+'; RETURN 7; END; $$ LANGUAGE plpgsql;')
            setup('SAVEPOINT returning_error;')
            check('binding-'+str(index),query('SELECT '+function+'();')[1],state)
            setup('ROLLBACK TO SAVEPOINT returning_error;')
            setup('RELEASE SAVEPOINT returning_error;')
        setup('SAVEPOINT returning_effect;')
        check('not-executed',query("SELECT currval('returning_sequence');")[1],'55000')
        setup('ROLLBACK TO SAVEPOINT returning_effect;')
        setup('RELEASE SAVEPOINT returning_effect;')
        check('no-writes',query('SELECT id FROM returning_rows;')[0],[])
        result=query("INSERT INTO returning_rows VALUES(1,10) RETURNING old.id,new.id,id;")
        check('insert-old-typed-null',(result[0],result[1],result[5]),([[None,'1','1']],None,[23,23,23]))
        result=query('UPDATE returning_rows SET v=v+1 RETURNING WITH(OLD AS before_row,NEW AS after_row) before_row.v,after_row.v,v;')
        check('actual-old-new-values',(result[0],result[1],result[5]),([['10','11','11']],None,[23,23,23]))
        for label,seed,sql,expected in (
            ('actual-relation-masks-old',11,'UPDATE returning_rows AS old SET v=v+1 RETURNING old.v,new.v,v;',[['12','12','12']]),
            ('explicit-old-masks-new',12,'UPDATE returning_rows SET v=v+1 RETURNING WITH(OLD AS new) new.v,v;',[['12','13']]),
            ('explicit-new-masks-old',13,'UPDATE returning_rows SET v=v+1 RETURNING WITH(NEW AS old) old.v,v;',[['14','14']]),
            ('quoted-aliases-distinct',14,'UPDATE returning_rows SET v=v+1 RETURNING WITH(OLD AS o,NEW AS "O") o.v,"O".v,v;',[['14','15','15']]),
            ('quoted-compound-operands',14,'UPDATE returning_rows SET v=v+1 RETURNING WITH(OLD AS o,NEW AS "O") o.v+"O".v,CASE WHEN o.v IS NULL THEN 0 ELSE o.v END;',[['29','14']]),
        ):
            setup('UPDATE returning_rows SET v='+str(seed)+';')
            setup('SAVEPOINT returning_values;')
            result=query(sql)
            check(label,(result[0],result[1],result[5]),(expected,None,[23]*len(expected[0])))
            # Keep a failed baseline case from masking the following cases
            # with 25P02, without changing any intended row-value assertion.
            if result[1] is not None: setup('ROLLBACK TO SAVEPOINT returning_values;')
            setup('RELEASE SAVEPOINT returning_values;')
        setup('UPDATE returning_rows SET v=15;')
        result=query('DELETE FROM returning_rows RETURNING old.v,new.v,v;')
        check('actual-old-new-null',(result[0],result[1],result[5]),([['15',None,'15']],None,[23,23,23]))
        setup('ROLLBACK;')
        assert not failures,'%d RETURNING assertions failed: %r'%(len(failures),failures)
        print('[RETURNING TRANSITION '+('PG18.6 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference: sock.close()
        else: runner.stop_ours(server)
if __name__=='__main__': main()
