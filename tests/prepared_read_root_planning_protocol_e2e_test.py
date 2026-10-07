#!/usr/bin/env python3
"""The actual read/quantified-EXPLAIN root plans constants before effects."""
import importlib.util,socket,sys,uuid
from pathlib import Path

def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('prepared_root_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference='--reference18' in sys.argv[1:]
    if reference:
        h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=120);c.startup_reference(sock,u,d,pw);server={'sock':sock}
    else:server=r.start_ours(c)
    failures=[]
    def query(sql):
        if reference:c.simple_query(sock,'SAVEPOINT prepared_root_case;')
        result=r.decode_wire_result(c.simple_query(server['sock'],sql),include_types=True)
        if reference:
            if result[1] is not None:c.simple_query(sock,'ROLLBACK TO SAVEPOINT prepared_root_case;')
            c.simple_query(sock,'RELEASE SAVEPOINT prepared_root_case;')
        print('PREPARED_READ_ROOT',sql,result,flush=True);return result
    def check(label,actual,expected):
        if actual!=expected:failures.append((label,actual,expected));print('PREPARED_READ_ROOT_FAILURE',failures[-1],flush=True)
    try:
        if reference:
            r.verify_reference_version(c,sock);schema='prepared_root_'+uuid.uuid4().hex
            setup=r.decode_wire_result(c.simple_query(sock,'BEGIN; CREATE SCHEMA '+schema+'; SET search_path='+schema+',public;'),include_types=True);assert setup[1] is None,setup
        for sql in ('CREATE TEMP TABLE prepared_root_rows(id INT);','INSERT INTO prepared_root_rows VALUES(1);',
            'CREATE TEMP SEQUENCE prepared_root_calls;',
            "CREATE FUNCTION prepared_root_writer() RETURNS INT AS $$ BEGIN PERFORM nextval('prepared_root_calls'); RETURN 1; END; $$ LANGUAGE plpgsql;"):
            check(sql,query(sql)[1],None)
        for sql in (
            'SELECT (SELECT 1/0) WHERE false;',
            'SELECT (SELECT 1/0) LIMIT 0;',
            'VALUES(prepared_root_writer(),(SELECT 1/0));',
            'SELECT CASE WHEN true THEN 1/0 ELSE 1 END FROM prepared_root_rows WHERE false;',
            'SELECT CASE WHEN true THEN 1/0 ELSE 1 END FROM prepared_root_rows LIMIT 0;',
            "WITH w AS (INSERT INTO prepared_root_rows VALUES(prepared_root_writer()) RETURNING id) SELECT (SELECT 1/0) FROM w;",
            "EXPLAIN SELECT prepared_root_writer(),1/0 FROM prepared_root_rows WHERE 1=ANY(SELECT 1);",
            "EXPLAIN ANALYZE SELECT prepared_root_writer(),1/0 FROM prepared_root_rows WHERE 1=ANY(SELECT 1);",
        ):
            result=query(sql);check(sql+' state',result[1],'22012');check(sql+' no completion',result[4],None)
            check(sql+' has no effects',query("SELECT currval('prepared_root_calls');")[1],'55000')
        # Child construction must not reacquire a discarded CASE target.
        for sql in (
            'SELECT CASE WHEN false THEN (SELECT 1/0) ELSE 3 END;',
            'WITH r AS (SELECT 1/0 AS value) SELECT CASE WHEN false THEN (SELECT value FROM r) ELSE 3 END;',
        ):
            result=query(sql);check(sql+' state',result[1],None);check(sql+' value',result[0],[['3']]);check(sql+' type',result[5],[23])
        # Successful writing CTEs still execute even when no row is demanded.
        sql="WITH w AS (INSERT INTO prepared_root_rows VALUES(prepared_root_writer()) RETURNING id) SELECT CASE WHEN false THEN (SELECT 1/0) ELSE id END FROM w LIMIT 0;"
        result=query(sql);check('successful writer zero demand state',result[1],None);check('successful writer zero demand rows',result[0],[])
        result=query("SELECT currval('prepared_root_calls');");check('successful writer runs once',result[0],[['1']]);check('successful writer state',result[1],None)
        result=query('SELECT count(*) FROM prepared_root_rows;');check('successful writer persisted',result[0],[['2']])
        assert not failures,'%d read-root planning failures: %r'%(len(failures),failures)
        print('[PREPARED READ ROOT '+('PG18.6 REFERENCE' if reference else 'PROTOCOL E2E')+'] passed')
    finally:
        if reference:c.simple_query(sock,'ROLLBACK;');sock.close()
        else:r.stop_ours(server)
if __name__=='__main__':main()
