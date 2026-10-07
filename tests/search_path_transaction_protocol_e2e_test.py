#!/usr/bin/env python3
"""SET/SET LOCAL search_path transaction and savepoint lifecycle."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('path_transaction_runner',root/'tests/compat/pg_diff_runner.py')
    r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
    reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=r.wire_timeout())
        c.startup_reference(sock,u,d,password=pw);r.verify_reference_version(c,sock)
    else:server=r.start_ours(c);sock=server['sock']
    failures=[]
    def query(sql,rows=None,state=None):
        result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
        print('SEARCH_PATH_TRANSACTION',sql,result,flush=True)
        if result[1]!=state or (rows is not None and result[0]!=rows):failures.append((sql,result,rows,state))
    def show(value):query('SHOW search_path;',[[value]])
    try:
        default_result=r.decode_wire_result(c.simple_query(sock,'SHOW search_path;'),include_types=True)
        assert default_result[1] is None and len(default_result[0])==1 and default_result[5]==[25],default_result
        default_path=default_result[0][0][0]
        query('SET search_path=public;');show('public')
        query('SET LOCAL search_path=pg_catalog;');show('public')
        query('BEGIN;');query('SET LOCAL search_path=public,pg_catalog;');show('public, pg_catalog')
        query('SAVEPOINT outer_path;');query('SET LOCAL search_path=pg_catalog,public;');show('pg_catalog, public')
        query('SAVEPOINT inner_path;');query('SET search_path="Quoted.Path",pg_catalog;');show('"Quoted.Path", pg_catalog')
        query('ROLLBACK TO outer_path;');show('public, pg_catalog')
        query('SET search_path=pg_catalog,public;');query('SET LOCAL search_path=public;');show('public')
        query('COMMIT;');show('pg_catalog, public')
        query('BEGIN;');query('SET LOCAL search_path="Quoted.Path",public;');show('"Quoted.Path", public')
        query('SAVEPOINT same_path;');query('SET LOCAL search_path=pg_catalog;')
        query('SAVEPOINT same_path;');query('SET LOCAL search_path=public;')
        query('ROLLBACK TO same_path;');show('pg_catalog')
        query('RELEASE same_path;');query('ROLLBACK TO same_path;');show('"Quoted.Path", public')
        query('ROLLBACK;');show('pg_catalog, public')
        query('BEGIN;');query('SET search_path=public;');query('SET LOCAL search_path=pg_catalog;')
        query('SAVEPOINT released_path;');query('SET search_path="Quoted.Path";');query('RELEASE released_path;')
        query('COMMIT;');show('"Quoted.Path"')
        query('BEGIN;');query('SET LOCAL search_path=pg_catalog;');query('RESET search_path;');show(default_path)
        query('ROLLBACK;');show('"Quoted.Path"')
        query('BEGIN;');query('SET LOCAL search_path=pg_catalog;');query('RESET search_path;');query('COMMIT;');show(default_path)
        query('SET search_path="Quoted.Path";')
        query('BEGIN;');query('SET LOCAL search_path=public;');query('SELECT 1/0;',state='22012')
        query('SHOW search_path;',state='25P02');query('ROLLBACK;');show('"Quoted.Path"')
        query('BEGIN;');query('SET LOCAL search_path=public;');query('SAVEPOINT recover_path;')
        query('SET LOCAL search_path=pg_catalog;');query('SELECT 1/0;',state='22012')
        query('ROLLBACK TO recover_path;');show('public');query('COMMIT;');show('"Quoted.Path"')
        query('BEGIN;');query('SET LOCAL search_path=public;');query('COMMIT AND CHAIN;');show('"Quoted.Path"')
        query('SET LOCAL search_path=pg_catalog;');query('ROLLBACK AND CHAIN;');show('"Quoted.Path"');query('ROLLBACK;')
        query('SET search_path=public;')
        if failures:
            for failure in failures:print('SEARCH_PATH_TRANSACTION_FAILURE',failure,flush=True)
            raise AssertionError(f'{len(failures)} lifecycle assertions failed')
        print('[SEARCH PATH TRANSACTION '+('PG18.6' if reference else 'PROTOCOL')+'] passed',flush=True)
    finally:
        try:c.simple_query(sock,'ROLLBACK;')
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
