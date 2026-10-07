#!/usr/bin/env python3
"""Qualified projection identity is bound before any target/demand effects."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('qualified_frontend_runner', root/'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
    c = r.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        h,p,u,d,pw = r._reference_connection_settings()
        sock = socket.create_connection((h,p), timeout=r.wire_timeout())
        c.startup_reference(sock,u,d,password=pw); r.verify_reference_version(c,sock)
    else:
        server = r.start_ours(c); sock = server['sock']
    failures = []

    def query(sql, state=None, rows=None, types=None):
        result = r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
        print('QUALIFIED_ROUTINE',sql,result,flush=True)
        for role,actual,expected in [('state',result[1],state),('rows',result[0],rows),('types',result[5],types)]:
            if expected is not None or role == 'state':
                if actual != expected: failures.append((sql,role,actual,expected))
        if state and (result[0] or result[4] is not None): failures.append((sql,'partial success',result))

    def reject(sql,state):
        query('SAVEPOINT qualified_error')
        query(sql,state=state)
        query('ROLLBACK TO SAVEPOINT qualified_error'); query('RELEASE SAVEPOINT qualified_error')

    try:
        query('BEGIN')
        query('CREATE TEMP SEQUENCE qualified_effects')
        for suffix in ['', ' WHERE false', ' LIMIT 0']:
            reject('SELECT public.unnest(NULL)'+suffix,'42883')
            reject("SELECT nextval('qualified_effects'),public.unnest(ARRAY[1,2])"+suffix,'42883')
            reject("SELECT pg_catalog.upper('x'),public.missing_qualified(nextval('qualified_effects'))"+suffix,'42883')
        query("SELECT currval('qualified_effects')",state='55000')
        query('ROLLBACK'); query('BEGIN')
        query('CREATE TEMP SEQUENCE qualified_effects')
        query("SELECT pg_catalog.upper('x'),7",rows=[['X','7']],types=[25,23])
        query("SELECT \"pg_catalog\".\"upper\"('x')",rows=[['X']],types=[25])
        reject("SELECT \"PG_catalog\".upper('x')",'3F000')
        reject("SELECT pg_catalog.\"Upper\"('x')",'42883')
        reject("SELECT public.upper('x')",'42883')
        query('SELECT pg_catalog.unnest(ARRAY[1,2,NULL])',rows=[['1'],['2'],[None]],types=[23])
        query('CREATE FUNCTION unnest(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN p+10; END$$')
        query('SELECT public.unnest(7)',rows=[['17']],types=[23])
        query('SELECT unnest(7)',rows=[['17']],types=[23])
        query('SELECT pg_catalog.unnest(ARRAY[3,4])',rows=[['3'],['4']],types=[23])
        query('CREATE FUNCTION "Unnest"(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN p+20; END$$')
        query('SELECT public."Unnest"(7)',rows=[['27']],types=[23])
        query("SELECT nextval('qualified_effects')",rows=[['1']],types=[20])
        query('ROLLBACK')
        print('QUALIFIED_ROUTINE_FAILURES',failures,flush=True)
        assert not failures, failures
    finally:
        try:c.simple_query(sock,'ROLLBACK')
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__ == '__main__':main()
