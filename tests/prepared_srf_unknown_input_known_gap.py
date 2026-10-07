#!/usr/bin/env python3
"""Whole UNKNOWN input matrix; qualified legacy dispatch remains a real gap."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('srf_unknown_runner', root/'tests/compat/pg_diff_runner.py')
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
        result = r.decode_wire_result(c.simple_query(sock,sql), include_types=True)
        print('SRF_UNKNOWN_INPUT',sql,result,flush=True)
        for role,actual,expected in [('state',result[1],state),('rows',result[0],rows),('types',result[5],types)]:
            if expected is not None or role == 'state':
                if actual != expected: failures.append((sql,role,actual,expected))
        if state and (result[0] or result[4] is not None): failures.append((sql,'partial success',result))

    try:
        query('CREATE TEMP SEQUENCE unknown_input_effects;')
        for expression in ['NULL', "'{1,2}'", "'NULL'"]:
            for suffix in ['', ' WHERE false', ' LIMIT 0']:
                query('SELECT unnest('+expression+')'+suffix+';', state='42725')
        query('SELECT pg_catalog.unnest(NULL);', state='42725')
        query('SELECT "unnest"(NULL);', state='42725')
        query('SELECT "Unnest"(NULL);', state='42883')
        query('SELECT public.unnest(NULL);', state='42883')
        query('SELECT unnest(NULL::text);', state='42883')
        query('SELECT unnest(COALESCE(NULL,NULL));', state='42883')
        query('SELECT unnest(CASE WHEN true THEN NULL ELSE NULL END);', state='42883')
        query('SELECT unnest(NULLIF(NULL,NULL));', state='42883')
        query('SELECT unnest(missing_unknown_input_function());', state='42883')
        query("SELECT unnest(missing_unknown_input_function(nextval('unknown_input_effects')));", state='42883')
        query("SELECT currval('unknown_input_effects');", state='55000')
        query('SELECT unnest(NULL::int[]);', rows=[], types=[23])
        query('SELECT unnest(NULL::text[]);', rows=[], types=[25])
        query('SELECT unnest(ARRAY[1,2,NULL]);', rows=[['1'],['2'],[None]], types=[23])
        query('SELECT unnest(ARRAY[NULL]);', rows=[[None]], types=[25])
        query('SELECT unnest((SELECT ARRAY[1,2]));', rows=[['1'],['2']], types=[23])
        query('SELECT NULL;', rows=[[None]], types=[25])
        query("SELECT '{1,2}';", rows=[['{1,2}']], types=[25])
        query("SELECT nextval('unknown_input_effects');", rows=[['1']], types=[20])
        print('SRF_UNKNOWN_FAILURES',failures,flush=True)
        assert not failures, failures
    finally:
        if server: r.stop_ours(server)
        else: sock.close()


if __name__ == '__main__': main()
