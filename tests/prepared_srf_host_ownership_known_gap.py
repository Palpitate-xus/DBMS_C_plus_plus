#!/usr/bin/env python3
"""Whole SRF host/limit/unknown-input diagnostic; retained gaps are not skips."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('srf_owner_runner', root/'tests/compat/pg_diff_runner.py')
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

    def query(sql, rows=None, types=None, labels=None, state=None):
        result = r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
        print('SRF_HOST_OWNER',sql,result,flush=True)
        for role,actual,expected in [('state',result[1],state),('rows',result[0],rows),('types',result[5],types),('labels',result[3],labels)]:
            if expected is not None or role == 'state':
                if actual != expected: failures.append((sql,role,actual,expected))
        if state and (result[0] or result[4] is not None): failures.append((sql,'partial success',result))

    channels = "SELECT pg_listening_channels();"
    try:
        query(channels,[],[25],['pg_listening_channels'])
        query('LISTEN host_alpha;'); query('LISTEN host_zeta;')
        query(channels,[['host_alpha'],['host_zeta']],[25],['pg_listening_channels'])
        query('SELECT pg_listening_channels(),7 AS marker;', [['host_alpha','7'],['host_zeta','7']],[25,23],['pg_listening_channels','marker'])
        query('SELECT pg_listening_channels() LIMIT 0;',[],[25],['pg_listening_channels'])
        query('BEGIN;'); query('UNLISTEN host_alpha;'); query('LISTEN host_beta;')
        query(channels,[['host_alpha'],['host_zeta']],[25],['pg_listening_channels'])
        query('ROLLBACK;')
        query(channels,[['host_alpha'],['host_zeta']],[25],['pg_listening_channels'])
        query('BEGIN;'); query('UNLISTEN host_alpha;'); query('COMMIT;')
        query(channels,[['host_zeta']],[25],['pg_listening_channels'])
        query('UNLISTEN *;'); query(channels,[],[25],['pg_listening_channels'])
        query('SELECT unnest(ARRAY[1,2,NULL]);',[['1'],['2'],[None]],[23],['unnest'])
        query('SELECT pg_catalog.unnest(ARRAY[1,2]);',[['1'],['2']],[23],['unnest'])
        query('SELECT unnest(missing_host_function());',state='42883')
        query('SELECT unnest(NULL);',state='42725')
        query('CREATE TEMP SEQUENCE host_effects;')
        query("SELECT missing_host_function(nextval('host_effects'));",state='42883')
        query("SELECT currval('host_effects');",state='55000')
        query("SELECT upper('x');",[['X']],[25],['upper'])
        query('SELECT 1;',[['1']],[23])
        assert not failures, failures
        print('[PREPARED SRF HOST OWNERSHIP '+('PG18.6' if reference else 'PROTOCOL')+'] passed',flush=True)
    finally:
        try:c.simple_query(sock,'UNLISTEN *;')
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__ == '__main__':main()
