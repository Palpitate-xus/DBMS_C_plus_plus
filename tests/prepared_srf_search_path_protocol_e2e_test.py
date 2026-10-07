#!/usr/bin/env python3
"""Namespace order, scalar shadows and actual backend SRF ownership."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('srf_search_path_runner', root/'tests/compat/pg_diff_runner.py')
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
    failures=[]
    in_transaction=False

    def query(sql, rows=None, types=None, labels=None, state=None):
        if state and in_transaction:
            c.simple_query(sock,'SAVEPOINT expected_error')
        result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
        print('SRF_SEARCH_PATH',sql,result,flush=True)
        for role,actual,expected in [('state',result[1],state),('rows',result[0],rows),('types',result[5],types),('labels',result[3],labels)]:
            if expected is not None or role=='state':
                if actual!=expected:failures.append((sql,role,actual,expected))
        if state and (result[0] or result[4] is not None):failures.append((sql,'partial success',result))
        if state and in_transaction:
            c.simple_query(sock,'ROLLBACK TO expected_error')
            c.simple_query(sock,'RELEASE expected_error')

    def described(sql, rows, types, labels):
        statement=b'path_description';portal=b'path_portal'
        parse=statement+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        bind=portal+b'\0'+statement+b'\0'+struct.pack('!HHH',0,0,0)
        sock.sendall(c.typed(b'P',parse)+c.typed(b'D',b'S'+statement+b'\0')+
            c.typed(b'B',bind)+c.typed(b'D',b'P'+portal+b'\0')+
            c.typed(b'E',portal+b'\0'+struct.pack('!I',0))+c.typed(b'S'))
        messages=c.read_until_ready(sock)
        descriptions=[c.row_description_fields([m]) for m in messages if m[0]==b'T']
        print('SRF_PATH_DESCRIBE',sql,descriptions,c.data_row_values(messages),flush=True)
        if len(descriptions)!=2:failures.append((sql,'two descriptions',descriptions))
        for fields in descriptions:
            if [field[3] for field in fields]!=types or [field[0].decode() for field in fields]!=labels:
                failures.append((sql,'descriptor',fields,types,labels))
        wanted=[[None if value is None else value.encode() for value in row] for row in rows]
        if c.data_row_values(messages)!=wanted or any(kind==b'E' for kind,_ in messages):
            failures.append((sql,'extended execution',messages))
        sock.sendall(c.typed(b'C',b'P'+portal+b'\0')+c.typed(b'C',b'S'+statement+b'\0')+c.typed(b'S'))
        c.read_until_ready(sock)

    try:
        query('LISTEN path_alpha;');query('LISTEN path_zeta;')
        query('BEGIN;');in_transaction=True
        query('CREATE TEMP SEQUENCE path_effects;')
        query("CREATE FUNCTION pg_listening_channels() RETURNS BIGINT LANGUAGE plpgsql AS $$BEGIN RETURN nextval('path_effects'); END;$$;")
        query('CREATE FUNCTION unnest(p INT[]) RETURNS INT LANGUAGE SQL AS $$SELECT -9$$;')
        query('SET LOCAL search_path=public;')
        query('SELECT pg_listening_channels();',[['path_alpha'],['path_zeta']],[25],['pg_listening_channels'])
        described('SELECT pg_listening_channels() LIMIT 0;',[],[25],['pg_listening_channels'])
        query("SELECT currval('path_effects');",state='55000')
        query('SET LOCAL search_path=public,pg_catalog;')
        for suffix in [' WHERE false',' WHERE NULL::BOOL',' LIMIT 0']:
            query('SELECT pg_listening_channels(),pg_catalog.upper(\'x\') AS marker'+suffix+';',[],[20,25],['pg_listening_channels','marker'])
            described('SELECT pg_listening_channels(),pg_catalog.upper(\'x\') AS marker'+suffix+';',[],[20,25],['pg_listening_channels','marker'])
        query("SELECT currval('path_effects');",state='55000')
        query('SELECT pg_listening_channels(),pg_catalog.upper(\'x\') AS marker;',[['1','X']],[20,25],['pg_listening_channels','marker'])
        query('SELECT pg_catalog.pg_listening_channels();',[['path_alpha'],['path_zeta']],[25],['pg_listening_channels'])
        described('SELECT "pg_catalog"."pg_listening_channels"(),7;', [['path_alpha','7'],['path_zeta','7']],[25,23],['pg_listening_channels','?column?'])
        for suffix in ['',' WHERE false',' LIMIT 0']:
            query("SELECT nextval('path_effects'),pg_listening_channels(),missing_path_function()"+suffix+';',state='42883')
        query("SELECT currval('path_effects');",[['1']],[20],['currval'])
        query('SELECT CASE WHEN false THEN pg_listening_channels() ELSE 7::BIGINT END;',[['7']],[20],['case'])
        query("SELECT currval('path_effects');",[['1']],[20],['currval'])
        query('SET LOCAL search_path=pg_catalog,public;')
        query('SELECT pg_listening_channels(),pg_catalog.upper(\'x\') AS marker;',[['path_alpha','X'],['path_zeta','X']],[25,25],['pg_listening_channels','marker'])
        query('SELECT public.pg_listening_channels();',[['2']],[20],['pg_listening_channels'])
        query('SELECT unnest(ARRAY[1,2]);',[['-9']],[23],['unnest'])
        query('SELECT pg_catalog.unnest(ARRAY[1,2]);',[['1'],['2']],[23],['unnest'])
        query('SELECT unnest(NULL);',state='42725')
        query('SELECT public.unnest(NULL);',[['-9']],[23],['unnest'])
        query('SELECT unnest(ARRAY[\'a\',\'b\']);',[['a'],['b']],[25],['unnest'])
        query('CREATE SCHEMA path_first;');query('CREATE SCHEMA "Path.Second";')
        query('CREATE FUNCTION path_first.pg_listening_channels() RETURNS INT LANGUAGE SQL AS $$SELECT 11$$;')
        query('CREATE FUNCTION "Path.Second".pg_listening_channels() RETURNS INT LANGUAGE SQL AS $$SELECT 22$$;')
        query('CREATE FUNCTION "Path.Second".unnest(p TEXT[]) RETURNS TEXT LANGUAGE SQL AS $$SELECT \'second scalar\'::TEXT$$;')
        query('SET LOCAL search_path=path_missing,"Path.Second",path_first,public,pg_catalog;')
        query('SELECT pg_listening_channels();',[['22']],[23],['pg_listening_channels'])
        described('SELECT pg_listening_channels() WHERE false;',[],[23],['pg_listening_channels'])
        query('SELECT path_first.pg_listening_channels();',[['11']],[23],['pg_listening_channels'])
        query('SELECT "Path.Second".pg_listening_channels();',[['22']],[23],['pg_listening_channels'])
        query('SELECT unnest(ARRAY[\'a\',\'b\']);',[['second scalar']],[25],['unnest'])
        query('SELECT unnest(ARRAY[1,2]);',[['-9']],[23],['unnest'])
        query('SET LOCAL search_path=path_first,"Path.Second",public,pg_catalog;')
        query('SELECT pg_listening_channels();',[['11']],[23],['pg_listening_channels'])
        query('SET LOCAL search_path=path_first,"Path.Second",public;')
        query('SELECT pg_listening_channels();',[['path_alpha'],['path_zeta']],[25],['pg_listening_channels'])
        query("SET LOCAL search_path='';")
        query('SELECT pg_listening_channels();',[['path_alpha'],['path_zeta']],[25],['pg_listening_channels'])
        query('SELECT path_missing.pg_listening_channels() LIMIT 0;',state='3F000')
        query('SELECT path_first.missing_path_function() WHERE false;',state='42883')
        query('SELECT "PG_catalog".pg_listening_channels();',state='3F000')
        query('SET LOCAL search_path=public,pg_catalog;')
        query('SELECT pg_listening_channels(1);',state='42883')
        query('SELECT pg_listening_channels(missing_path_function());',state='42883')
        query('ROLLBACK;');in_transaction=False
        query('BEGIN;');in_transaction=True
        query('CREATE FUNCTION public.unnest(p TEXT) RETURNS TEXT LANGUAGE SQL AS $$SELECT p$$;')
        query('SET LOCAL search_path=pg_catalog,public;')
        query("SELECT unnest('scalar');",[['scalar']],[25],['unnest'])
        query('SELECT unnest(NULL);',[[None]],[25],['unnest'])
        query("SELECT unnest('scalar') WHERE false;",[],[25],['unnest'])
        described("SELECT unnest('scalar') LIMIT 0;",[],[25],['unnest'])
        query('SELECT unnest(ARRAY[1,2]);',[['1'],['2']],[23],['unnest'])
        query('SELECT pg_catalog.unnest(ARRAY[1,2]);',[['1'],['2']],[23],['unnest'])
        query('ROLLBACK;');in_transaction=False
        # The backend provider still uses committed subscriptions after all
        # transient scalar declarations and search_path changes rolled back.
        query('SELECT pg_listening_channels();',[['path_alpha'],['path_zeta']],[25],['pg_listening_channels'])
        query('BEGIN;');in_transaction=True
        query('UNLISTEN path_alpha;');query('LISTEN path_beta;')
        query('SELECT pg_catalog.pg_listening_channels();',[['path_alpha'],['path_zeta']],[25],['pg_listening_channels'])
        query('ROLLBACK;');in_transaction=False
        query('SELECT pg_listening_channels();',[['path_alpha'],['path_zeta']],[25],['pg_listening_channels'])
        if failures:
            for failure in failures:print('SRF_SEARCH_PATH_FAILURE',failure,flush=True)
            raise AssertionError(f'{len(failures)} complete search_path assertions failed')
        print('[PREPARED SRF SEARCH PATH '+('PG18.6' if reference else 'PROTOCOL')+'] passed',flush=True)
    finally:
        try:
            c.simple_query(sock,'ROLLBACK;');c.simple_query(sock,'UNLISTEN *;')
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
