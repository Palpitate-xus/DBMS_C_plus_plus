#!/usr/bin/env python3
"""Whole SRF host/limit/unknown-input diagnostic; retained gaps are not skips."""
import importlib.util
import socket
import struct
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

    def described(sql, rows, types, labels):
        statement=b'host_description'; portal=b'host_portal'
        parse=statement+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        bind=portal+b'\0'+statement+b'\0'+struct.pack('!HHH',0,0,0)
        sock.sendall(c.typed(b'P',parse)+c.typed(b'D',b'S'+statement+b'\0')+
            c.typed(b'B',bind)+c.typed(b'D',b'P'+portal+b'\0')+
            c.typed(b'E',portal+b'\0'+struct.pack('!I',0))+c.typed(b'S'))
        messages=c.read_until_ready(sock)
        descriptions=[c.row_description_fields([message]) for message in messages if message[0]==b'T']
        print('SRF_HOST_DESCRIBE',sql,descriptions,c.data_row_values(messages),flush=True)
        if len(descriptions)!=2: failures.append((sql,'two descriptions',descriptions))
        for fields in descriptions:
            if [field[3] for field in fields]!=types or [field[0].decode() for field in fields]!=labels:
                failures.append((sql,'descriptor',fields,types,labels))
        wanted=[[None if cell is None else cell.encode() for cell in row] for row in rows]
        if c.data_row_values(messages)!=wanted or any(kind==b'E' for kind,_ in messages):
            failures.append((sql,'extended execution',messages))
        sock.sendall(c.typed(b'C',b'P'+portal+b'\0')+c.typed(b'C',b'S'+statement+b'\0')+c.typed(b'S'))
        c.read_until_ready(sock)

    def describe_error(sql,state):
        statement=b'host_bad_description'
        parse=statement+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        sock.sendall(c.typed(b'P',parse)+c.typed(b'D',b'S'+statement+b'\0')+c.typed(b'S'))
        messages=c.read_until_ready(sock)
        result=r.decode_wire_result(messages,include_types=True)
        print('SRF_HOST_DESCRIBE_ERROR',sql,result,flush=True)
        if result[1]!=state or result[0] or any(kind==b'T' for kind,_ in messages):
            failures.append((sql,'pure descriptor error',result,state))

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
        query('SELECT pg_catalog.pg_listening_channels();',[],[25],['pg_listening_channels'])
        query('SELECT "pg_catalog"."pg_listening_channels"();',[],[25],['pg_listening_channels'])
        query('SELECT pg_listening_channels(),pg_catalog.upper(\'x\') AS marker;',[],[25,25],['pg_listening_channels','marker'])
        query("SELECT pg_listening_channels(),nextval('host_effects');",[],[25,20],['pg_listening_channels','nextval'])
        # ProjectSet evaluates an ordinary target on its terminal attempt
        # too, even when this otherwise-qualified input emits no SRF row.
        query("SELECT currval('host_effects');",[['1']],[20],['currval'])
        for suffix in ['', ' WHERE false', ' LIMIT 0']:
            query('SELECT public.pg_listening_channels()'+suffix+';',state='42883')
            query("SELECT nextval('host_effects'),pg_listening_channels(),missing_host_projection()"+suffix+';',state='42883')
            query("SELECT pg_listening_channels(missing_host_argument(nextval('host_effects')))"+suffix+';',state='42883')
        query('SELECT pg_listening_channels(1);',state='42883')
        query('SELECT "Pg_listening_channels"();',state='42883')
        query("SELECT currval('host_effects');",[['1']],[20],['currval'])
        describe_error("SELECT nextval('host_effects'),pg_listening_channels(),missing_host_projection() LIMIT 0",'42883')
        describe_error("SELECT pg_listening_channels(missing_host_argument(nextval('host_effects'))) LIMIT 0",'42883')
        describe_error('SELECT pg_listening_channels(1) LIMIT 0','42883')
        query("SELECT currval('host_effects');",[['1']],[20],['currval'])
        query('LISTEN host_alpha;'); query('LISTEN host_zeta;')
        for suffix in [' WHERE false', ' LIMIT 0']:
            sql="SELECT pg_catalog.pg_listening_channels(),nextval('host_effects') AS marker"+suffix
            query(sql,[],[25,20],['pg_listening_channels','marker'])
            described(sql,[],[25,20],['pg_listening_channels','marker'])
        query("SELECT currval('host_effects');",[['1']],[20],['currval'])
        query("SELECT pg_catalog.pg_listening_channels(),pg_catalog.upper('x') AS marker;",
            [['host_alpha','X'],['host_zeta','X']],[25,25],['pg_listening_channels','marker'])
        query('SELECT unnest(ARRAY[1,2,3]),pg_listening_channels();',
            [['1','host_alpha'],['2','host_zeta'],['3',None]],[23,25],['unnest','pg_listening_channels'])
        query("SELECT pg_listening_channels(),nextval('host_effects');",
            [['host_alpha','2'],['host_zeta','3']],[25,20],['pg_listening_channels','nextval'])
        query("SELECT currval('host_effects');",[['4']],[20],['currval'])
        described('SELECT pg_catalog.pg_listening_channels() LIMIT 0',[],[25],['pg_listening_channels'])
        query('BEGIN;')
        query("CREATE FUNCTION pg_listening_channels() RETURNS TEXT LANGUAGE plpgsql AS $$BEGIN RETURN 'public scalar'; END$$")
        query('SELECT public.pg_listening_channels();',[['public scalar']],[25],['pg_listening_channels'])
        query(channels,[['host_alpha'],['host_zeta']],[25],['pg_listening_channels'])
        query('ROLLBACK;')
        assert not failures, failures
        print('[PREPARED SRF HOST OWNERSHIP '+('PG18.6' if reference else 'PROTOCOL')+'] passed',flush=True)
    finally:
        try:c.simple_query(sock,'UNLISTEN *;')
        finally:
            if server:r.stop_ours(server)
            else:sock.close()


if __name__ == '__main__':main()
