#!/usr/bin/env python3
"""Observe separate Parse/Describe, Bind/Describe and Execute protocol phases.

This is a manual diagnostic, not a compatibility pass gate. Its zero exit only
means every observation completed; compare each phase with the PG18 fixture.
Use --reference18 for the configured PGREF endpoint, otherwise DBMS_MAIN.
"""
import argparse
import importlib.util
import json
import socket
import struct
from pathlib import Path

options=argparse.ArgumentParser(description=__doc__)
options.add_argument('--reference18',action='store_true')
arguments=options.parse_args()
src=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('r244_probe_runner',src/'tests/compat/pg_diff_runner.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);client=r.load_protocol_client()
reference=arguments.reference18
server=None
if reference:
    host,port,user,database,password=r._reference_connection_settings()
    sock=socket.create_connection((host,port),timeout=r.wire_timeout())
    client.startup_reference(sock,user,database,password=password);r.verify_reference_version(client,sock)
else:
    server=r.start_ours(client);sock=server['sock']

def decode(messages):
    result=r.decode_wire_result(messages,include_types=True)
    return dict(rows=result[0],state=result[1],message=result[2],columns=result[3],tag=result[4],oids=result[5],
        messages=[kind.decode() for kind,_ in messages],fields=[[field[0].decode(),*field[1:]] for field in client.row_description_fields(messages)] if any(kind==b'T' for kind,_ in messages) else [],
        ready=[payload.decode() for kind,payload in messages if kind==b'Z'])
def query(sql):
    value=decode(client.simple_query(sock,sql));assert value['state'] is None,(sql,value);return value
def exchange(parts):
    sock.sendall(b''.join(parts)+client.typed(b'S'));return decode(client.read_until_ready(sock))

cases=[
    ('literal-error',"SELECT 1/0 AS v",[],[]),
    ('typed-parameter-error',"SELECT $1::int / 0 AS v",[23],['1']),
    ('typed-cast-error',"SELECT 1::integer/0 AS v",[],[]),
    ('abs-error',"SELECT abs(-2147483647 - 1) AS v",[],[]),
    ('sqrt-error',"SELECT sqrt(-1::float8) AS v",[],[]),
    ('date-part-unit-error',"SELECT date_part('nonesuch',INTERVAL '1 day') AS v",[],[]),
    ('date-part-inputs-error',"SELECT date_part($1,$2::interval) AS v",[25,1186],['nonesuch','1 day']),
    ('extract-unit-error',"SELECT extract(nonesuch FROM INTERVAL '1 day') AS v",[],[]),
    ('subquery-error',"SELECT (SELECT 1/0) AS v LIMIT 0",[],[]),
    ('source-error',"SELECT 1/0 AS v FROM (VALUES(1)) AS t(x) WHERE false",[],[]),
    ('literal-dead-error',"SELECT 1/0 AS v WHERE false",[],[]),
    ('dead-arm',"SELECT CASE WHEN false THEN 1/0 ELSE 7 END AS v",[],[]),
    ('immutable-success',"SELECT upper('abc') AS v",[],[]),
    ('volatile-success',"SELECT r244_writer() AS v",[],[]),
    ('volatile-suppressed',"SELECT r244_writer() AS v WHERE false",[],[]),
]
observations=[]
try:
    for index,(label,sql,oids,values) in enumerate(cases):
        query('BEGIN;')
        query('CREATE TEMP SEQUENCE r244_effects;')
        query("CREATE FUNCTION r244_writer() RETURNS INT LANGUAGE SQL VOLATILE AS 'SELECT nextval(''r244_effects'')::int';")
        statement=('r244_stmt_'+str(index)).encode();portal=('r244_portal_'+str(index)).encode()
        parsed=exchange([client.typed(b'P',statement+b'\0'+sql.encode()+b'\0'+struct.pack('!H',len(oids))+b''.join(struct.pack('!I',oid) for oid in oids)),
            client.typed(b'D',b'S'+statement+b'\0')])
        row=dict(label=label,sql=sql,parameterOids=oids,parseDescribe=parsed)
        if not parsed['state']:
            row['describeEffectCheck']=query("SELECT nextval('r244_effects');")
            query("SELECT setval('r244_effects',1,false);")
            parameters=b''.join(struct.pack('!i',-1) if value is None else struct.pack('!i',len(value.encode()))+value.encode() for value in values)
            bound=exchange([client.typed(b'B',portal+b'\0'+statement+b'\0'+struct.pack('!HH',0,len(values))+parameters+struct.pack('!H',0)),
                client.typed(b'D',b'P'+portal+b'\0')])
            row['bindDescribe']=bound
            if not bound['state']:
                row['bindEffectCheck']=query("SELECT nextval('r244_effects');")
                row['execute']=exchange([client.typed(b'E',portal+b'\0'+struct.pack('!I',0))])
        # Error paths deliberately leave the explicit transaction failed.
        query('ROLLBACK;')
        print('BIND_PHASE_OBSERVATION',json.dumps(row,ensure_ascii=False),flush=True)
        observations.append(row)
    print('BIND_PHASE_OBSERVER_COMPLETE',json.dumps(dict(observedControls=len(observations),
        expectedControls=len(cases),reference18=reference,diagnosticNotCompatibilityPass=True)),flush=True)
finally:
    if server:r.stop_ours(server)
    else:sock.close()
