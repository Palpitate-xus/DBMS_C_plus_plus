"""Descriptors follow actual source/ordinal, not physical paths or labels."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('physical_origin_runner', repo/'tests/compat/pg_diff_runner.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
c = r.load_protocol_client()
reference = '--reference18' in sys.argv[1:]
server = None
if reference:
    host, port, user, database, password = r._reference_connection_settings()
    sock = socket.create_connection((host, port), timeout=15)
    c.startup_reference(sock, user, database, password=password)
else:
    server = r.start_ours(c)
    sock = server['sock']
failures = []
counter = 0

def query(sql):
    messages = c.simple_query(sock, sql)
    result = r.decode_wire_result(messages, include_types=True)
    print('PHYSICAL_ORIGIN', sql, result, flush=True)
    assert result[1] is None, (sql, result)
    return messages

def check(label, actual, expected):
    if actual != expected:
        failures.append((label, actual, expected))
        print('PHYSICAL_ORIGIN_FAILURE', failures[-1], flush=True)

def verify(sql, expected, extended=True):
    global counter
    descriptions = [c.row_description_fields(query(sql))]
    if extended:
        name = ('physical_origin_'+str(counter)).encode()
        counter += 1
        parse = name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        bind = name+b'\0'+name+b'\0'+struct.pack('!HHH',0,0,0)
        sock.sendall(c.typed(b'P',parse)+c.typed(b'D',b'S'+name+b'\0')+
            c.typed(b'B',bind)+c.typed(b'D',b'P'+name+b'\0')+
            c.typed(b'E',name+b'\0'+struct.pack('!I',0))+c.typed(b'S'))
        messages = c.read_until_ready(sock)
        check(sql+' error', any(kind == b'E' for kind,_ in messages), False)
        fields = [c.row_description_fields([m]) for m in messages if m[0] == b'T']
        check(sql+' Statement/Portal descriptions', len(fields), 2)
        descriptions += fields
    for index, fields in enumerate(descriptions):
        actual = [(f[3],f[4],f[5],f[2],bool(f[1])) for f in fields]
        check(sql+' metadata '+str(index), actual, expected)
    return descriptions[0]

try:
    if reference:
        assert r.decode_wire_result(query('SHOW server_version_num'))[0] == [['180006']]
        query('BEGIN')
    table = 'origin_'+uuid.uuid4().hex
    query('CREATE TABLE public.'+table+'(id INT,v VARCHAR(8)[])')
    query('CREATE TEMP TABLE '+table+'(id INT,v VARCHAR(4)[],"V" CHAR(3)[])')
    temp = [(23,4,-1,1,True),(1015,-1,8,2,True),(1014,-1,7,3,True)]
    actual = verify('SELECT * FROM '+table+' WHERE false', temp)
    public = verify('SELECT * FROM public.'+table+' WHERE false',
                    [(23,4,-1,1,True),(1015,-1,12,2,True)])
    check('TEMP/public identities differ', actual[0][1] != public[0][1], True)
    verify('SELECT pg_temp.'+table+'.v FROM pg_temp.'+table+' WHERE false', [temp[1]])
    verify('SELECT "A"."V", "A".v AS "V" FROM '+table+' AS "A" WHERE false', [temp[2],temp[1]])
    verify('SELECT id+1 AS v,v AS id FROM '+table+' WHERE false',
           [(23,4,-1,0,False),temp[1]])
    verify('SELECT v AS "V","V" AS v FROM '+table+' WHERE false', [temp[1],temp[2]])
    verify("SELECT CAST('x' AS TEXT) AS v FROM "+table+' WHERE false', [(25,-1,-1,0,False)])
    # A CTE shadows an identically named physical range; this legacy Describe
    # adapter has no CTE output support, so retain the Simple origin control.
    verify('WITH '+table+' AS (SELECT 1 AS v) SELECT v FROM '+table,
           [(23,4,-1,0,False)], extended=False)
    verify('INSERT INTO '+table+' VALUES(1,NULL,NULL) RETURNING v AS "V","V" AS v', [temp[1],temp[2]])
    verify('UPDATE '+table+' AS "A" SET id=id RETURNING "A"."V" AS v,"A".v AS "V"', [temp[2],temp[1]])
    verify('DELETE FROM '+table+' WHERE false RETURNING v,"V"', [temp[1],temp[2]])
    assert not failures, failures
    print('[PHYSICAL COLUMN ORIGIN '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
finally:
    if server:
        r.stop_ours(server)
    else:
        c.simple_query(sock,'ROLLBACK')
        sock.close()
