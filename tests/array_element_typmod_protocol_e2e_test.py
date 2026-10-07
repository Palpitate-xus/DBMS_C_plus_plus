"""Declared element modifiers survive CREATE/ADD/ALTER and all descriptions."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('array_element_mod_runner', repo/'tests/compat/pg_diff_runner.py')
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

def query(sql):
    messages = c.simple_query(sock, sql)
    result = r.decode_wire_result(messages, include_types=True)
    print('ARRAY_ELEMENT_TYPMOD', sql, result, flush=True)
    assert result[1] is None, (sql, result)
    return messages

counter = 0
def verify(expected):
    global counter
    sql = 'SELECT v,c,n,z,u,dc,dn,maximum FROM element_mods WHERE false'
    descriptions = [c.row_description_fields(query(sql))]
    name = ('element_mod_'+str(counter)).encode()
    counter += 1
    parse = name+b'\0'+sql.encode()+b'\0'+struct.pack('!H', 0)
    bind = name+b'\0'+name+b'\0'+struct.pack('!HHH', 0, 0, 0)
    sock.sendall(c.typed(b'P', parse)+c.typed(b'D', b'S'+name+b'\0')+
                 c.typed(b'B', bind)+c.typed(b'D', b'P'+name+b'\0')+
                 c.typed(b'E', name+b'\0'+struct.pack('!I', 0))+c.typed(b'S'))
    messages = c.read_until_ready(sock)
    assert not any(kind == b'E' for kind, _ in messages), messages
    extended = [c.row_description_fields([m]) for m in messages if m[0] == b'T']
    assert len(extended) == 2 and c.data_row_values(messages) == [], messages
    descriptions += extended
    for fields in descriptions:
        actual = [(f[3], f[4], f[5]) for f in fields]
        assert actual == expected, (actual, expected)

try:
    if reference:
        assert r.decode_wire_result(query('SHOW server_version_num'))[0] == [['180006']]
        query('BEGIN')
        schema = 'array_mod_'+uuid.uuid4().hex
        query('CREATE SCHEMA '+schema)
        query('SET LOCAL search_path='+schema)
    query('CREATE TABLE element_mods(v VARCHAR(4)[],c CHAR(3)[],n NUMERIC(6,2)[],z NUMERIC(6,0)[],u VARCHAR[],dc CHAR[],dn NUMERIC[],maximum VARCHAR(65535)[])')
    initial = [(1015,-1,8),(1014,-1,7),(1231,-1,393222),(1231,-1,393220),
               (1015,-1,-1),(1014,-1,5),(1231,-1,-1),(1015,-1,65539)]
    verify(initial)
    query('ALTER TABLE element_mods ADD COLUMN unrelated INT')
    verify(initial)
    query('BEGIN' if not reference else 'SAVEPOINT before_modifiers')
    query('ALTER TABLE element_mods ALTER COLUMN v TYPE VARCHAR(7)[]')
    query('ALTER TABLE element_mods ALTER COLUMN n TYPE NUMERIC(8,3)[]')
    verify([(1015,-1,11),(1014,-1,7),(1231,-1,524295)]+initial[3:])
    query('ROLLBACK' if not reference else 'ROLLBACK TO before_modifiers')
    verify(initial)
    print('[ARRAY ELEMENT TYPMOD '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
finally:
    if server:
        r.stop_ours(server)
    else:
        c.simple_query(sock, 'ROLLBACK')
        sock.close()
