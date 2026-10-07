"""Plain physical arrays keep array OIDs in simple and extended descriptions."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('physical_array_runner', repo/'tests/compat/pg_diff_runner.py')
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
    server = r.start_ours(c); sock = server['sock']
failures = []

def query(sql):
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('PHYSICAL_ARRAY', sql, result, flush=True)
    assert result[1] is None, (sql, result)
    return result

def check(label, actual, expected):
    if actual != expected:
        failures.append((label, actual, expected))
        print('PHYSICAL_ARRAY_FAILURE', failures[-1], flush=True)

cases = [
    ('SELECT n FROM physical_array_rows', [1007]),
    ('SELECT t FROM physical_array_rows WHERE id=2', [1009]),
    ('SELECT b,f,n2 FROM physical_array_rows WHERE false', [1016,1000,1007]),
    ('SELECT * FROM physical_array_rows WHERE false', [23,1007,1009,1016,1000,1007,23,1009]),
    ('SELECT n AS numbers,t AS words FROM physical_array_rows WHERE id=1', [1007,1009]),
    ('SELECT a.n,a.t,a.b,a.f,a.n2,a."N" FROM physical_array_rows AS a WHERE a.id=1', [1007,1009,1016,1000,1007,1009]),
    ('SELECT n,id+1,t FROM physical_array_rows WHERE id=1', [1007,23,1009]),
    ('SELECT n FROM physical_array_rows WHERE id=99', [1007]),
    ('SELECT n FROM physical_array_rows ORDER BY id LIMIT 1', [1007]),
]
expected_rows = [
    [['{1,NULL,2}'], [None]], [[None]], [], [],
    [['{1,NULL,2}', '{"NULL","","a,b"}']],
    [['{1,NULL,2}', '{"NULL","","a,b"}', '{2147483648}', '{t,NULL}', '{{1,2},{3,4}}', '{quote}']],
    [['{1,NULL,2}', '2', '{"NULL","","a,b"}']], [], [['{1,NULL,2}']],
]

try:
    if reference:
        assert query('SHOW server_version_num')[0] == [['180006']]
        query('BEGIN')
    query('CREATE TEMP TABLE physical_array_rows(id INT,n INT[],t TEXT[],b BIGINT[],f BOOL[],n2 INT[][],i INT,"N" TEXT[])')
    query("INSERT INTO physical_array_rows VALUES(1,ARRAY[1,NULL,2],ARRAY['NULL','','a,b'],ARRAY[2147483648],ARRAY[true,NULL],ARRAY[ARRAY[1,2],ARRAY[3,4]],7,ARRAY['quote']),(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
    for index, (sql, oids) in enumerate(cases):
        result = query(sql)
        check(sql+' simple OIDs', result[5], oids)
        check(sql+' simple rows/NULL', result[0], expected_rows[index])
        messages = c.simple_query(sock, sql)
        fields = c.row_description_fields(messages)
        check(sql+' simple sizes', [field[4] for field in fields], [-1 if oid >= 1000 else 4 for oid in oids])
        statement = ('array_desc_s'+str(index)).encode(); portal = ('array_desc_p'+str(index)).encode()
        parse = statement+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
        bind = portal+b'\0'+statement+b'\0'+struct.pack('!HHH',0,0,0)
        execute = portal+b'\0'+struct.pack('!I',0)
        sock.sendall(c.typed(b'P',parse)+c.typed(b'D',b'S'+statement+b'\0')+
            c.typed(b'B',bind)+c.typed(b'D',b'P'+portal+b'\0')+c.typed(b'E',execute)+c.typed(b'S'))
        messages = c.read_until_ready(sock)
        described = [c.row_description_fields([message]) for message in messages if message[0] == b'T']
        check(sql+' extended description count', len(described), 2)
        for fields in described:
            check(sql+' extended OIDs', [field[3] for field in fields], oids)
            check(sql+' extended sizes', [field[4] for field in fields], [-1 if oid >= 1000 else 4 for oid in oids])
        check(sql+' execute rows', c.data_row_values(messages),
            [[None if cell is None else cell.encode() for cell in row] for row in result[0]])
        check(sql+' extended error', any(kind == b'E' for kind, _ in messages), False)
    query('CREATE TEMP TABLE physical_array_empty(n INT[])')
    check('initial empty array table OID', query('SELECT n FROM physical_array_empty')[5], [1007])
    assert not failures, failures
    print('[PHYSICAL ARRAY DESCRIPTOR '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
finally:
    if server:r.stop_ours(server)
    else:c.simple_query(sock,'ROLLBACK');sock.close()
