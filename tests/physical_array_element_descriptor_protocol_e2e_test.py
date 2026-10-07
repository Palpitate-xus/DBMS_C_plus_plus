"""Actual physical array element modifiers and protocol origin metadata."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('array_typmod_runner', repo/'tests/compat/pg_diff_runner.py')
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
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
statement_count = 0

def check(label, actual, expected):
    if actual != expected:
        failures.append((label, actual, expected))
        print('ARRAY_TYPMOD_FAILURE', failures[-1], flush=True)

def query(sql):
    messages = c.simple_query(sock, sql)
    result = r.decode_wire_result(messages, include_types=True)
    print('ARRAY_TYPMOD_SQL', sql, result, flush=True)
    assert result[1] is None, (sql, result)
    return messages, result

def verify(sql, expected, expected_rows):
    global statement_count
    messages, result = query(sql)
    check(sql+' rows/NULL', result[0], expected_rows)
    sets = [('simple', c.row_description_fields(messages))]
    name = ('array_typmod_'+str(statement_count)).encode(); statement_count += 1
    parse = name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
    bind = name+b'\0'+name+b'\0'+struct.pack('!HHH',0,0,0)
    sock.sendall(c.typed(b'P',parse)+c.typed(b'D',b'S'+name+b'\0')+
        c.typed(b'B',bind)+c.typed(b'D',b'P'+name+b'\0')+
        c.typed(b'E',name+b'\0'+struct.pack('!I',0))+c.typed(b'S'))
    extended = c.read_until_ready(sock)
    desc = [c.row_description_fields([message]) for message in extended if message[0] == b'T']
    check(sql+' extended error', any(kind == b'E' for kind, _ in extended), False)
    check(sql+' statement/portal descriptions', len(desc), 2)
    sets += [('extended '+str(i), fields) for i, fields in enumerate(desc)]
    check(sql+' extended rows/NULL', c.data_row_values(extended),
        [[None if cell is None else cell.encode() for cell in row] for row in expected_rows])
    for mode, fields in sets:
        actual = [(field[3], field[4], field[5], field[2]) for field in fields]
        check(sql+' '+mode+' OID/size/typmod/attribute', actual, expected)
        check(sql+' '+mode+' relation-origin present', [field[1] != 0 for field in fields],
            [attribute != 0 for _, _, _, attribute in expected])

try:
    if reference:
        assert query('SHOW server_version_num')[1][0] == [['180006']]
        query('BEGIN')
    query('CREATE TEMP TABLE array_element_mods(id INT,v VARCHAR(4)[],c CHAR(3)[],n NUMERIC(6,2)[],"V" VARCHAR(2)[],u VARCHAR[],z NUMERIC(6,0)[],dc CHAR[],dn NUMERIC[])')
    specs = [(23,4,-1,1),(1015,-1,8,2),(1014,-1,7,3),(1231,-1,393222,4),
        (1015,-1,6,5),(1015,-1,-1,6),(1231,-1,393220,7),(1014,-1,5,8),(1231,-1,-1,9)]
    verify('SELECT * FROM array_element_mods', specs, [])
    query('INSERT INTO array_element_mods(id) VALUES(1)')
    verify('SELECT * FROM array_element_mods', specs, [[str(1)]+[None]*8])
    verify('SELECT v,c,n FROM array_element_mods WHERE false', specs[1:4], [])
    verify('SELECT v,c,n,"V",u,z,dc,dn FROM array_element_mods', specs[1:], [[None]*8])
    verify('SELECT v AS strings,c AS chars,n AS numbers FROM array_element_mods', specs[1:4], [[None]*3])
    verify('SELECT a.v,a.c,a.n,a."V" FROM array_element_mods AS a', specs[1:5], [[None]*4])
    verify('SELECT id+1 AS v,v AS changed,c FROM array_element_mods WHERE false',
        [(23,4,-1,0),specs[1],specs[2]], [])
    verify('SELECT v AS "V", "V" AS v FROM array_element_mods WHERE false', [specs[1],specs[4]], [])
    verify('SELECT n,v FROM array_element_mods ORDER BY id LIMIT 1', [specs[3],specs[1]], [[None,None]])
    broad = [('d','DATE',1182),('ti','TIME',1183),('iv','INTERVAL',1187),
        ('uid','UUID',2951),('by','BYTEA',1001),('net','INET',1041),('p','POINT',1017),
        ('ts','TIMESTAMP',1115),('tt','TIMESTAMPTZ',1185),('tz','TIMETZ',1270),
        ('ci','CIDR',651),('ma','MACADDR',1040),('m8','MACADDR8',775),
        ('ls','LSEG',1018),('pa','PATH',1019),('bo','BOX',1020),('po','POLYGON',1027),
        ('li','LINE',629),('cr','CIRCLE',719),('re','REAL',1021),('dp','DOUBLE PRECISION',1022),
        ('js','JSON',199),('jb','JSONB',3807),('xm','XML',143)]
    query('CREATE TEMP TABLE broad_array_types('+','.join(name+' '+kind+'[]' for name,kind,_ in broad)+')')
    broad_specs = [(oid,-1,-1,index+1) for index,(_,_,oid) in enumerate(broad)]
    verify('SELECT * FROM broad_array_types WHERE false', broad_specs, [])
    query('INSERT INTO broad_array_types VALUES('+','.join(['NULL']*len(broad))+')')
    verify('SELECT * FROM broad_array_types', broad_specs, [[None]*len(broad)])
    verify('SELECT a.d AS dates,a.ti AS times,a.iv,a.uid,a.by,a.net,a.p FROM broad_array_types AS a', broad_specs[:7], [[None]*7])
    print('ARRAY_TYPMOD_FAILURES', failures, flush=True)
    assert not failures, failures
    print('[PHYSICAL ARRAY ELEMENT MODIFIERS '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
finally:
    if server: r.stop_ours(server)
    else: c.simple_query(sock,'ROLLBACK'); sock.close()
