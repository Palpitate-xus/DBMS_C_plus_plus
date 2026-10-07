"""ALTER TYPE retains complete array declarations and nullable array data."""
import importlib.util
from pathlib import Path
import socket
import sys

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('alter_array_runner', repo/'tests/compat/pg_diff_runner.py')
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

def query(sql, rows=None, oids=None):
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('ALTER_ARRAY_TYPE', sql, result, flush=True)
    assert result[1] is None, (sql, result)
    if rows is not None:
        assert result[0] == rows, (sql, result, rows)
    if oids is not None:
        assert result[5] == oids, (sql, result, oids)

def reject(sql, state):
    if reference:
        query('SAVEPOINT array_reject')
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('ALTER_ARRAY_REJECT', sql, result, flush=True)
    assert result[1] == state, (sql, result, state)
    if reference:
        query('ROLLBACK TO array_reject')
        query('RELEASE array_reject')

def modifiers(expected):
    sql = 'SELECT i,v,n FROM array_values WHERE false'
    messages = c.simple_query(sock, sql)
    result = r.decode_wire_result(messages, include_types=True)
    print('ALTER_ARRAY_MODIFIERS', sql, result, flush=True)
    assert result[1] is None and result[0] == [], (sql, result)
    actual = [(field[3],field[4],field[5]) for field in c.row_description_fields(messages)]
    assert actual == expected, (actual, expected)

try:
    if reference:
        query('SHOW server_version_num', [['180006']])
        query('BEGIN')
    query('CREATE TEMP TABLE alter_arrays(v VARCHAR(4)[],n NUMERIC(6,2)[],i INT[])')
    query('INSERT INTO alter_arrays VALUES(NULL,NULL,ARRAY[1,NULL,2])')
    query('ALTER TABLE alter_arrays ALTER COLUMN v TYPE VARCHAR(7)[], ALTER COLUMN n SET DATA TYPE NUMERIC(8,3)[]')
    query('SELECT v,n,i FROM alter_arrays', [[None,None,'{1,NULL,2}']], [1015,1231,1007])
    query('ALTER TABLE alter_arrays ALTER COLUMN i TYPE BIGINT[]')
    query('SELECT i,v,n FROM alter_arrays', [['{1,NULL,2}',None,None]], [1016,1015,1231])
    query('SELECT v,n,i FROM alter_arrays WHERE false', [], [1015,1231,1016])
    query('CREATE TEMP TABLE array_values(id INT PRIMARY KEY,tag TEXT,i INT[],v VARCHAR(4)[],n NUMERIC(6,2)[])')
    query("INSERT INTO array_values VALUES(1,'one',ARRAY[32768,NULL,-32769],ARRAY['abcd',NULL,'xy'],ARRAY[12.35,-7.89,NULL]),(2,'two',NULL,NULL,NULL),(3,'three','{}'::INT[],'{}'::VARCHAR[],'{}'::NUMERIC[]),(4,'four',ARRAY[[1,NULL],[2,3]],ARRAY[['NULL',''],['ab','cd']],ARRAY[[1.25,NULL],[2.55,-1.25]])")
    query('CREATE INDEX array_values_tag ON array_values(tag)')
    query('CREATE INDEX array_values_i ON array_values(i)')
    query('ALTER TABLE array_values ALTER COLUMN i TYPE BIGINT[]')
    modifiers([(1016,-1,-1),(1015,-1,8),(1231,-1,393222)])
    integers = [['1','{32768,NULL,-32769}'],['2',None],['3','{}'],['4','{{1,NULL},{2,3}}']]
    query('SELECT id,i FROM array_values ORDER BY id', integers, [23,1016])
    query('SELECT id FROM array_values WHERE id=4', [['4']], [23])
    query("SELECT id,i FROM array_values WHERE tag='one'", [integers[0]], [23,1016])
    query('SELECT id FROM array_values WHERE i=ARRAY[32768,NULL,-32769]::BIGINT[]', [['1']], [23])
    query('SELECT array_dims(i),array_ndims(i),cardinality(i) FROM array_values WHERE id=4', [['[1:2][1:2]','2','4']], [25,23,23])
    reject('ALTER TABLE array_values ALTER COLUMN i TYPE SMALLINT[]', '22003')
    query('SELECT id,i FROM array_values ORDER BY id', integers, [23,1016])
    reject('ALTER TABLE array_values ALTER COLUMN v TYPE VARCHAR(2)[]', '22001')
    query('SELECT id,v FROM array_values ORDER BY id', [['1','{abcd,NULL,xy}'],['2',None],['3','{}'],['4','{{"NULL",""},{ab,cd}}']], [23,1015])
    query('ALTER TABLE array_values ALTER COLUMN v TYPE VARCHAR(7)[], ALTER COLUMN n TYPE NUMERIC(4,1)[]')
    modifiers([(1016,-1,-1),(1015,-1,11),(1231,-1,262149)])
    decimals = [['1','{12.4,-7.9,NULL}'],['2',None],['3','{}'],['4','{{1.3,NULL},{2.6,-1.3}}']]
    query('SELECT id,n FROM array_values ORDER BY id', decimals, [23,1231])
    reject('ALTER TABLE array_values ALTER COLUMN n TYPE NUMERIC(2,1)[]', '22003')
    query('SELECT id,n FROM array_values ORDER BY id', decimals, [23,1231])
    reject('ALTER TABLE array_values ALTER COLUMN i TYPE NUMERIC(12,1)[], ALTER COLUMN v TYPE VARCHAR(2)[]', '22001')
    query('SELECT id,i FROM array_values ORDER BY id', integers, [23,1016])
    query("SELECT id,i FROM array_values WHERE tag='one'", [integers[0]], [23,1016])
    query('SELECT id FROM array_values WHERE i=ARRAY[32768,NULL,-32769]::BIGINT[]', [['1']], [23])
    query('SELECT i,v,n FROM array_values WHERE false', [], [1016,1015,1231])
    modifiers([(1016,-1,-1),(1015,-1,11),(1231,-1,262149)])
    query('SAVEPOINT array_rollback' if reference else 'BEGIN')
    query('ALTER TABLE array_values ALTER COLUMN i TYPE NUMERIC(12,1)[]')
    query('SELECT id,i FROM array_values ORDER BY id', [['1','{32768.0,NULL,-32769.0}'],['2',None],['3','{}'],['4','{{1.0,NULL},{2.0,3.0}}']], [23,1231])
    query('ROLLBACK TO array_rollback' if reference else 'ROLLBACK')
    if reference:
        query('RELEASE array_rollback')
    query('SELECT id,i FROM array_values ORDER BY id', integers, [23,1016])
    query('SELECT id FROM array_values WHERE i=ARRAY[32768,NULL,-32769]::BIGINT[]', [['1']], [23])
    modifiers([(1016,-1,-1),(1015,-1,11),(1231,-1,262149)])
    reject('ALTER TABLE array_values ALTER COLUMN i TYPE INT', '42804')
    query('CREATE TEMP TABLE array_assignment(t TEXT[])')
    query("INSERT INTO array_assignment VALUES(ARRAY['12',NULL])")
    reject('ALTER TABLE array_assignment ALTER COLUMN t TYPE INT[]', '42804')
    query('SELECT t FROM array_assignment', [['{12,NULL}']], [1009])
    query('CREATE TEMP TABLE array_assignment_empty(t TEXT[])')
    reject('ALTER TABLE array_assignment_empty ALTER COLUMN t TYPE INT[]', '42804')
    query('SELECT t FROM array_assignment_empty', [], [1009])
    query('CREATE TEMP TABLE array_unique(id INT PRIMARY KEY,n NUMERIC(6,2)[])')
    query('INSERT INTO array_unique VALUES(1,ARRAY[1.21]),(2,ARRAY[1.24])')
    query('CREATE UNIQUE INDEX array_unique_n ON array_unique(n)')
    reject('ALTER TABLE array_unique ALTER COLUMN n TYPE NUMERIC(3,1)[]', '23505')
    query('SELECT id,n FROM array_unique ORDER BY id', [['1','{1.21}'],['2','{1.24}']], [23,1231])
    query('SELECT id FROM array_unique WHERE n=ARRAY[1.24]::NUMERIC[]', [['2']], [23])
    print('[ALTER ARRAY TYPE ENVELOPE '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
finally:
    if server:
        r.stop_ours(server)
    else:
        c.simple_query(sock, 'ROLLBACK')
        sock.close()
