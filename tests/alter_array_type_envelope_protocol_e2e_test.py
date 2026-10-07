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
    print('[ALTER ARRAY TYPE ENVELOPE '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
finally:
    if server:
        r.stop_ours(server)
    else:
        c.simple_query(sock, 'ROLLBACK')
        sock.close()
