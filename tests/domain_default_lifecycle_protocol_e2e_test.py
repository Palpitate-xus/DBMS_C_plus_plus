"""Whole dynamic DOMAIN default contract; strict PostgreSQL 18.6 oracle."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys

repo = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('domain_runner', repo/'tests/compat/pg_diff_runner.py')
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
c = r.load_protocol_client()
reference = '--reference18' in sys.argv
server = None
if reference:
    host, port, user, database, password = r._reference_connection_settings()
    sock = socket.create_connection((host, port), timeout=15)
    c.startup_reference(sock, user, database, password=password)
else:
    server = r.start_ours(c); sock = server['sock']
failures = []

def query(sql, rows=None, oids=None):
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('DOMAIN_DEFAULT', sql, result, flush=True)
    if result[1] is not None: failures.append((sql, 'SQLSTATE', result[1], None))
    if rows is not None and result[0] != rows: failures.append((sql, 'rows', result[0], rows))
    if oids is not None and result[5] != oids: failures.append((sql, 'types', result[5], oids))
    return result

def error(sql, state):
    query('SAVEPOINT default_error')
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('DOMAIN_DEFAULT_ERROR', sql, result, flush=True)
    if result[1] != state: failures.append((sql, 'SQLSTATE', result[1], state))
    if result[0]: failures.append((sql, 'partial rows', result[0], []))
    query('ROLLBACK TO SAVEPOINT default_error')
    query('RELEASE SAVEPOINT default_error')

def describe_insert():
    sql = 'INSERT INTO default_effect_rows DEFAULT VALUES RETURNING v'
    name = b'default_metadata'
    sock.sendall(c.typed(b'P',name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
                 c.typed(b'D',b'S'+name+b'\0')+c.typed(b'S'))
    messages = c.read_until_ready(sock)
    result = r.decode_wire_result(messages, include_types=True)
    print('DOMAIN_DEFAULT_DESCRIBE',result,flush=True)
    if result[1] is not None or result[0]: failures.append(('prepare default','metadata',result,None))
    fields = c.row_description_fields(messages)
    if len(fields)!=1 or fields[0][3]!=23: failures.append(('prepare default','descriptor',fields,23))

try:
    if reference: query('SHOW server_version_num', [['180006']])
    query('BEGIN')
    query('CREATE DOMAIN live_default_base AS INT DEFAULT 1')
    query('CREATE DOMAIN live_default_mid AS live_default_base')
    query('CREATE DOMAIN live_default_outer AS live_default_mid DEFAULT NULL')
    query('CREATE TEMP TABLE live_default_rows(id INT,b live_default_base,c live_default_base DEFAULT 9,n live_default_outer,e live_default_base DEFAULT NULL)')
    query('CREATE TEMP TABLE live_mid_rows(v live_default_mid)')
    query('INSERT INTO live_default_rows(id) VALUES(1)')
    query('ALTER DOMAIN live_default_base SET DEFAULT 2')
    query('INSERT INTO live_mid_rows DEFAULT VALUES')
    query('SELECT v FROM live_mid_rows', [['1']])
    query('CREATE TEMP TABLE live_mid_after_parent(v live_default_mid)')
    query('INSERT INTO live_mid_after_parent DEFAULT VALUES')
    query('SELECT v FROM live_mid_after_parent', [['1']])
    query('INSERT INTO live_default_rows(id,b,c,n,e) VALUES(2,DEFAULT,DEFAULT,DEFAULT,DEFAULT)')
    query('SELECT * FROM live_default_rows ORDER BY id', [['1','1','9',None,None],['2','2','9',None,None]], [23,23,23,23,23])
    query('ALTER DOMAIN live_default_outer DROP DEFAULT')
    query('CREATE TEMP TABLE live_outer_after_drop(v live_default_outer)')
    query('INSERT INTO live_outer_after_drop DEFAULT VALUES')
    query('SELECT v FROM live_outer_after_drop', [[None]])
    query('INSERT INTO live_default_rows(id) VALUES(3)')
    query('SELECT b,c,n,e FROM live_default_rows WHERE id=3', [['2','9',None,None]])
    query('ALTER DOMAIN live_default_mid SET DEFAULT 4')
    query('INSERT INTO live_mid_rows DEFAULT VALUES')
    query('SELECT v FROM live_mid_rows ORDER BY v', [['1'],['4']])
    query('INSERT INTO live_default_rows(id) VALUES(4)')
    query('SELECT b,c,n,e FROM live_default_rows WHERE id=4', [['2','9',None,None]])
    query('ALTER DOMAIN live_default_mid DROP DEFAULT')
    query('ALTER TABLE live_default_rows ALTER COLUMN b SET DEFAULT 7')
    query('ALTER DOMAIN live_default_base SET DEFAULT 3')
    query('INSERT INTO live_default_rows(id) VALUES(5)')
    query('SELECT b,c,n,e FROM live_default_rows WHERE id=5', [['7','9',None,None]])
    query('ALTER TABLE live_default_rows ALTER COLUMN b DROP DEFAULT')
    query('INSERT INTO live_default_rows(id) VALUES(6)')
    query('INSERT INTO live_default_rows VALUES(7,NULL,NULL,NULL,NULL)')
    query('SELECT b,c,n,e FROM live_default_rows WHERE id=6', [['3','9',None,None]])
    query('SELECT b,c,n,e FROM live_default_rows WHERE id=7', [[None,None,None,None]])
    query('ALTER DOMAIN live_default_base DROP DEFAULT')
    query('INSERT INTO live_default_rows(id) VALUES(8)')
    query('SELECT b,c,n,e FROM live_default_rows WHERE id=8', [[None,'9',None,None]])
    query('ALTER DOMAIN live_default_base SET DEFAULT 5')
    query('SAVEPOINT domain_change')
    query('ALTER DOMAIN live_default_base SET DEFAULT 6')
    query('INSERT INTO live_default_rows(id) VALUES(9)')
    query('ROLLBACK TO SAVEPOINT domain_change')
    query('RELEASE SAVEPOINT domain_change')
    query('INSERT INTO live_default_rows(id) VALUES(10)')
    query('SELECT b,c,n,e FROM live_default_rows WHERE id=10', [['5','9',None,None]])
    query('SELECT id FROM live_default_rows WHERE id=9', [])

    query('CREATE DOMAIN live_late_default AS INT')
    query('CREATE TEMP TABLE live_late_rows(v live_late_default)')
    query('ALTER DOMAIN live_late_default SET DEFAULT 11')
    query('INSERT INTO live_late_rows DEFAULT VALUES')
    query('SELECT v FROM live_late_rows', [['11']], [23])
    query('CREATE DOMAIN live_checked_default AS INT DEFAULT 1 NOT NULL CHECK(VALUE>0)')
    query('CREATE TEMP TABLE live_checked_rows(v live_checked_default)')
    query('ALTER DOMAIN live_checked_default SET DEFAULT -1')
    error('INSERT INTO live_checked_rows DEFAULT VALUES','23514')
    query('ALTER DOMAIN live_checked_default SET DEFAULT NULL')
    error('INSERT INTO live_checked_rows DEFAULT VALUES','23502')
    query('ALTER DOMAIN live_checked_default SET DEFAULT 2')
    query('INSERT INTO live_checked_rows VALUES(DEFAULT),(DEFAULT)')
    query('SELECT v FROM live_checked_rows', [['2'],['2']])
    error('INSERT INTO live_checked_rows VALUES(NULL)','23502')

    query('CREATE TEMP SEQUENCE default_effect_sequence')
    query("CREATE DOMAIN live_effect_default AS INT DEFAULT nextval('default_effect_sequence') CHECK(VALUE<>2)")
    query('CREATE TEMP TABLE default_effect_rows(v live_effect_default)')
    error("SELECT currval('default_effect_sequence')",'55000')
    describe_insert()
    error("SELECT currval('default_effect_sequence')",'55000')
    error('INSERT INTO default_effect_rows VALUES(DEFAULT),(DEFAULT)','23514')
    query('SELECT v FROM default_effect_rows', [])
    query("SELECT currval('default_effect_sequence')",[['2']],[20])
    query('INSERT INTO default_effect_rows DEFAULT VALUES')
    query('SELECT v FROM default_effect_rows', [['3']], [23])
    query("ALTER DOMAIN live_effect_default SET DEFAULT nextval('default_effect_sequence')+10")
    query("SELECT currval('default_effect_sequence')",[['3']])
    query('INSERT INTO default_effect_rows DEFAULT VALUES')
    query('SELECT v FROM default_effect_rows ORDER BY v', [['3'],['14']])
    query('ROLLBACK')
    assert not failures, failures
    print('[DOMAIN DEFAULT '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed',flush=True)
finally:
    if server: r.stop_ours(server)
    else:
        c.simple_query(sock,'ROLLBACK');sock.close()
