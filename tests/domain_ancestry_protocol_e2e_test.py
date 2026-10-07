"""Whole DOMAIN inheritance contract; strict PostgreSQL 18.6 oracle."""
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
describe_count = 0

def query(sql, expected_rows=None, expected_oids=None):
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('DOMAIN_ANCESTRY', sql, result, flush=True)
    assert result[1] is None, (sql, result)
    if expected_rows is not None and result[0] != expected_rows:
        failures.append((sql, 'rows', result[0], expected_rows))
    if expected_oids is not None and result[5] != expected_oids:
        failures.append((sql, 'types', result[5], expected_oids))
    return result

def error(sql, state):
    query('SAVEPOINT domain_error')
    result = r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    print('DOMAIN_ANCESTRY_ERROR', sql, result, flush=True)
    if result[1] != state: failures.append((sql, 'SQLSTATE', result[1], state))
    query('ROLLBACK TO SAVEPOINT domain_error')
    query('RELEASE SAVEPOINT domain_error')

def describe(sql, expected):
    global describe_count
    messages = c.simple_query(sock, sql)
    decoded = r.decode_wire_result(messages, include_types=True)
    assert decoded[1] is None, (sql, decoded)
    fields = [c.row_description_fields(messages)]
    name = ('domain_desc_'+str(describe_count)).encode(); describe_count += 1
    parse = name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0)
    bind = name+b'\0'+name+b'\0'+struct.pack('!HHH',0,0,0)
    sock.sendall(c.typed(b'P',parse)+c.typed(b'D',b'S'+name+b'\0')+
                 c.typed(b'B',bind)+c.typed(b'D',b'P'+name+b'\0')+
                 c.typed(b'E',name+b'\0'+struct.pack('!I',0))+c.typed(b'S'))
    extended = c.read_until_ready(sock)
    decoded = r.decode_wire_result(extended, include_types=True)
    assert decoded[1] is None, (sql, decoded)
    fields += [c.row_description_fields([m]) for m in extended if m[0] == b'T']
    assert len(fields) == 3, (sql, fields)
    for description in fields:
        actual = [(f[3],f[4],f[5],f[2],bool(f[1])) for f in description]
        print('DOMAIN_DESCRIPTION',sql,actual,flush=True)
        if actual != expected: failures.append((sql,'description',actual,expected))

try:
    if reference: query('SHOW server_version_num', [['180006']])
    query('BEGIN')
    # The original unfiltered priority fixture is retained verbatim.
    query('CREATE DOMAIN priority_pattern_text AS TEXT')
    query('CREATE DOMAIN priority_pattern_chain AS priority_pattern_text')
    query('CREATE DOMAIN priority_pattern_int AS INT')
    query('CREATE DOMAIN priority_pattern_char AS CHAR(2)')
    query('CREATE TEMP TABLE priority_pattern_domains(t priority_pattern_text,ch priority_pattern_chain,n priority_pattern_int,c priority_pattern_char)')
    query("INSERT INTO priority_pattern_domains VALUES('x','y',1,'z')")
    query('SELECT * FROM priority_pattern_domains', [['x','y','1','z ']], [25,25,23,1042])

    query("CREATE DOMAIN ancestry_base AS TEXT DEFAULT 'seed' NOT NULL CHECK(VALUE <> 'VALUE')")
    query("CREATE DOMAIN ancestry_mid AS ancestry_base CHECK(length(VALUE) < 6)")
    query("CREATE DOMAIN ancestry_outer AS ancestry_mid DEFAULT 'near' CHECK(VALUE <> 'bad')")
    query("CREATE TEMP TABLE ancestry_rows(id INT,v ancestry_outer CHECK(v <> 'col'))")
    query('INSERT INTO ancestry_rows(id) VALUES(1)')
    query("INSERT INTO ancestry_rows VALUES(2,'ok')")
    query('SELECT * FROM ancestry_rows ORDER BY id', [['1','near'],['2','ok']], [23,25])
    error("INSERT INTO ancestry_rows VALUES(3,'VALUE')", '23514')
    error("INSERT INTO ancestry_rows VALUES(3,'longer')", '23514')
    error("INSERT INTO ancestry_rows VALUES(3,'bad')", '23514')
    error("INSERT INTO ancestry_rows VALUES(3,'col')", '23514')
    error('INSERT INTO ancestry_rows VALUES(3,NULL)', '23502')
    error('UPDATE ancestry_rows SET v=NULL WHERE id=2', '23502')
    error("UPDATE ancestry_rows SET v='VALUE' WHERE id=2", '23514')
    query('SELECT * FROM ancestry_rows ORDER BY id', [['1','near'],['2','ok']])
    query('CREATE DOMAIN ancestry_null_default AS ancestry_mid DEFAULT NULL')
    query('CREATE TEMP TABLE ancestry_null(v ancestry_null_default)')
    error('INSERT INTO ancestry_null DEFAULT VALUES', '23502')
    query("CREATE TEMP TABLE ancestry_override(v ancestry_null_default DEFAULT 'table')")
    query('INSERT INTO ancestry_override DEFAULT VALUES')
    query('SELECT v FROM ancestry_override', [['table']], [25])
    query("CREATE DOMAIN ancestry_literal_base AS TEXT DEFAULT 'NULL' NOT NULL CHECK(VALUE <> 'VALUE|text')")
    query('CREATE DOMAIN ancestry_literal_child AS ancestry_literal_base')
    query('CREATE TEMP TABLE ancestry_literal_rows(v ancestry_literal_child)')
    query('INSERT INTO ancestry_literal_rows DEFAULT VALUES')
    query('SELECT v FROM ancestry_literal_rows', [['NULL']], [25])
    error("INSERT INTO ancestry_literal_rows VALUES('VALUE|text')", '23514')
    error('CREATE DOMAIN ancestry_bad_role AS TEXT CHECK("VALUE" <> \'bad\')', '42703')
    error('CREATE DOMAIN ancestry_bad_bool AS TEXT CHECK(VALUE)', '42804')
    error('CREATE DOMAIN ancestry_bad_syntax AS TEXT CHECK()', '42601')

    query("CREATE DOMAIN ancestry_char_base AS CHAR(3) DEFAULT 'ab' CHECK(VALUE <> 'bad')")
    query('CREATE DOMAIN ancestry_char AS ancestry_char_base')
    query('CREATE TEMP TABLE ancestry_chars(v ancestry_char)')
    query('INSERT INTO ancestry_chars DEFAULT VALUES')
    query('SELECT v FROM ancestry_chars', [['ab ']], [1042])
    error("INSERT INTO ancestry_chars VALUES('bad')", '23514')
    error("INSERT INTO ancestry_chars VALUES('toolong')", '22001')
    query('CREATE DOMAIN ancestry_numeric_base AS NUMERIC(6,2) DEFAULT 1.25 CHECK(VALUE > 0)')
    query('CREATE DOMAIN ancestry_numeric AS ancestry_numeric_base CHECK(VALUE < 10)')
    query('CREATE TEMP TABLE ancestry_numbers(v ancestry_numeric)')
    query('INSERT INTO ancestry_numbers DEFAULT VALUES')
    query('SELECT v FROM ancestry_numbers', [['1.25']], [1700])
    describe('SELECT v AS "Value" FROM ancestry_chars WHERE false', [(1042,-1,7,1,True)])
    describe('SELECT v FROM ancestry_numbers WHERE false', [(1700,-1,(6<<16)+2+4,1,True)])
    describe('SELECT * FROM ancestry_rows WHERE false', [(23,4,-1,1,True),(25,-1,-1,2,True)])
    error('INSERT INTO ancestry_numbers VALUES(0)', '23514')
    error('INSERT INTO ancestry_numbers VALUES(10)', '23514')

    query('CREATE SCHEMA "DomainScope"')
    query('CREATE DOMAIN "DomainScope"."Mixed" AS TEXT DEFAULT \'quoted\' CHECK(VALUE <> \'bad\')')
    query('CREATE DOMAIN "DomainScope"."Chain" AS "DomainScope"."Mixed"')
    query('CREATE DOMAIN mixed AS INT DEFAULT 8')
    query('CREATE TEMP TABLE ancestry_scope(a "DomainScope"."Chain",b mixed)')
    query('INSERT INTO ancestry_scope DEFAULT VALUES')
    query('SELECT a,b FROM ancestry_scope', [['quoted','8']], [25,23])
    error('CREATE TEMP TABLE ancestry_wrong(v "DomainScope".mixed)', '42704')
    query('ALTER TABLE ancestry_scope ADD COLUMN c "DomainScope"."Chain"')
    query('SELECT c FROM ancestry_scope', [['quoted']], [25])
    query('SAVEPOINT domain_ddl')
    query('CREATE DOMAIN ancestry_rolled AS ancestry_outer')
    query('CREATE TEMP TABLE ancestry_rolled_rows(v ancestry_rolled)')
    query("INSERT INTO ancestry_rolled_rows VALUES('ok')")
    query('ROLLBACK TO SAVEPOINT domain_ddl')
    query('RELEASE SAVEPOINT domain_ddl')
    error('CREATE TEMP TABLE ancestry_rolled_missing(v ancestry_rolled)', '42704')
    assert not failures, failures
    print('[DOMAIN ANCESTRY '+('STRICT PG18.6' if reference else 'PROTOCOL')+'] passed', flush=True)
finally:
    if server: r.stop_ours(server)
    else:
        c.simple_query(sock, 'ROLLBACK'); sock.close()
