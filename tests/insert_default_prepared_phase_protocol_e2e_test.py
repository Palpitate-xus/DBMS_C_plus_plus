"""Analysis vs planning and genuine invalidation of named INSERT defaults."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path

repo=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('default_phase_runner',repo/'tests/compat/pg_diff_runner.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
server=None
if reference:
    h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=r.wire_timeout())
    c.startup_reference(sock,u,d,pw);r.verify_reference_version(c,sock)
else:
    server=r.start_ours(c);sock=server['sock']

def q(sql,state=None,rows=None):
    result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    print('DEFAULT_PHASE_SQL',sql,result,flush=True)
    assert result[1]==state,(sql,result,state)
    if rows is not None:assert result[0]==rows,(sql,result,rows)
    if state:assert not result[0] and result[4] is None
    return result

def phase(sql,name):
    encoded=name.encode()
    sock.sendall(c.typed(b'P',encoded+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
        c.typed(b'D',b'S'+encoded+b'\0')+c.typed(b'S'))
    result=r.decode_wire_result(c.read_until_ready(sock),include_types=True)
    print('DEFAULT_PARSE_DESCRIBE',sql,result,flush=True)
    assert result[1] is None and result[3]==['QUERY PLAN'] and result[5]==[25],result
    assert not result[0] and result[4] is None

def execute(name,state=None):
    sock.sendall(c.typed(b'B',b'\0'+name.encode()+b'\0'+struct.pack('!HHH',0,0,0))+
        c.typed(b'E',b'\0'+struct.pack('!I',0))+c.typed(b'S'))
    result=r.decode_wire_result(c.read_until_ready(sock),include_types=True)
    print('DEFAULT_EXECUTE',name,result,flush=True)
    assert result[1]==state,result
    if state:assert not result[0] and result[4] is None
    else:assert result[0] and result[4]=='EXPLAIN',result

try:
    q('BEGIN');q('CREATE TEMP SEQUENCE idp_phase_calls');q('CREATE TEMP TABLE idp_phase(v INT DEFAULT 1)')
    phase('EXPLAIN (ANALYZE TRUE,TIMING FALSE) INSERT INTO idp_phase DEFAULT VALUES RETURNING v','idp_cached')
    q('SAVEPOINT undefined_counter');q("SELECT currval('idp_phase_calls')",'55000')
    q('ROLLBACK TO undefined_counter');q('RELEASE undefined_counter')
    q("ALTER TABLE idp_phase ALTER COLUMN v SET DEFAULT nextval('idp_phase_calls')")
    execute('idp_cached');q('SELECT v FROM idp_phase',rows=[['1']])
    q("SELECT currval('idp_phase_calls')",rows=[['1']])
    q('ALTER TABLE idp_phase ALTER COLUMN v SET DEFAULT 22')
    execute('idp_cached');q('SELECT v FROM idp_phase ORDER BY v',rows=[['1'],['22']])
    q("SELECT currval('idp_phase_calls')",rows=[['1']])
    q('ALTER TABLE idp_phase ALTER COLUMN v SET DEFAULT NULL')
    execute('idp_cached');q('SELECT v FROM idp_phase WHERE v IS NULL',rows=[[None]])
    q('ALTER TABLE idp_phase ALTER COLUMN v SET DEFAULT 1/0')
    q('SAVEPOINT default_plan_error')
    phase('EXPLAIN INSERT INTO idp_phase DEFAULT VALUES','idp_error')
    execute('idp_error','22012')
    q('ROLLBACK TO default_plan_error');q('RELEASE default_plan_error')
    q("SELECT currval('idp_phase_calls')",rows=[['1']])
    q('ROLLBACK')
    print('[INSERT DEFAULT PREPARED PHASE '+('STRICT18' if reference else 'PROTOCOL')+'] passed',flush=True)
finally:
    try:c.simple_query(sock,'ROLLBACK')
    finally:
        if server:r.stop_ours(server)
        else:sock.close()
