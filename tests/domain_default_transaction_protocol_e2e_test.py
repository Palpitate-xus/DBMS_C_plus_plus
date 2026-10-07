"""ALTER DOMAIN defaults on preexisting committed objects; strict 180006."""
import importlib.util
import os
from pathlib import Path
import socket
import sys

repo=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('runner',repo/'tests/compat/pg_diff_runner.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
reference='--reference18' in sys.argv
server=None
if reference:
    host,port,user,database,password=r._reference_connection_settings()
    sock=socket.create_connection((host,port),timeout=15)
    c.startup_reference(sock,user,database,password=password)
else:
    server=r.start_ours(c);sock=server['sock']
domain='transaction_default_'+str(os.getpid())
failures=[]
def q(sql,rows=None,state=None,tag=None):
    result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    print('DOMAIN_DEFAULT_TX',sql,result,flush=True)
    if result[1]!=state:failures.append((sql,'state',result[1],state))
    if rows is not None and result[0]!=rows:failures.append((sql,'rows',result[0],rows))
    if tag is not None and result[4]!=tag:failures.append((sql,'tag',result[4],tag))
    return result
try:
    if reference:q('SHOW server_version_num',[['180006']])
    q('CREATE DOMAIN '+domain+' AS INT DEFAULT 1')
    q('CREATE TEMP TABLE default_transaction_rows(v '+domain+')')
    q('BEGIN')
    q('ALTER DOMAIN '+domain+' SET DEFAULT 2')
    q('INSERT INTO default_transaction_rows DEFAULT VALUES')
    q('SELECT v FROM default_transaction_rows',[['2']])
    q('ROLLBACK')
    q('INSERT INTO default_transaction_rows DEFAULT VALUES')
    q('SELECT v FROM default_transaction_rows',[['1']])
    q('BEGIN')
    q('ALTER DOMAIN '+domain+' SET DEFAULT 2')
    q('SAVEPOINT before_second_default')
    q('ALTER DOMAIN '+domain+' SET DEFAULT 3')
    q('INSERT INTO default_transaction_rows DEFAULT VALUES')
    q('ROLLBACK TO SAVEPOINT before_second_default')
    q('RELEASE SAVEPOINT before_second_default')
    q('INSERT INTO default_transaction_rows DEFAULT VALUES')
    q('COMMIT')
    q('SELECT v FROM default_transaction_rows ORDER BY v',[['1'],['2']])
    q('BEGIN')
    q('ALTER DOMAIN '+domain+' SET DEFAULT 3')
    q('SELECT 1/0',state='22012')
    q('COMMIT',tag='ROLLBACK')
    q('INSERT INTO default_transaction_rows DEFAULT VALUES')
    q('SELECT v FROM default_transaction_rows ORDER BY v',[['1'],['2'],['2']])
    q('BEGIN')
    q('ALTER DOMAIN '+domain+' DROP DEFAULT')
    q('INSERT INTO default_transaction_rows DEFAULT VALUES')
    q('ROLLBACK')
    q('INSERT INTO default_transaction_rows DEFAULT VALUES')
    q('SELECT v FROM default_transaction_rows ORDER BY v',[['1'],['2'],['2'],['2']])
    assert not failures,failures
    print('[DOMAIN DEFAULT TRANSACTION '+('STRICT18' if reference else 'OURS')+'] passed',flush=True)
finally:
    c.simple_query(sock,'ROLLBACK')
    c.simple_query(sock,'DROP TABLE IF EXISTS default_transaction_rows')
    c.simple_query(sock,'DROP DOMAIN IF EXISTS '+domain)
    if server:r.stop_ours(server)
    else:sock.close()
