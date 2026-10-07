"""Committed UPDATE DEFAULT targets, failed-block rollback and new backend."""
import importlib.util,os,socket,sys
from pathlib import Path
repo=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('update_default_tx_runner',repo/'tests/compat/pg_diff_runner.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
reference='--reference18' in sys.argv;server=None
if reference:
    h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=15);c.startup_reference(sock,u,d,pw)
else:server=r.start_ours(c);sock=server['sock']
domain='update_default_transaction_'+str(os.getpid());table=domain+'_rows';failures=[]
def q(sql,rows=None,state=None,tag=None):
    result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True);print('UPDATE_DEFAULT_TX',sql,result,flush=True)
    for label,actual,expected in [('state',result[1],state),('rows',result[0],rows),('tag',result[4],tag)]:
        if (label=='state' or expected is not None) and actual!=expected:failures.append((sql,label,actual,expected))
try:
    if reference:q('SHOW server_version_num',[['180006']])
    q('CREATE DOMAIN '+domain+' AS INT DEFAULT 1')
    q('CREATE TABLE '+table+'(id INT,v '+domain+')')
    q('INSERT INTO '+table+' VALUES(1,10)')
    q('BEGIN');q('ALTER DOMAIN '+domain+' SET DEFAULT 2')
    q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['2']])
    q('ROLLBACK');q('SELECT v FROM '+table,[['10']])
    q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['1']],tag='UPDATE 1')
    q('BEGIN');q('ALTER DOMAIN '+domain+' SET DEFAULT 3');q('SAVEPOINT update_default_change')
    q('ALTER DOMAIN '+domain+' SET DEFAULT 4');q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['4']])
    q('ROLLBACK TO SAVEPOINT update_default_change');q('RELEASE SAVEPOINT update_default_change')
    q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['3']]);q('COMMIT')
    q('BEGIN');q('ALTER DOMAIN '+domain+' SET DEFAULT 5');q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['5']])
    q('SELECT 1/0',state='22012');q('COMMIT',tag='ROLLBACK')
    q('SELECT v FROM '+table,[['3']]);q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['3']])
    # Reopen the backend on the same committed objects. Default lookup must
    # use their stored origin and the actual current domain, not old metadata.
    sock.close()
    if reference:sock=socket.create_connection((h,p),timeout=15);c.startup_reference(sock,u,d,pw)
    else:
        sock=socket.create_connection(('127.0.0.1',server['port']),timeout=15);c.startup(sock,'alice','info');server['sock']=sock
    q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['3']],tag='UPDATE 1')
    q('ALTER TABLE '+table+' ALTER COLUMN v SET DEFAULT 8')
    q('ALTER DOMAIN '+domain+' SET DEFAULT 6')
    q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['8']])
    q('ALTER TABLE '+table+' ALTER COLUMN v DROP DEFAULT')
    q('UPDATE '+table+' SET v=DEFAULT RETURNING v',[['6']])
    assert not failures,failures
    print('[UPDATE DOMAIN DEFAULT TRANSACTION '+('STRICT18' if reference else 'PROTOCOL')+'] passed',flush=True)
finally:
    c.simple_query(sock,'ROLLBACK');c.simple_query(sock,'DROP TABLE IF EXISTS '+table);c.simple_query(sock,'DROP DOMAIN IF EXISTS '+domain)
    if server:r.stop_ours(server)
    else:sock.close()
