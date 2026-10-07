"""Actual UPDATE DEFAULT row demand, current sources and rollback; strict180006."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys

repo=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('update_default_runner',repo/'tests/compat/pg_diff_runner.py')
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);c=r.load_protocol_client()
reference='--reference18' in sys.argv
server=None
if reference:
    h,p,u,d,pw=r._reference_connection_settings();sock=socket.create_connection((h,p),timeout=15)
    c.startup_reference(sock,u,d,pw)
else:
    server=r.start_ours(c);sock=server['sock']
failures=[]
in_transaction=False
def q(sql,rows=None,state=None,oids=None,tag=None):
    global in_transaction
    control=sql.split()[0].upper() in {'BEGIN','COMMIT','ROLLBACK','SAVEPOINT','RELEASE'}
    guard=in_transaction and state is None and not control
    if guard:c.simple_query(sock,'SAVEPOINT update_default_whole_guard')
    result=r.decode_wire_result(c.simple_query(sock,sql),include_types=True)
    if guard:
        if result[1] is not None:c.simple_query(sock,'ROLLBACK TO SAVEPOINT update_default_whole_guard')
        c.simple_query(sock,'RELEASE SAVEPOINT update_default_whole_guard')
    if sql=='BEGIN' and result[1] is None:in_transaction=True
    if sql=='ROLLBACK' and result[1] is None:in_transaction=False
    print('UPDATE_DEFAULT',sql,result,flush=True)
    for label,actual,expected in [('state',result[1],state),('rows',result[0],rows),('oids',result[5],oids),('tag',result[4],tag)]:
        if (label=='state' or expected is not None) and actual!=expected:
            failures.append((sql,label,actual,expected))
    return result
def error(sql,state):
    q('SAVEPOINT update_default_error')
    q(sql,rows=[],state=state)
    q('ROLLBACK TO SAVEPOINT update_default_error');q('RELEASE SAVEPOINT update_default_error')
def describe(sql):
    name=b'update_default_description'
    sock.sendall(c.typed(b'P',name+b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
        c.typed(b'D',b'S'+name+b'\0')+c.typed(b'S'))
    messages=c.read_until_ready(sock);result=r.decode_wire_result(messages,include_types=True)
    print('UPDATE_DEFAULT_DESCRIBE',sql,result,flush=True)
    if result[1] is not None or result[0] or result[5]!=[23]:failures.append((sql,'describe',result,[23]))
    sock.sendall(c.typed(b'C',b'S'+name+b'\0')+c.typed(b'S'));c.read_until_ready(sock)
try:
    if reference:q('SHOW server_version_num',[['180006']])
    q('BEGIN')
    q('CREATE DOMAIN update_direct_default AS INT DEFAULT 1')
    q('CREATE DOMAIN update_child_default AS update_direct_default')
    q('CREATE TEMP TABLE update_default_rows(id INT PRIMARY KEY,d update_direct_default,e update_direct_default DEFAULT 9,n update_direct_default DEFAULT NULL,c update_child_default,b INT DEFAULT 7)')
    q('INSERT INTO update_default_rows VALUES(1,99,99,99,99,99),(2,98,98,98,98,98)')
    q('ALTER DOMAIN update_direct_default SET DEFAULT 2')
    q('UPDATE update_default_rows SET d=DEFAULT,e=DEFAULT,n=DEFAULT,c=DEFAULT,b=DEFAULT WHERE id=1 RETURNING d,e,n,c,b',
        [['2','9',None,'1','7']],oids=[23]*5,tag='UPDATE 1')
    q('UPDATE update_default_rows AS x SET d=DEFAULT WHERE CASE WHEN x.id=2 THEN true ELSE false END RETURNING x.id,x.d',
        [['2','2']],oids=[23,23],tag='UPDATE 1')
    q('ALTER TABLE update_default_rows ALTER COLUMN d SET DEFAULT 5')
    q('ALTER DOMAIN update_direct_default SET DEFAULT 3')
    q('UPDATE update_default_rows SET d=DEFAULT WHERE id=1 RETURNING d',[['5']],tag='UPDATE 1')
    q('ALTER TABLE update_default_rows ALTER COLUMN d DROP DEFAULT')
    q('UPDATE update_default_rows SET d=DEFAULT WHERE id=1 RETURNING d',[['3']],tag='UPDATE 1')
    q('ALTER DOMAIN update_direct_default DROP DEFAULT')
    q('UPDATE update_default_rows SET d=DEFAULT WHERE id=1 RETURNING d',[[None]],tag='UPDATE 1')
    q('UPDATE update_default_rows SET d=NULL WHERE id=2 RETURNING d',[[None]])
    q('ALTER DOMAIN update_direct_default SET DEFAULT 4')
    q('SAVEPOINT before_default_change')
    q('ALTER DOMAIN update_direct_default SET DEFAULT 6')
    q('UPDATE update_default_rows SET d=DEFAULT WHERE id=1 RETURNING d',[['6']])
    q('ROLLBACK TO SAVEPOINT before_default_change');q('RELEASE SAVEPOINT before_default_change')
    q('UPDATE update_default_rows SET d=DEFAULT WHERE id=1 RETURNING d',[['4']])
    q('WITH input AS(SELECT 2 AS id) UPDATE update_default_rows AS x SET d=DEFAULT FROM input WHERE x.id=input.id RETURNING x.d',
        [['4']],oids=[23],tag='UPDATE 1')
    q('WITH changed AS(UPDATE update_default_rows SET d=DEFAULT WHERE id=2 RETURNING d) SELECT d FROM changed', [['4']],oids=[23])
    q('CREATE DOMAIN update_checked_default AS INT DEFAULT 1 NOT NULL CHECK(VALUE>0)')
    q('CREATE TEMP TABLE update_checked_rows(id INT,v update_checked_default)')
    q('INSERT INTO update_checked_rows VALUES(1,10),(2,20)')
    q('ALTER DOMAIN update_checked_default SET DEFAULT NULL')
    q('UPDATE update_checked_rows SET v=DEFAULT WHERE false RETURNING v',[],oids=[23],tag='UPDATE 0')
    error('UPDATE update_checked_rows SET v=DEFAULT WHERE id=1 RETURNING v','23502')
    q('SELECT id,v FROM update_checked_rows ORDER BY id',[['1','10'],['2','20']])
    q('ALTER DOMAIN update_checked_default SET DEFAULT -1')
    q('UPDATE update_checked_rows SET v=DEFAULT WHERE id=99 RETURNING v',[],oids=[23],tag='UPDATE 0')
    error('UPDATE update_checked_rows SET v=DEFAULT WHERE true RETURNING v','23514')
    q('SELECT id,v FROM update_checked_rows ORDER BY id',[['1','10'],['2','20']])
    q('CREATE TEMP SEQUENCE update_default_calls')
    q("CREATE DOMAIN update_effect_default AS INT DEFAULT nextval('update_default_calls') CHECK(VALUE<>2)")
    q('CREATE TEMP TABLE update_effect_rows(id INT,v update_effect_default)')
    q('INSERT INTO update_effect_rows VALUES(1,10),(2,20)')
    error("SELECT currval('update_default_calls')",'55000')
    describe('UPDATE update_effect_rows SET v=DEFAULT WHERE id=1 RETURNING v')
    error("SELECT currval('update_default_calls')",'55000')
    q('UPDATE update_effect_rows SET v=DEFAULT WHERE false RETURNING v',[],oids=[23],tag='UPDATE 0')
    q('UPDATE update_effect_rows SET v=DEFAULT WHERE CAST(id AS INT)=99 RETURNING v',[],oids=[23],tag='UPDATE 0')
    error("SELECT currval('update_default_calls')",'55000')
    error('UPDATE update_effect_rows SET v=DEFAULT WHERE true RETURNING v','23514')
    q('SELECT id,v FROM update_effect_rows ORDER BY id',[['1','10'],['2','20']])
    q("SELECT currval('update_default_calls')",[['2']])
    q('UPDATE update_effect_rows SET v=DEFAULT WHERE true RETURNING v',[['3'],['4']],oids=[23],tag='UPDATE 2')
    q("SELECT currval('update_default_calls')",[['4']])
    q('SAVEPOINT before_update_effect')
    q('UPDATE update_effect_rows SET v=DEFAULT WHERE id=1 RETURNING v',[['5']])
    q('ROLLBACK TO SAVEPOINT before_update_effect');q('RELEASE SAVEPOINT before_update_effect')
    q('SELECT id,v FROM update_effect_rows ORDER BY id',[['1','3'],['2','4']])
    q("SELECT currval('update_default_calls')",[['5']])
    error('UPDATE update_effect_rows SET v=DEFAULT WHERE missing_update_default(id)=1','42883')
    q("SELECT currval('update_default_calls')",[['5']])
    error('UPDATE update_effect_rows SET v=DEFAULT RETURNING missing_update_default(id)','42883')
    q("SELECT currval('update_default_calls')",[['5']])
    q('CREATE SCHEMA "DefaultScope"')
    q('CREATE TEMP SEQUENCE update_namespace_calls')
    q('CREATE FUNCTION "DefaultScope"."Pick"() RETURNS INT AS $$ BEGIN PERFORM nextval(\'update_namespace_calls\'); RETURN 7; END; $$ LANGUAGE plpgsql')
    q('CREATE TEMP TABLE "DefaultRows"(id INT,"V" INT DEFAULT "DefaultScope"."Pick"())')
    q('INSERT INTO "DefaultRows" VALUES(1,10),(2,20)')
    describe('UPDATE "DefaultRows" SET "V"=DEFAULT WHERE false RETURNING "V"')
    error("SELECT currval('update_namespace_calls')",'55000')
    q('UPDATE "DefaultRows" AS "Alias" SET "V"=DEFAULT WHERE false RETURNING "Alias"."V"',[],oids=[23],tag='UPDATE 0')
    error("SELECT currval('update_namespace_calls')",'55000')
    q('UPDATE "DefaultRows" AS "Alias" SET "V"=DEFAULT WHERE true RETURNING "Alias"."V"',[['7'],['7']],oids=[23],tag='UPDATE 2')
    q("SELECT currval('update_namespace_calls')",[['2']])
    q('UPDATE "DefaultRows" SET "V"=DEFAULT RETURNING old."V",new."V"',[['7','7'],['7','7']],oids=[23,23],tag='UPDATE 2')
    q("SELECT currval('update_namespace_calls')",[['4']])
    q('CREATE TEMP TABLE update_default_rounding(id INT,v INT DEFAULT 1.9,n INT DEFAULT -1.5)')
    q('INSERT INTO update_default_rounding VALUES(1,10,20)')
    q('UPDATE update_default_rounding SET v=DEFAULT,n=DEFAULT RETURNING v,n',[['2','-2']],oids=[23,23],tag='UPDATE 1')
    q('CREATE FUNCTION "DefaultScope"."Arg"(p INT) RETURNS INT AS $$ BEGIN PERFORM nextval(\'update_namespace_calls\'); RETURN p; END; $$ LANGUAGE plpgsql')
    q('CREATE TEMP TABLE update_default_priority(v INT DEFAULT "DefaultScope"."Arg"(1/0))')
    q('INSERT INTO update_default_priority VALUES(10)')
    error('UPDATE update_default_priority SET v=DEFAULT WHERE false RETURNING v','22012')
    q("SELECT currval('update_namespace_calls')",[['4']])
    q('ALTER TABLE update_default_priority ALTER COLUMN v SET DEFAULT CASE WHEN false THEN "DefaultScope"."Arg"(1/0) ELSE 1 END')
    q('UPDATE update_default_priority SET v=DEFAULT RETURNING v',[['1']],oids=[23],tag='UPDATE 1')
    q("SELECT currval('update_namespace_calls')",[['4']])
    q('CREATE TABLE update_default_mv_source(id INT)')
    q('INSERT INTO update_default_mv_source VALUES(1)')
    q('CREATE MATERIALIZED VIEW update_default_mv AS SELECT id FROM update_default_mv_source')
    error('UPDATE update_default_mv SET id=DEFAULT WHERE false RETURNING id','42809')
    q('CREATE MATERIALIZED VIEW update_default_unfilled AS SELECT id FROM update_default_mv_source WITH NO DATA')
    error('UPDATE update_default_unfilled SET id=DEFAULT WHERE false RETURNING id','42809')
    error('SELECT id FROM update_default_unfilled','55000')
    q('ROLLBACK')
    assert not failures,failures
    print('[UPDATE DOMAIN DEFAULT '+('STRICT18' if reference else 'PROTOCOL')+'] passed',flush=True)
finally:
    if server:r.stop_ours(server)
    else:c.simple_query(sock,'ROLLBACK');sock.close()
