#!/usr/bin/env python3
"""Actual bound UPDATE FROM consumers, pure errors and same-owner effects."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('update_source_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference=sys.argv[1:]==['--reference18'];assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    failures=[]
    def query(sql,rows=None,oids=None,state=None,tag=None,ready=b'I',unordered=False):
        messages=client.simple_query(sock,sql);actual=runner.decode_wire_result(messages,include_types=True)
        statuses=[body for kind,body in messages if kind==b'Z']
        print('UPDATE_SOURCE',sql,actual,'READY',statuses,flush=True)
        good=actual[1]==state and statuses==[ready]
        if rows is not None:
            key=lambda row:tuple((cell is None,cell or '') for cell in row)
            good=good and (sorted(actual[0],key=key) if unordered else actual[0])==(sorted(rows,key=key) if unordered else rows)
        if oids is not None:good=good and actual[5]==oids
        if tag is not None:good=good and actual[4]==tag
        if not good:failures.append((sql,actual,statuses,rows,oids,state,tag,ready))
    seed="INSERT INTO update_source_target VALUES(1,'old1','a'::CHAR(3),decode('0041','hex'),'[0:1]={1,2}'::INT[]),(2,'old2','old'::CHAR(3),decode('ff','hex'),NULL),(3,'',''::CHAR(3),decode('','hex'),ARRAY[]::INT[]),(4,NULL,NULL,NULL,NULL);"
    read='SELECT id,v FROM update_source_target ORDER BY id;'
    originals=[['1','old1'],['2','old2'],['3',''],['4',None]]
    try:
        query('CREATE TABLE update_source_target(id INT PRIMARY KEY,v TEXT,c CHAR(3),b BYTEA,a INT[]);')
        query('CREATE INDEX update_source_v_idx ON update_source_target(v);')
        query('CREATE TABLE update_source_input(id INT PRIMARY KEY,v TEXT,c CHAR(3),b BYTEA,a INT[]);')
        query("INSERT INTO update_source_input VALUES(1,'new1','é'::CHAR(3),decode('00','hex'),'[2:3]={5,6}'::INT[]),(2,NULL,''::CHAR(3),decode('','hex'),NULL),(3,'NULL','x'::CHAR(3),decode('ff','hex'),ARRAY[]::INT[]),(4,'',NULL,NULL,NULL);")
        query('CREATE TABLE update_source_audit(id INT);')
        query('CREATE SEQUENCE update_source_effects;')
        query("CREATE FUNCTION update_source_fail(k INT) RETURNS TEXT VOLATILE LANGUAGE plpgsql AS $$ BEGIN INSERT INTO update_source_audit VALUES(k); PERFORM nextval('update_source_effects'); IF k=2 THEN RETURN (1/0)::TEXT; END IF; RETURN 'effect'; END; $$;")
        query("CREATE FUNCTION update_source_where(k INT) RETURNS BOOLEAN VOLATILE LANGUAGE plpgsql AS $$ BEGIN INSERT INTO update_source_audit VALUES(k+100); RETURN k<=2; END; $$;")
        query(seed)
        query('UPDATE update_source_target AS victim SET v=producer.v FROM update_source_input AS producer WHERE victim.id=producer.id RETURNING id;',rows=[],state='42702')
        query(read,rows=originals,oids=[23,25])
        # Reseed only after recording both baseline failures; never replace
        # the negative statement's no-mutation expectations with cleanup.
        query('DELETE FROM update_source_target;');query(seed)
        query('UPDATE update_source_target AS victim SET v=update_source_fail(victim.id) FROM update_source_input AS producer WHERE FALSE RETURNING abs(victim.id),victim.v,victim.c,encode(victim.b,\'hex\'),victim.a;',rows=[],oids=[23,25,1042,25,1007],tag='UPDATE 0')
        query('SELECT id FROM update_source_audit ORDER BY id;',rows=[],oids=[23])
        query('UPDATE update_source_target AS victim SET v=producer.v FROM update_source_input AS producer WHERE FALSE RETURNING missing_update_source_function(victim.id);',rows=[],state='42883')
        query('UPDATE update_source_target AS victim SET v=producer.v FROM update_source_input AS producer WHERE FALSE RETURNING 1/0;',rows=[],state='22012')
        query(read,rows=originals,oids=[23,25])
        query('UPDATE update_source_target AS victim SET v=update_source_fail(victim.id) FROM update_source_input AS producer WHERE victim.id=producer.id AND victim.id<=2 RETURNING victim.id;',rows=[],state='22012')
        query(read,rows=originals,oids=[23,25]);query('SELECT id FROM update_source_audit ORDER BY id;',rows=[],oids=[23])
        query("SELECT nextval('update_source_effects');",rows=[['3']],oids=[20])
        query("UPDATE update_source_target AS victim SET v=producer.v,c=producer.c,b=producer.b,a=producer.a FROM update_source_input AS producer WHERE victim.id=producer.id AND victim.id<=2 RETURNING abs(victim.id),victim.v,victim.c,encode(victim.b,'hex'),victim.a,old.v;",rows=[['1','new1','é  ','00','[2:3]={5,6}','old1'],['2',None,'   ','',None,'old2']],oids=[23,25,1042,25,1007,25],tag='UPDATE 2',unordered=True)
        query('DELETE FROM update_source_target;');query(seed)
        query('BEGIN;',ready=b'T');query('UPDATE update_source_target SET v=NULL WHERE id=3;',ready=b'T')
        query('INSERT INTO update_source_audit VALUES(99);',ready=b'T');query('SAVEPOINT source_parent;',ready=b'T')
        query('UPDATE update_source_target AS victim SET v=update_source_fail(victim.id) FROM update_source_input AS producer WHERE victim.id=producer.id AND victim.id<=2 RETURNING victim.id;',rows=[],state='22012',ready=b'E')
        query('ROLLBACK TO source_parent;',ready=b'T')
        prior=[['1','old1'],['2','old2'],['3',None],['4',None]]
        query(read,rows=prior,oids=[23,25],ready=b'T');query('SELECT id FROM update_source_audit ORDER BY id;',rows=[['99']],oids=[23],ready=b'T')
        query('RELEASE source_parent;',ready=b'T');query('COMMIT;');query('DELETE FROM update_source_audit;')
        query('UPDATE update_source_target AS victim SET v=producer.v FROM update_source_input AS producer WHERE victim.id=producer.id AND update_source_where(victim.id) RETURNING victim.id,victim.v;',rows=[['1','new1'],['2',None]],oids=[23,25],tag='UPDATE 2',unordered=True)
        query('SELECT id FROM update_source_audit ORDER BY id;',rows=[['101'],['102'],['103'],['104']],oids=[23])
        query('CREATE TABLE update_source_right(id INT,v TEXT);');query("INSERT INTO update_source_right VALUES(1,'right'),(99,'excluded');")
        query("UPDATE update_source_target AS victim SET v=COALESCE(nullable.v,'outer') FROM update_source_input AS producer LEFT JOIN update_source_right AS nullable ON producer.id=nullable.id WHERE victim.id=producer.id AND nullable.id IS NULL RETURNING victim.id,victim.v,nullable.id,nullable.v;",rows=[['2','outer',None,None],['3','outer',None,None],['4','outer',None,None]],oids=[23,25,23,25],tag='UPDATE 3',unordered=True)
        query(read,rows=[['1','new1'],['2','outer'],['3','outer'],['4','outer']],oids=[23,25])
        query('CREATE TABLE update_source_duplicate(id INT,v TEXT);');query("INSERT INTO update_source_duplicate VALUES(1,'dup'),(1,'dup');")
        query('UPDATE update_source_target AS victim SET v=producer.v FROM update_source_duplicate AS producer WHERE victim.id=producer.id RETURNING abs(victim.id),victim.v;',rows=[['1','dup']],oids=[23,25],tag='UPDATE 1')
        print('UPDATE_SOURCE_FAILURES',failures,flush=True);assert not failures,failures
    finally:
        try:
            for sql in ['ROLLBACK;','DROP FUNCTION IF EXISTS update_source_fail(INT);','DROP FUNCTION IF EXISTS update_source_where(INT);','DROP TABLE IF EXISTS update_source_target;','DROP TABLE IF EXISTS update_source_input;','DROP TABLE IF EXISTS update_source_audit;','DROP TABLE IF EXISTS update_source_right;','DROP TABLE IF EXISTS update_source_duplicate;','DROP SEQUENCE IF EXISTS update_source_effects;']:
                client.simple_query(sock,sql)
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
