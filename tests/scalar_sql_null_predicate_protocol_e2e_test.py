"""Actual SQL NULL operands are not text, empty data or native API values."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('scalar_null_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv;server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server['sock']
    schema='scalar_null_'+uuid.uuid4().hex[:12];table='"'+schema+'".rows';failures=[];controls=0

    def query(sql,rows=None,state=None,oid=None):
        nonlocal controls
        controls+=1;actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print('SCALAR_SQL_NULL',sql,actual,flush=True)
        if actual[1]!=state or rows is not None and actual[0]!=rows or oid is not None and actual[5]!=[oid]:
            failures.append((sql,actual,rows,state,oid))

    def error(sql,state):
        query('SAVEPOINT scalar_null_error');query(sql,[],state)
        query('ROLLBACK TO SAVEPOINT scalar_null_error');query('RELEASE SAVEPOINT scalar_null_error')

    try:
        query('BEGIN');query('CREATE SCHEMA "'+schema+'"')
        query('CREATE TABLE '+table+'(id INTEGER PRIMARY KEY,i INTEGER,n BIGINT,v VARBIT,t TEXT,a TEXT)')
        query('CREATE INDEX scalar_null_v ON '+table+'(v)');query('CREATE INDEX scalar_null_a ON '+table+'(a)')
        for populated in (False,True):
            if populated:query('INSERT INTO '+table+" VALUES(1,1,9007199254740993,B'01','NULL','NULL'),(2,2,2,B'','',''),(3,NULL,NULL,NULL,NULL,NULL)")
            for column in ('id','i','n','v','t','a'):
                for op in ('=','<>','!=','<','>','<=','>='):
                    for predicate in (column+op+'NULL','NULL'+op+column):
                        query('SELECT id FROM '+table+' WHERE '+predicate+' ORDER BY id',[],oid=23)
                        query('SELECT id FROM '+table+' WHERE ('+predicate+') OR id=1 ORDER BY id',[['1']] if populated else [],oid=23)
            query('SELECT id FROM '+table+" WHERE a='NULL' ORDER BY id",[['1']] if populated else [],oid=23)
            query('SELECT id FROM '+table+" WHERE a='' ORDER BY id",[['2']] if populated else [],oid=23)
            query('SELECT id FROM '+table+' WHERE a IS NULL ORDER BY id',[['3']] if populated else [],oid=23)
            error('SELECT id FROM '+table+" WHERE v=NULL AND v='xg' LIMIT 0",'22P02')
            for predicate in ('NOT(v=NULL)','CASE WHEN v=NULL THEN TRUE ELSE FALSE END',
                              'COALESCE(v=NULL,FALSE)','(v=NULL) AND (id=1 OR id=2)'):
                query('SELECT id FROM '+table+' WHERE '+predicate+' ORDER BY id',[],oid=23)
            query('SELECT id FROM '+table+' WHERE (v=NULL) OR id=1 ORDER BY id',[['1']] if populated else [],oid=23)
            error('SELECT id FROM '+table+' WHERE v=NULL AND scalar_null_missing(id)>0 LIMIT 0','42883')
            for predicate in ('missing=NULL','NULL=missing','(missing=NULL) OR id=1'):
                for limit in ('',' LIMIT 0'):
                    error('SELECT id FROM '+table+' WHERE '+predicate+limit,'42703')
        query('SELECT count(*) FROM '+table+' WHERE v=NULL',[['0']],oid=20)
        query('SELECT id FROM '+table+' WHERE a=NULL OR a=\'NULL\' ORDER BY id',[['1']],oid=23)
        query('SELECT id FROM '+table+' WHERE a=\'NULL\' AND v=NULL ORDER BY id',[],oid=23)
        sequence='"'+schema+'".effects';writer='scalar_null_writer_'+uuid.uuid4().hex[:12]
        query('CREATE SEQUENCE '+sequence)
        if reference:query('SET LOCAL search_path TO "'+schema+'",pg_catalog')
        query('CREATE FUNCTION '+writer+'(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN PERFORM nextval(\''+
              schema+'.effects\'); RETURN p; END$$')
        query("SELECT nextval('"+schema+".effects')",[['1']],oid=20)
        for predicate in ('v<>NULL','a=NULL','NULL<a','v=NULL AND '+writer+'(id)>0',writer+'(id)>0 AND v=NULL'):
            query('SELECT '+writer+'(id) FROM '+table+' WHERE '+predicate+' LIMIT 1',[],oid=23)
            query("SELECT currval('"+schema+".effects')",[['1']],oid=20)
        query('SELECT '+writer+'(id) FROM '+table+' WHERE (v=NULL AND '+writer+'(id)>0) OR id=1 LIMIT 1',[['1']],oid=23)
        query("SELECT currval('"+schema+".effects')",[['2']],oid=20)
        query('SELECT id FROM '+table+' WHERE NOT(v=NULL OR '+writer+'(id)>0) ORDER BY id',[],oid=23)
        query("SELECT currval('"+schema+".effects')",[['2']],oid=20)
        print('SCALAR_SQL_NULL_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        try:client.simple_query(sock,'ROLLBACK')
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
