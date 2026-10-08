"""Real AST commutation keeps BIT source, operator, NULL and effects owners."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('bit_reverse_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv;server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server['sock']
    schema='bit_reverse_'+uuid.uuid4().hex[:12];table='"'+schema+'".rows'
    values=['01','001','',None,'0001','1','0','00'];failures=[];controls=0

    def query(sql,rows=None,state=None,oid=None):
        nonlocal controls
        controls+=1;actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print('BIT_REVERSE',sql,actual,flush=True)
        if actual[1]!=state or rows is not None and actual[0]!=rows or oid is not None and actual[5]!=[oid]:
            failures.append((sql,actual,rows,state,oid))

    def error(sql,state):
        query('SAVEPOINT bit_reverse_bad');query(sql,[],state)
        query('ROLLBACK TO SAVEPOINT bit_reverse_bad');query('RELEASE SAVEPOINT bit_reverse_bad')

    def match(op,lhs,rhs):
        if lhs is None or rhs is None:return False
        return {'=':lhs==rhs,'<>':lhs!=rhs,'!=':lhs!=rhs,'<':lhs<rhs,'>':lhs>rhs,'<=':lhs<=rhs,'>=':lhs>=rhs}[op]

    try:
        query('BEGIN');query('CREATE SCHEMA "'+schema+'"')
        query('CREATE TABLE '+table+'(id INTEGER PRIMARY KEY,v VARBIT,u VARBIT,p TEXT)')
        query('CREATE INDEX bit_reverse_v ON '+table+'(v)')
        inputs=[("'b01'",'01'),("'B001'",'001'),("'b'",''),("'X'",''),("'x1'",'0001'),
                ("'X0aF'",'000010101111'),("'01'",'01'),("''",''),("B'01'",'01'),("X'1'",'0001'),
                ("B''",''),("X''",''),('NULL',None)]
        for populated in (False,True):
            if populated:
                for i,value in enumerate(values):
                    literal='NULL' if value is None else "B'"+value+"'"
                    query('INSERT INTO '+table+' VALUES('+str(i+1)+','+literal+','+literal+",'b01')")
            for column in ('v','u'):
                for literal,bits in inputs:
                    for op in ('=','<>','!=','<','>','<=','>='):
                        for reverse in (False,True):
                            predicate=literal+op+column if reverse else column+op+literal
                            expected=[[str(i+1)] for i,value in enumerate(values) if match(op,bits,value) if reverse] if reverse else [[str(i+1)] for i,value in enumerate(values) if match(op,value,bits)]
                            query('SELECT id FROM '+table+' WHERE '+predicate+' ORDER BY id',expected if populated else [],oid=23)
                for bad in ("'xg'","'b02'","'b 01'","'x 1'","'B''01'''","'NULL'"):
                    error('SELECT id FROM '+table+' WHERE '+bad+'='+column+' ORDER BY id','22P02')
            error('SELECT id FROM '+table+" WHERE '01'::text=v",'42883')
            query('SELECT id FROM '+table+" WHERE 'b01'=p ORDER BY id",[[str(i+1)] for i in range(len(values))] if populated else [],oid=23)
        quoted='"'+schema+'".quoted'
        query('CREATE TABLE '+quoted+'(k BIT(4) PRIMARY KEY,id INTEGER,"V Space" VARBIT,"Q""Bit" BIT(4))')
        query('INSERT INTO '+quoted+" VALUES(B'0001',1,B'01',B'0001'),(B'0100',2,NULL,NULL)")
        for predicate,expected in [("'x1'=r.k",[['1']]),("B'0001'=r.k",[['1']]),("'b01'=r.k",[]),
                                   ("'b01'=r.\"V Space\"",[['1']]),("X'1'=r.\"Q\"\"Bit\"",[['1']])]:
            query('SELECT r.id FROM '+quoted+' r WHERE '+predicate+' ORDER BY r.id',expected,oid=23)
        for suffix in (' LIMIT 0',' AND FALSE',' OR id=1'):
            error('SELECT id FROM '+table+" WHERE 'xg'=v"+suffix,'22P02')
        query('SELECT id FROM '+table+" WHERE 'b01'=v OR 'x1'=v ORDER BY id",[['1'],['5']],oid=23)
        sequence='"'+schema+'".effects';writer='bit_reverse_writer_'+uuid.uuid4().hex[:12]
        query('CREATE SEQUENCE '+sequence)
        if reference:query('SET LOCAL search_path TO "'+schema+'",pg_catalog')
        query('CREATE FUNCTION '+writer+'(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN PERFORM nextval(\''+
              schema+'.effects\'); RETURN p; END$$')
        query("SELECT nextval('"+schema+".effects')",[['1']],oid=20)
        error('SELECT '+writer+'(id) FROM '+table+" WHERE 'xg'=v",'22P02')
        query("SELECT currval('"+schema+".effects')",[['1']],oid=20)
        query('SELECT '+writer+'(id) FROM '+table+" WHERE 'b01'=v LIMIT 1",[['1']],oid=23)
        query("SELECT currval('"+schema+".effects')",[['2']],oid=20)
        query('SELECT '+writer+'(id) FROM '+table+' WHERE NULL=v LIMIT 1',[],oid=23)
        query("SELECT currval('"+schema+".effects')",[['2']],oid=20)
        print('BIT_REVERSE_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        try:client.simple_query(sock,'ROLLBACK')
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
