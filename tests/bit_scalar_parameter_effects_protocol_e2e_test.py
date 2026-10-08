"""BIT scalar Parse/Bind/Describe are metadata-only; Execute effects once."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('bit_scalar_effect_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv;server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server['sock']
    schema='bit_param_effect_'+uuid.uuid4().hex[:12];table='"'+schema+'".rows'
    writer='bit_param_writer_'+uuid.uuid4().hex[:12];failures=[];controls=0

    def query(sql,rows=None):
        nonlocal controls
        controls+=1;actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print('BIT_PARAM_EFFECT',sql,actual,flush=True)
        if actual[1] is not None or rows is not None and actual[0]!=rows:failures.append((sql,actual,rows))

    def phase(messages,state=None,rows=None):
        nonlocal controls
        controls+=1;actual=runner.decode_wire_result(messages,include_types=True)
        print('BIT_PARAM_EFFECT_PHASE',actual,flush=True)
        if actual[1]!=state or rows is not None and actual[0]!=rows:failures.append((state,rows,actual))

    try:
        query('CREATE SCHEMA "'+schema+'"');query('CREATE TABLE '+table+'(id INTEGER PRIMARY KEY,v VARBIT)')
        query('INSERT INTO '+table+" VALUES(1,B'01'),(2,B'01'),(3,NULL)")
        query('CREATE SEQUENCE "'+schema+'".effects')
        if reference:query('SET search_path TO "'+schema+'",pg_catalog')
        query('CREATE FUNCTION '+writer+'(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN PERFORM nextval(\''+
              schema+'.effects\'); RETURN p; END$$')
        query("SELECT nextval('"+schema+".effects')",[['1']])
        query('BEGIN')
        for oid,raw,expected in ((1562,'b01',[['1']]),(1560,None,[]),(25,None,None),(23,'b01',None)):
            statement=('param_effect_'+uuid.uuid4().hex[:12]).encode();portal=statement+b'_p'
            sql='SELECT '+writer+'(id) FROM '+table+' WHERE v=$1 LIMIT 1'
            if expected is None:query('SAVEPOINT scalar_param_type')
            sock.sendall(client.typed(b'P',statement+b'\0'+sql.encode()+b'\0'+struct.pack('!HI',1,oid))+
                         client.typed(b'D',b'S'+statement+b'\0')+client.typed(b'S'))
            messages=client.read_until_ready(sock);phase(messages,'42883' if expected is None else None,[])
            if expected is None:
                query('ROLLBACK TO SAVEPOINT scalar_param_type');query('RELEASE SAVEPOINT scalar_param_type')
            query("SELECT currval('"+schema+".effects')",[['1' if oid==1562 else '2']])
            if expected is None:continue
            value=struct.pack('!i',-1) if raw is None else struct.pack('!i',len(raw))+raw.encode()
            bind=portal+b'\0'+statement+b'\0'+struct.pack('!HH',0,1)+value+struct.pack('!H',0)
            sock.sendall(client.typed(b'B',bind)+client.typed(b'D',b'P'+portal+b'\0')+client.typed(b'S'))
            phase(client.read_until_ready(sock),None,[])
            query("SELECT currval('"+schema+".effects')",[['1' if oid==1562 else '2']])
            sock.sendall(client.typed(b'E',portal+b'\0'+struct.pack('!I',0))+client.typed(b'S'))
            phase(client.read_until_ready(sock),None,expected)
            query("SELECT currval('"+schema+".effects')",[['2']])
            sock.sendall(client.typed(b'C',b'P'+portal+b'\0')+client.typed(b'C',b'S'+statement+b'\0')+client.typed(b'S'))
            phase(client.read_until_ready(sock),None,[])
        print('BIT_PARAM_EFFECT_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        try:
            client.simple_query(sock,'ROLLBACK');client.simple_query(sock,'DROP SCHEMA "'+schema+'" CASCADE')
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
