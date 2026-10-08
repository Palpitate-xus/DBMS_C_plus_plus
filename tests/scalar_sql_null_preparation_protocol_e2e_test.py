"""Real Parse/Bind/Describe/Execute demand for literal-NULL qualification."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('null_preparation_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv;server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server['sock']
    schema='null_prepare_'+uuid.uuid4().hex[:12];table='"'+schema+'".rows'
    writer='null_prepare_writer_'+uuid.uuid4().hex[:12];controls=0;failures=[]

    def check(ok,label,actual):
        nonlocal controls
        controls+=1;print('NULL_PREPARATION',label,actual,flush=True)
        if not ok:failures.append((label,actual))

    def query(sql,rows=None,oid=None):
        actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        check(actual[1] is None and (rows is None or actual[0]==rows) and (oid is None or actual[5]==[oid]),sql,actual)

    def effects():query("SELECT currval('"+schema+".effects')",[['1']],20)

    try:
        query('BEGIN');query('CREATE SCHEMA "'+schema+'"')
        query('CREATE TABLE '+table+'(id INTEGER PRIMARY KEY,v VARBIT)')
        query('CREATE SEQUENCE "'+schema+'".effects')
        if reference:query('SET LOCAL search_path TO "'+schema+'",pg_catalog')
        query('CREATE FUNCTION '+writer+'(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN PERFORM nextval(\''+
              schema+'.effects\'); RETURN p; END$$')
        query("SELECT nextval('"+schema+".effects')",[['1']],20)
        for populated in (False,True):
            if populated:query('INSERT INTO '+table+" VALUES(1,B'01'),(2,NULL),(3,B'')")
            for predicate in ('v=NULL','v=NULL AND '+writer+'($1)>0',writer+'($1)>0 AND v=NULL'):
                for value in ('1',None):
                    sql='SELECT '+writer+'($1) FROM '+table+' WHERE '+predicate
                    statement=('null_prepare_'+uuid.uuid4().hex[:12]).encode();portal=statement+b'_p'
                    sock.sendall(client.typed(b'P',statement+b'\0'+sql.encode()+b'\0'+struct.pack('!HI',1,23))+
                                 client.typed(b'D',b'S'+statement+b'\0')+client.typed(b'S'))
                    messages=client.read_until_ready(sock);decoded=runner.decode_wire_result(messages,include_types=True)
                    check(decoded[1] is None and any(kind==b'1' for kind,_ in messages) and not any(kind==b'D' for kind,_ in messages),
                          ('actual Parse/Describe statement',sql,value),decoded)
                    check([payload for kind,payload in messages if kind==b't']==[struct.pack('!HI',1,23)],
                          'original declared OID23 source, not a NULL-value inference',messages)
                    check(decoded[5]==[23],'original function result INTEGER descriptor',decoded)
                    effects()
                    raw=struct.pack('!i',-1) if value is None else struct.pack('!i',len(value))+value.encode()
                    sock.sendall(client.typed(b'B',portal+b'\0'+statement+b'\0'+struct.pack('!HH',0,1)+raw+struct.pack('!H',0))+
                                 client.typed(b'D',b'P'+portal+b'\0')+client.typed(b'S'))
                    messages=client.read_until_ready(sock);decoded=runner.decode_wire_result(messages,include_types=True)
                    check(decoded[1] is None and any(kind==b'2' for kind,_ in messages) and not any(kind==b'D' for kind,_ in messages) and decoded[5]==[23],
                          ('actual Bind/Describe portal',value),decoded)
                    effects()
                    sock.sendall(client.typed(b'E',portal+b'\0'+struct.pack('!I',0))+client.typed(b'S'))
                    messages=client.read_until_ready(sock);decoded=runner.decode_wire_result(messages,include_types=True)
                    check(decoded[1] is None and decoded[0]==[] and decoded[4]=='SELECT 0',
                          ('actual Execute zero demand',value),decoded)
                    effects()
                    sock.sendall(client.typed(b'C',b'P'+portal+b'\0')+client.typed(b'C',b'S'+statement+b'\0')+client.typed(b'S'))
                    messages=client.read_until_ready(sock)
                    check(not any(kind==b'E' for kind,_ in messages) and sum(kind==b'3' for kind,_ in messages)==2,
                          'actual Close owned statement and portal',messages)
        # NOT of an UNKNOWN AND an unknown runtime boolean can be TRUE.
        # This value role must not be folded into the positive WHERE branch.
        query('SELECT id FROM '+table+' WHERE NOT(v=NULL AND '+writer+'(id)>0) ORDER BY id',[],23)
        query("SELECT currval('"+schema+".effects')",[['4']],20)
        print('NULL_PREPARATION_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        try:client.simple_query(sock,'ROLLBACK')
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
