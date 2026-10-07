"""Real integer literal-left predicates keep the physical column/index owner."""
import importlib.util
import json
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('integer_reverse_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv;server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    failures=[];controls=0;schema='reverse_integer_'+uuid.uuid4().hex[:14];table='"'+schema+'".bits'
    def query(sql,rows=None,oids=None):
        nonlocal controls
        controls+=1;actual=runner.decode_wire_result(client.simple_query(sock,sql),include_types=True)
        print('INTEGER_REVERSE',sql,actual,flush=True)
        if actual[1] or rows is not None and actual[0]!=rows or oids is not None and actual[5]!=oids:
            failures.append((sql,actual,rows,oids))
        return actual
    def execute(sql,expected):
        nonlocal controls
        controls+=1
        sock.sendall(client.typed(b'P',b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
                     client.typed(b'B',b'\0\0'+struct.pack('!HHH',0,0,0))+
                     client.typed(b'D',b'P\0')+client.typed(b'E',b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        messages=client.read_until_ready(sock);actual=runner.decode_wire_result(messages,include_types=True)
        print('INTEGER_REVERSE_EXTENDED',sql,actual,flush=True)
        if actual[1] or actual[0]!=expected or actual[5]!=[23] or actual[4]!='SELECT '+str(len(expected)):
            failures.append(('extended',sql,actual,expected))
        if not any(kind==b'1' for kind,_ in messages) or not any(kind==b'2' for kind,_ in messages):
            failures.append(('actual Parse/Bind ownership',sql,messages))
    try:
        if reference:query('BEGIN')
        query('CREATE SCHEMA "'+schema+'"')
        query('CREATE TABLE '+table+'(id INT PRIMARY KEY,i INT,b BIGINT,"q I" INT)')
        query('CREATE INDEX reverse_i ON '+table+'(i)')
        for populated in (False,True):
            if populated:query('INSERT INTO '+table+' VALUES(1,1,1,1),(2,2,2,2),(3,NULL,NULL,NULL)')
            for column in ['id','i','b','"q I"']:
                for op in ['=','<>','<','>','<=','>=']:
                    for reverse in (False,True):
                        predicate="'1'"+op+column if reverse else column+op+"'1'"
                        expected=[]
                        if populated:
                            for row in (1,2,3):
                                value=row if column=='id' or row!=3 else None
                                if value is None:continue
                                l,r=(1,value) if reverse else (value,1)
                                if {'=':l==r,'<>':l!=r,'<':l<r,'>':l>r,'<=':l<=r,'>=':l>=r}[op]:expected.append([str(row)])
                        query('SELECT id FROM '+table+' WHERE '+predicate+' ORDER BY id',expected,[23])
                execute('SELECT id FROM '+table+" WHERE '1'="+column+' ORDER BY id',[['1']] if populated else [])
                query('SELECT id FROM '+table+' WHERE NULL='+column+' ORDER BY id',[],[23])
        for predicate in ["'1'=id","id='1'","'1'=i","i='1'"]:
            actual=query('EXPLAIN (FORMAT JSON,ANALYZE TRUE,TIMING FALSE) SELECT id FROM '+table+' WHERE '+predicate,oids=[114])
            if actual[1]:continue
            document=json.loads('\n'.join(row[0] for row in actual[0]))
            if reference:
                rows=document[0]['Plan']['Actual Rows']
            else:
                rows=document['actualRows']
                if 'IndexScan' not in json.dumps(document):failures.append(('physical index owner changed',predicate,document))
            if rows!=1:failures.append(('executed EXPLAIN predicate',predicate,rows,document))
        query('SELECT "r I".id FROM '+table+' AS "r I" WHERE \'1\'="r I"."q I" ORDER BY "r I".id',[['1']],[23])
        print('INTEGER_REVERSE_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:
            try:client.simple_query(sock,'ROLLBACK')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
