"""Real prepared SQL children retain FETCH peer/demand/type/error ownership."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('scalar_fetch_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv;server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server['sock']
    failures=[];controls=0;schema='scalar_fetch_'+uuid.uuid4().hex[:15];table='"'+schema+'".rows'
    def check(sql,messages,rows=None,types=None,state=None):
        nonlocal controls
        controls+=1;actual=runner.decode_wire_result(messages,include_types=True)
        print('SCALAR_FETCH',sql,actual,flush=True)
        if actual[1]!=state or rows is not None and actual[0]!=rows or types is not None and actual[5]!=types:
            failures.append((sql,actual,rows,types,state))
        if state and (actual[0] or actual[4] is not None):failures.append(('partial error publication',sql,actual))
        if types is not None and state is None and actual[1] is None:
            fields=client.row_description_fields(messages)
            if len(fields)!=len(types):failures.append(('metadata field count',sql,fields,types))
            for field,oid in zip(fields,types):
                if field[3:]!=(oid,{16:1,20:8,23:4,25:-1}[oid],-1,0):failures.append(('OID/width/typmod/format',sql,field,oid))
        return actual
    def query(sql,**expected):return check(sql,client.simple_query(sock,sql),**expected)
    def error(sql,state):
        if reference:query('SAVEPOINT fetch_error')
        query(sql,state=state)
        if reference:query('ROLLBACK TO fetch_error');query('RELEASE fetch_error')
    def extended(sql,rows,types):
        # Actual Describe must publish static metadata without running child
        # sort keys, projection, qualification or a volatile target.
        sock.sendall(client.typed(b'P',b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
                     client.typed(b'D',b'S\0')+client.typed(b'S'))
        metadata=client.read_until_ready(sock);check('PARSE DESCRIBE '+sql,metadata,types=types)
        if not any(kind==b'1' for kind,_ in metadata):failures.append(('missing actual ParseComplete',sql,metadata))
        for field in client.row_description_fields(metadata) if any(kind==b'T' for kind,_ in metadata) else []:
            if field[1] or field[2]:failures.append(('scalar child borrowed physical origin',sql,field))
        sock.sendall(client.typed(b'B',b'\0\0'+struct.pack('!HHH',0,0,0))+
                     client.typed(b'D',b'P\0')+client.typed(b'E',b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        executed=client.read_until_ready(sock);check('BIND EXECUTE '+sql,executed,rows=rows,types=types)
        if not any(kind==b'2' for kind,_ in executed):failures.append(('missing actual BindComplete',sql,executed))
    try:
        if reference:query('BEGIN')
        query('CREATE SCHEMA "'+schema+'"')
        query('CREATE TABLE '+table+'(id INT,k INT,p TEXT,b BIGINT)')
        scalar=lambda body:'SELECT('+body+') AS value'
        query(scalar('SELECT id FROM '+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES'),rows=[[None]],types=[23])
        error(scalar('SELECT id FROM '+table+' FETCH FIRST 1 ROW WITH TIES'),'42601')
        query('INSERT INTO '+table+" VALUES(1,1,'',9007199254740993),(2,1,'NULL',9007199254740992),(3,2,NULL,NULL),(4,NULL,'spaces here',0),(5,NULL,'last',1)")
        cases=[
            ('id DESC','FETCH FIRST 1 ROW WITH TIES',[['5']]),
            ('k,id','FETCH FIRST 1 ROW WITH TIES',[['1']]),
            ('k','OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES',[['2']]),
            ('k NULLS FIRST','OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES',[['5']]),
            ('id','FETCH FIRST 0 ROWS WITH TIES',[[None]]),
            ('id','OFFSET 99 ROWS FETCH FIRST 1 ROW WITH TIES',[[None]]),
            ('id','FETCH FIRST +1 ROW WITH TIES',[['1']]),
            ('id','FETCH FIRST -0 ROWS WITH TIES',[[None]]),
            ('id','FETCH FIRST ROW WITH TIES',[['1']]),
        ]
        for order,tail,rows in cases:query(scalar('SELECT id FROM '+table+' ORDER BY '+order+' '+tail),rows=rows,types=[23])
        for order in ['k','k NULLS FIRST','k % 2 DESC NULLS LAST']:
            error(scalar('SELECT id FROM '+table+' ORDER BY '+order+' FETCH FIRST 1 ROW WITH TIES'),'21000')
        for order in ['k DESC NULLS LAST','k % 2']:
            query(scalar('SELECT id FROM '+table+' ORDER BY '+order+' FETCH FIRST 1 ROW WITH TIES'),rows=[['3']],types=[23])
        for where,rows in [('id=1',[['']]),('id=2',[['NULL']]),('id=3',[[None]]),('FALSE',[[None]])]:
            query(scalar('SELECT p FROM '+table+' WHERE '+where+' ORDER BY id FETCH FIRST 1 ROW WITH TIES'),rows=rows,types=[25])
        query(scalar('SELECT b FROM '+table+' ORDER BY b DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES'),rows=[['9007199254740993']],types=[20])
        for predicate in ['',' WHERE FALSE',' LIMIT 0']:
            error(scalar('SELECT id FROM '+table+' FETCH FIRST 1 ROW WITH TIES')+predicate,'42601')
        for body,state in [('SELECT missing FROM '+table+' FETCH FIRST 1 ROW WITH TIES','42601'),
            ('SELECT id FROM "'+schema+'".missing FETCH FIRST 1 ROW WITH TIES','42601'),
            ('SELECT id FROM '+table+' WHERE missing=1 FETCH FIRST 1 ROW WITH TIES','42601'),
            ("SELECT CAST('bad' AS INTEGER) FROM "+table+' FETCH FIRST 1 ROW WITH TIES','42601'),
            ('SELECT missing FROM '+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES','42703'),
            ('SELECT id FROM "'+schema+'".missing ORDER BY id FETCH FIRST 1 ROW WITH TIES','42P01'),
            ("SELECT CAST('bad' AS INTEGER) FROM "+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES','22P02'),
            ('SELECT id FROM '+table+' ORDER BY 2 FETCH FIRST 1 ROW WITH TIES','42P10'),
            ('SELECT id,id FROM '+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES','42601'),
            ('SELECT id FROM '+table+' ORDER BY id FETCH FIRST -1 ROW WITH TIES','2201W')]:error(scalar(body),state)
        query('SELECT EXISTS(SELECT id FROM '+table+' ORDER BY k FETCH FIRST 1 ROW WITH TIES)',rows=[['t']],types=[16])
        query('SELECT EXISTS(SELECT id FROM '+table+' ORDER BY k FETCH FIRST 0 ROWS WITH TIES)',rows=[['f']],types=[16])
        # Main-query peer definition must match the scalar child's real sort:
        # all keys, NULL equality, direction, exact typed keys and OFFSET.
        for order,tail,rows in [('k','FETCH FIRST 1 ROW WITH TIES',[['1'],['2']]),
            ('k NULLS FIRST','FETCH FIRST 1 ROW WITH TIES',[['4'],['5']]),
            ('k,id','FETCH FIRST 1 ROW WITH TIES',[['1']]),
            ('k','OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES',[['2']]),
            ('b DESC NULLS LAST','FETCH FIRST 1 ROW WITH TIES',[['1']]),
            ('k % 2 DESC NULLS LAST','FETCH FIRST 1 ROW WITH TIES',[['1'],['2']]),
            ('id DESC','FETCH FIRST 0 ROWS WITH TIES',[]),
            ('b DESC NULLS LAST','OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES',[['2']])]:
            query('SELECT id FROM '+table+' ORDER BY '+order+' '+tail,rows=rows,types=[23])
        for body,rows,types in [('SELECT id FROM '+table+' ORDER BY id DESC FETCH FIRST 1 ROW WITH TIES',[['5']],[23]),
            ('SELECT p FROM '+table+' WHERE id=3 ORDER BY id FETCH FIRST 1 ROW WITH TIES',[[None]],[25]),
            ('SELECT b FROM '+table+' ORDER BY b DESC NULLS LAST FETCH FIRST 1 ROW WITH TIES',[['9007199254740993']],[20])]:extended(scalar(body),rows,types)
        sequence='"'+schema+'".effects';writer='fetch_writer_'+uuid.uuid4().hex[:12]
        query('CREATE SEQUENCE '+sequence)
        if reference:query('SET LOCAL search_path TO "'+schema+'",pg_catalog')
        query('CREATE FUNCTION '+writer+'(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN PERFORM nextval(\''+schema+'.effects\'); RETURN p; END$$')
        query("SELECT nextval('"+schema+".effects')",rows=[['1']],types=[20])
        calls=lambda:int(query("SELECT currval('"+schema+".effects')",types=[20])[0][0][0])
        for label,sql,rows,expected in [
            ('real target lookahead is retained',scalar('SELECT '+writer+'(id) FROM '+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES'),[['1']],2),
            ('zero child demand',scalar('SELECT '+writer+'(id) FROM '+table+' ORDER BY '+writer+'(id) FETCH FIRST 0 ROWS WITH TIES'),[[None]],0),
            ('outer zero demand',scalar('SELECT '+writer+'(id) FROM '+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES')+' LIMIT 0',[],0),
            ('two genuine child occurrences','SELECT(SELECT '+writer+'(id) FROM '+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES),(SELECT '+writer+'(id) FROM '+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES)',[['1','1']],4),
            ('sort target alias is one value',scalar('SELECT '+writer+'(id) AS v FROM '+table+' ORDER BY v FETCH FIRST 1 ROW WITH TIES'),[['1']],5),
            ('OFFSET retains target demand',scalar('SELECT '+writer+'(id) FROM '+table+' ORDER BY id OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES'),[['2']],3),
        ]:
            before=calls();query(sql,rows=rows,types=[23]*len(rows[0]) if rows else [23]);after=calls()
            if after-before!=expected:failures.append(('actual occurrence demand',label,after-before,expected,sql))
        before=calls();error(scalar('SELECT '+writer+'(id) FROM '+table+' FETCH FIRST 1 ROW WITH TIES'),'42601')
        if calls()!=before:failures.append(('invalid preparation executed a routine',before,calls()))
        before=calls();error(scalar('SELECT '+writer+'(id) FROM '+table+' ORDER BY k FETCH FIRST 1 ROW WITH TIES'),'21000')
        if calls()-before!=2:failures.append(('scalar peer cardinality occurrence count',calls()-before,2))
        before=calls();sql=scalar('SELECT '+writer+'(id) FROM '+table+' ORDER BY id FETCH FIRST 1 ROW WITH TIES')
        metadata_sql='EXPLAIN '+sql;result=query(metadata_sql)
        if not result[0] or calls()!=before:failures.append(('plain EXPLAIN ran a child',result,before,calls()))
        extended('SELECT(SELECT b FROM '+table+' WHERE false) AS value',[[None]],[20])
        extended("SELECT(SELECT 'typed text') AS value",[['typed text']],[25])
        extended(sql,[['1']],[23])
        if calls()-before!=2:failures.append(('Parse/Describe/Bind reexecuted child',calls()-before,2))
        print('SCALAR_FETCH_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:
            try:client.simple_query(sock,'ROLLBACK')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
