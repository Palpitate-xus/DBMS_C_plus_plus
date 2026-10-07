"""FETCH signed descriptors fail at real count demand, never at grammar/type discovery."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('signed_fetch_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client();reference='--reference18' in sys.argv;server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password);runner.verify_reference_version(client,sock)
    else:server=runner.start_ours(client);sock=server['sock']
    failures=[];controls=0;schema='signed_fetch_'+uuid.uuid4().hex[:14];table='"'+schema+'".rows'
    def check(sql,messages,rows=None,types=None,state=None):
        nonlocal controls
        controls+=1;actual=runner.decode_wire_result(messages,include_types=True)
        print('SIGNED_FETCH',sql,actual,flush=True)
        if actual[1]!=state or rows is not None and actual[0]!=rows or types is not None and actual[5]!=types:
            failures.append((sql,actual,rows,types,state))
        if state and (actual[0] or actual[4] is not None):failures.append(('error published partial rows/tag',sql,actual))
        if types is not None and actual[1] is None:
            fields=client.row_description_fields(messages) if any(kind==b'T' for kind,_ in messages) else []
            if len(fields)!=len(types):failures.append(('missing static metadata',sql,fields,types))
            for field,oid in zip(fields,types):
                if field[3:]!=(oid,{23:4,20:8,25:-1}[oid],-1,0):failures.append(('signed descriptor metadata',sql,field,oid))
        return actual
    def query(sql,**expected):return check(sql,client.simple_query(sock,sql),**expected)
    def error(sql,state):
        if reference:query('SAVEPOINT signed_error')
        query(sql,state=state)
        if reference:query('ROLLBACK TO signed_error');query('RELEASE signed_error')
    def extended(sql):
        if reference:query('SAVEPOINT signed_extended')
        sock.sendall(client.typed(b'P',b'\0'+sql.encode()+b'\0'+struct.pack('!H',0))+
                     client.typed(b'D',b'S\0')+client.typed(b'S'))
        parsed=client.read_until_ready(sock);check('PARSE DESCRIBE '+sql,parsed,types=[23])
        if not any(kind==b'1' for kind,_ in parsed):failures.append(('negative count rejected at grammar',sql,parsed))
        sock.sendall(client.typed(b'B',b'\0\0'+struct.pack('!HHH',0,0,0))+
                     client.typed(b'E',b'\0'+struct.pack('!I',0))+client.typed(b'S'))
        check('BIND EXECUTE '+sql,client.read_until_ready(sock),state='2201W')
        if reference:query('ROLLBACK TO signed_extended');query('RELEASE signed_extended')
    try:
        if reference:query('BEGIN')
        query('CREATE SCHEMA "'+schema+'"');query('CREATE TABLE '+table+'(id INT)')
        for populated in (False,True):
            if populated:query('INSERT INTO '+table+' VALUES(1),(2),(NULL)')
            for mode in ['ONLY','WITH TIES']:
                for count in ['-1','-2','-9223372036854775808']:
                    body='SELECT id FROM '+table+' ORDER BY id FETCH FIRST '+count+' ROWS '+mode
                    error(body,'2201W');error('SELECT('+body+')','2201W')
                    error('SELECT id FROM '+table+' WHERE FALSE ORDER BY id FETCH FIRST '+count+' ROWS '+mode,'2201W')
                    query('SELECT('+body+') WHERE FALSE',rows=[],types=[23])
                    query('SELECT('+body+') LIMIT 0',rows=[],types=[23])
                    query('SELECT CASE WHEN FALSE THEN('+body+') ELSE 1 END',rows=[['1']],types=[23])
                for count in ['0','-0','+0','+1','9223372036854775807']:
                    rows=[] if count in ['0','-0','+0'] or not populated else [['1']] if count=='+1' else [['1'],['2'],[None]]
                    query('SELECT id FROM '+table+' ORDER BY id FETCH FIRST '+count+' ROWS '+mode,rows=rows,types=[23])
                error('SELECT(SELECT missing FROM '+table+' ORDER BY id FETCH FIRST -1 ROWS '+mode+')','42703')
                error('SELECT(SELECT id FROM "'+schema+'".missing ORDER BY id FETCH FIRST -1 ROWS '+mode+')','42P01')
                error("SELECT(SELECT CAST('bad' AS INTEGER) FROM "+table+' ORDER BY id FETCH FIRST -1 ROWS '+mode+')','22P02')
                extended('SELECT id FROM '+table+' ORDER BY id FETCH FIRST -1 ROWS '+mode)
            # Actual top-level grammar provenance, not a sign-bearing text
            # fragment, quoted range name, comment or child clause.
            first=[['1']] if populated else []
            query('SELECT id FROM '+table+' /* FETCH FIRST -1 ROW ONLY */ ORDER BY id FETCH /* real owner */ FIRST + 1 ROW ONLY',rows=first,types=[23])
            query('SELECT "FETCH".id FROM '+table+' AS "FETCH" ORDER BY "FETCH".id FETCH NEXT +1 ROW ONLY',rows=first,types=[23])
            query('SELECT id FROM '+table+' ORDER BY id FETCH FIRST - 0 ROWS ONLY',rows=[],types=[23])
            query("SELECT 'FETCH FIRST +1 ROW ONLY' AS harmless",rows=[['FETCH FIRST +1 ROW ONLY']],types=[25])
            provider='SELECT unnest(ARRAY[1,2]) FETCH FIRST -1 ROW ONLY'
            error(provider,'2201W');error('SELECT('+provider+')','2201W')
            query('SELECT('+provider+') WHERE FALSE',rows=[],types=[23])
            query('SELECT CASE WHEN FALSE THEN('+provider+') ELSE 1 END',rows=[['1']],types=[23])
            error('SELECT(SELECT id FROM '+table+' FETCH FIRST -1 ROW WITH TIES)','42601')
            error('SELECT(SELECT id FROM '+table+' FETCH FIRST -1 ROW WITH TIES) WHERE FALSE LIMIT 0','42601')
        sequence='"'+schema+'".effects';writer='signed_writer_'+uuid.uuid4().hex[:12]
        query('CREATE SEQUENCE '+sequence)
        if reference:query('SET LOCAL search_path TO "'+schema+'",pg_catalog')
        query('CREATE FUNCTION '+writer+'(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN PERFORM nextval(\''+schema+'.effects\'); RETURN p; END$$')
        query("SELECT nextval('"+schema+".effects')",rows=[['1']],types=[20])
        error('SELECT unnest(ARRAY['+writer+'(1),'+writer+'(2)]) FETCH FIRST -1 ROW ONLY','2201W')
        query("SELECT currval('"+schema+".effects')",rows=[['1']],types=[20])
        for mode in ['ONLY','WITH TIES']:
            body='SELECT '+writer+'(id) FROM '+table+' WHERE '+writer+'(id)>0 ORDER BY '+writer+'(id) FETCH FIRST -1 ROW '+mode
            error(body,'2201W');error('SELECT('+body+')','2201W')
            query('SELECT('+body+') LIMIT 0',rows=[],types=[23])
            query('SELECT('+body+') WHERE FALSE',rows=[],types=[23])
            query("SELECT currval('"+schema+".effects')",rows=[['1']],types=[20])
        query('SELECT('+ 'SELECT '+writer+'(id) FROM '+table+' ORDER BY id FETCH FIRST +1 ROW ONLY'+')',rows=[['1']],types=[23])
        query("SELECT currval('"+schema+".effects')",rows=[['2']],types=[20])
        print('SIGNED_FETCH_COMPLETE',controls,'failures',len(failures),failures,flush=True)
        assert not failures,failures
    finally:
        if reference:
            try:client.simple_query(sock,'ROLLBACK')
            finally:sock.close()
        else:runner.stop_ours(server)


if __name__=='__main__':main()
