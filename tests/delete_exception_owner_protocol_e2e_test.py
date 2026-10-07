#!/usr/bin/env python3
"""DELETE exceptions retain SQLSTATE and restore their actual statement owner."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root=Path(__file__).resolve().parent.parent
    spec=importlib.util.spec_from_file_location('delete_owner_runner',root/'tests/compat/pg_diff_runner.py')
    runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
    client=runner.load_protocol_client()
    reference=sys.argv[1:]==['--reference18']
    assert not sys.argv[1:] or reference
    server=None
    if reference:
        host,port,user,database,password=runner._reference_connection_settings()
        sock=socket.create_connection((host,port),timeout=runner.wire_timeout())
        client.startup_reference(sock,user,database,password=password)
        runner.verify_reference_version(client,sock)
    else:
        server=runner.start_ours(client);sock=server['sock']
    failures=[]
    def query(sql,state=None,rows=None,oids=None,ready=b'I'):
        messages=client.simple_query(sock,sql)
        actual=runner.decode_wire_result(messages,include_types=True)
        statuses=[body for kind,body in messages if kind==b'Z']
        print('DELETE_OWNER_WIRE',sql,actual,'READY',statuses,flush=True)
        good=actual[1]==state and statuses==[ready]
        if rows is not None:good=good and actual[0]==rows
        if oids is not None:good=good and actual[5]==oids
        if not good:failures.append((sql,actual,statuses,state,rows,oids,ready))
        return actual
    rows=[['1','a','plain'],['2','ab','20000'],['3','',''],['4',None,None],['5','NULL','NULL']]
    read="SELECT id,v,CASE WHEN length(payload)>100 THEN length(payload)::text ELSE payload END FROM delete_owner_rows ORDER BY id;"
    try:
        query('CREATE TABLE delete_owner_rows(id INT PRIMARY KEY,v TEXT,payload TEXT);')
        query("INSERT INTO delete_owner_rows VALUES(1,'a','plain'),(2,'ab',repeat('x',20000)),(3,'',''),(4,NULL,NULL),(5,'NULL','NULL');")
        query('CREATE INDEX delete_owner_v_idx ON delete_owner_rows(v);')
        query('CREATE TABLE delete_owner_audit(id INT PRIMARY KEY);')
        query("CREATE FUNCTION delete_owner_note(k INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$ BEGIN INSERT INTO delete_owner_audit VALUES(k); IF k=2 THEN RETURN 1/0; END IF; RETURN k; END; $$;")
        query(read,rows=rows,oids=[23,25,25])
        for sql,state in [
            ("DELETE FROM delete_owner_rows WHERE v LIKE '_' ESCAPE 'xx';",'22025'),
            (r"DELETE FROM delete_owner_rows WHERE v LIKE E'a\\';",'22025'),
            ('DELETE FROM delete_owner_rows WHERE id<=2 RETURNING delete_owner_note(id);','22012'),
        ]:
            query(sql,state=state,rows=[])
            query(read,rows=rows,oids=[23,25,25])
            query('SELECT id FROM delete_owner_audit ORDER BY id;',rows=[],oids=[23])
            query('BEGIN;',ready=b'T');query('COMMIT;',ready=b'I')
        query('BEGIN;',ready=b'T')
        query("UPDATE delete_owner_rows SET v=NULL,payload=repeat('y',20000) WHERE id=3;",ready=b'T')
        query('INSERT INTO delete_owner_audit VALUES(99);',ready=b'T')
        query('SAVEPOINT delete_owner_parent;',ready=b'T')
        query('DELETE FROM delete_owner_rows WHERE id<=2 RETURNING delete_owner_note(id);',state='22012',rows=[],ready=b'E')
        query('ROLLBACK TO delete_owner_parent;',ready=b'T')
        parent=[row[:] for row in rows];parent[2]=['3',None,'20000']
        query(read,rows=parent,oids=[23,25,25],ready=b'T')
        query('SELECT id FROM delete_owner_audit ORDER BY id;',rows=[['99']],oids=[23],ready=b'T')
        query('RELEASE delete_owner_parent;',ready=b'T');query('COMMIT;',ready=b'I')
        query(read,rows=parent,oids=[23,25,25])
        query("SELECT id FROM delete_owner_rows WHERE id=2 AND payload=repeat('x',20000);",rows=[['2']],oids=[23])
        print('DELETE_OWNER_WIRE_FAILURES',failures,flush=True)
        assert not failures,failures
    finally:
        try:
            client.simple_query(sock,'ROLLBACK;')
            client.simple_query(sock,'DROP FUNCTION IF EXISTS delete_owner_note(INT);')
            client.simple_query(sock,'DROP TABLE IF EXISTS delete_owner_audit;')
            client.simple_query(sock,'DROP TABLE IF EXISTS delete_owner_rows;')
        finally:
            if server:runner.stop_ours(server)
            else:sock.close()


if __name__=='__main__':main()
