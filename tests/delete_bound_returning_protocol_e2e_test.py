#!/usr/bin/env python3
"""Whole retained DELETE qualification/projection and same-owner effects."""
import importlib.util
import socket
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('delete_bound_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password=password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client); sock = server['sock']
    failures = []

    def query(sql, rows=None, oids=None, state=None, tag=None, ready=b'I', unordered=False):
        messages = client.simple_query(sock, sql)
        actual = runner.decode_wire_result(messages, include_types=True)
        statuses = [body for kind, body in messages if kind == b'Z']
        print('DELETE_BOUND', sql, actual, 'READY', statuses, flush=True)
        good = actual[1] == state and statuses == [ready]
        if rows is not None:
            key = lambda row: tuple((cell is None, cell or '') for cell in row)
            got = sorted(actual[0], key=key) if unordered else actual[0]
            expected = sorted(rows, key=key) if unordered else rows
            good = good and got == expected
        if oids is not None: good = good and actual[5] == oids
        if tag is not None: good = good and actual[4] == tag
        if not good: failures.append((sql, actual, statuses, rows, oids, state, tag, ready))
        return actual

    seed = "INSERT INTO delete_bound_rows VALUES(1,'a','a'::CHAR(3),decode('0041','hex'),ARRAY[1,2]),(2,'ab','é'::CHAR(3),decode('ff','hex'),NULL),(3,'',''::CHAR(3),decode('','hex'),ARRAY[]::INT[]),(4,NULL,NULL,NULL,NULL),(5,'NULL','x'::CHAR(3),decode('00','hex'),'[0:1]={3,4}'::INT[]);"
    rows = [['1','a'],['2','ab'],['3',''],['4',None],['5','NULL']]
    read = 'SELECT id,v FROM delete_bound_rows ORDER BY id;'
    try:
        query('CREATE TABLE delete_bound_rows(id INT PRIMARY KEY,v TEXT,c CHAR(3),b BYTEA,a INT[]);')
        query('CREATE INDEX delete_bound_v_idx ON delete_bound_rows(v);')
        query('CREATE TABLE delete_bound_audit(id INT PRIMARY KEY);')
        query('CREATE SEQUENCE delete_bound_seq;')
        query('CREATE SEQUENCE delete_bound_native_seq;')
        query("CREATE FUNCTION delete_bound_fail(k INT) RETURNS INT VOLATILE LANGUAGE plpgsql AS $$ BEGIN INSERT INTO delete_bound_audit VALUES(k); PERFORM nextval('delete_bound_seq'); IF k=2 THEN RETURN 1/0; END IF; RETURN k; END; $$;")
        query("CREATE FUNCTION delete_bound_where(k INT) RETURNS BOOLEAN VOLATILE LANGUAGE plpgsql AS $$ BEGIN INSERT INTO delete_bound_audit VALUES(k+100); RETURN k<=2; END; $$;")
        query(seed)
        query('DELETE FROM delete_bound_rows WHERE FALSE RETURNING delete_bound_fail(id);',rows=[],oids=[23],tag='DELETE 0')
        query('SELECT id FROM delete_bound_audit ORDER BY id;',rows=[],oids=[23])
        query(read,rows=rows,oids=[23,25])
        query('DELETE FROM delete_bound_rows WHERE id<=2 RETURNING delete_bound_fail(id);',rows=[],state='22012')
        query(read,rows=rows,oids=[23,25])
        query('SELECT id FROM delete_bound_audit ORDER BY id;',rows=[],oids=[23])
        query("SELECT nextval('delete_bound_seq');",rows=[['3']],oids=[20])
        query("DELETE FROM delete_bound_rows WHERE id<=2 RETURNING 10/(id-2),nextval('delete_bound_native_seq');",rows=[],state='22012')
        query(read,rows=rows,oids=[23,25])
        query("SELECT nextval('delete_bound_native_seq');",rows=[['2']],oids=[20])
        query("DELETE FROM delete_bound_rows AS victim WHERE victim.id=99 RETURNING abs(victim.id),victim.v,encode(victim.b,'hex'),victim.c,victim.a;",rows=[],oids=[23,25,25,1042,1007],tag='DELETE 0')
        query('DELETE FROM delete_bound_rows WHERE FALSE RETURNING missing_delete_bound_function(id);',rows=[],state='42883')
        query(read,rows=rows,oids=[23,25])
        query("DELETE FROM delete_bound_rows AS victim WHERE victim.id<=2 RETURNING abs(victim.id) AS n,CASE WHEN victim.v IS NULL THEN 'sql-null' ELSE victim.v END AS value,encode(victim.b,'hex'),victim.c,victim.a;",
              rows=[['1','a','0041','a  ','{1,2}'],['2','ab','ff','é  ',None]],oids=[23,25,25,1042,1007],tag='DELETE 2',unordered=True)
        query(read,rows=rows[2:],oids=[23,25])
        query('DELETE FROM delete_bound_rows WHERE id>=3 RETURNING id,v,b,c,a;',rows=[['3','',r'\x','   ','{}'],['4',None,None,None,None],['5','NULL',r'\x00','x  ','[0:1]={3,4}']],oids=[23,25,17,1042,1007],tag='DELETE 3',unordered=True)
        query(seed)
        query('BEGIN;',ready=b'T')
        query("UPDATE delete_bound_rows SET v=NULL WHERE id=3;",ready=b'T')
        query('INSERT INTO delete_bound_audit VALUES(99);',ready=b'T')
        query('SAVEPOINT delete_bound_parent;',ready=b'T')
        query('DELETE FROM delete_bound_rows WHERE id<=2 RETURNING delete_bound_fail(id);',rows=[],state='22012',ready=b'E')
        query('ROLLBACK TO delete_bound_parent;',ready=b'T')
        prior=[row[:] for row in rows];prior[2]=['3',None]
        query(read,rows=prior,oids=[23,25],ready=b'T')
        query('SELECT id FROM delete_bound_audit ORDER BY id;',rows=[['99']],oids=[23],ready=b'T')
        query('RELEASE delete_bound_parent;',ready=b'T');query('COMMIT;',ready=b'I')
        query('DELETE FROM delete_bound_audit;')
        query('DELETE FROM delete_bound_rows WHERE delete_bound_where(id) RETURNING id,v;',rows=rows[:2],oids=[23,25],tag='DELETE 2',unordered=True)
        query('SELECT id FROM delete_bound_audit ORDER BY id;',rows=[['101'],['102'],['103'],['104'],['105']],oids=[23])
        query(read,rows=prior[2:],oids=[23,25])
        query('DELETE FROM delete_bound_rows WHERE v IS NULL OR v IN(\'\',\'NULL\') RETURNING id,v;',rows=prior[2:],oids=[23,25],tag='DELETE 3',unordered=True)
        query(read,rows=[],oids=[23,25])
        query(seed)
        query('CREATE TABLE delete_bound_source(id INT);')
        query('INSERT INTO delete_bound_source VALUES(1),(1),(2);')
        query('DELETE FROM delete_bound_rows AS victim USING delete_bound_source AS producer WHERE victim.id=producer.id RETURNING abs(victim.id),victim.v,producer.id;',rows=[['1','a','1'],['2','ab','2']],oids=[23,25,23],tag='DELETE 2',unordered=True)
        query(read,rows=rows[2:],oids=[23,25])
        query('DELETE FROM delete_bound_rows RETURNING abs(id),v;',rows=rows[2:],oids=[23,25],tag='DELETE 3',unordered=True)
        query(read,rows=[],oids=[23,25])
        # An actual nullable outer source supplies typed NULLs, not rendered
        # text sent back through the SQL parser. Duplicate source matches still
        # delete each target once and exclude unmatched target rows.
        query('CREATE TABLE delete_bound_outer_target(id INT PRIMARY KEY,v TEXT);')
        query("INSERT INTO delete_bound_outer_target VALUES(1,'matched'),(2,NULL),(3,'');")
        query('CREATE TABLE delete_bound_outer_source(id INT);')
        query('INSERT INTO delete_bound_outer_source VALUES(1),(2),(2),(99);')
        query('CREATE TABLE delete_bound_outer_nullable(id INT,v TEXT);')
        query("INSERT INTO delete_bound_outer_nullable VALUES(1,'right'),(99,'excluded');")
        query('DELETE FROM delete_bound_outer_target AS victim USING delete_bound_outer_source AS producer LEFT JOIN delete_bound_outer_nullable AS nullable ON producer.id=nullable.id WHERE victim.id=producer.id AND nullable.id IS NULL RETURNING abs(victim.id),victim.v,producer.id,nullable.id,nullable.v;',rows=[['2',None,'2',None,None]],oids=[23,25,23,23,25],tag='DELETE 1')
        query('SELECT id,v FROM delete_bound_outer_target ORDER BY id;',rows=[['1','matched'],['3','']],oids=[23,25])
        print('DELETE_BOUND_FAILURES',failures,flush=True)
        assert not failures,failures
    finally:
        try:
            for sql in ['ROLLBACK;','DROP FUNCTION IF EXISTS delete_bound_fail(INT);','DROP FUNCTION IF EXISTS delete_bound_where(INT);','DROP TABLE IF EXISTS delete_bound_audit;','DROP TABLE IF EXISTS delete_bound_source;','DROP TABLE IF EXISTS delete_bound_rows;','DROP TABLE IF EXISTS delete_bound_outer_target;','DROP TABLE IF EXISTS delete_bound_outer_source;','DROP TABLE IF EXISTS delete_bound_outer_nullable;','DROP SEQUENCE IF EXISTS delete_bound_seq;','DROP SEQUENCE IF EXISTS delete_bound_native_seq;']:
                client.simple_query(sock,sql)
        finally:
            if server: runner.stop_ours(server)
            else: sock.close()


if __name__=='__main__': main()
