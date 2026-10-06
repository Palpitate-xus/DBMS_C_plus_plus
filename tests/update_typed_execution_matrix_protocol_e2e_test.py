#!/usr/bin/env python3
"""Ordinary UPDATE binding, NULL, correlation, errors and per-row demand."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('update_matrix_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = '--reference' in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        try:
            client.startup_reference(sock, user, database, password)
            version = runner.decode_wire_result(client.simple_query(sock, 'SHOW server_version_num;'))
            assert version[1] is None and version[0] == [['170002']], version
            print('Actual PG17.2 diagnostic reference', version[0], flush=True)
            assert runner.decode_wire_result(client.simple_query(sock, 'BEGIN;'))[1] is None
            namespace = 'update_ref_' + uuid.uuid4().hex
            assert runner.decode_wire_result(client.simple_query(sock, 'CREATE SCHEMA ' + namespace + ';'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'SET LOCAL search_path TO ' + namespace + ';'))[1] is None
        except BaseException:
            try:
                client.simple_query(sock, 'ROLLBACK;')
            finally:
                sock.close()
            raise
        server = {'sock': sock}
    else:
        server = runner.start_ours(client)
    failures = []

    def query(sql, state=None, rows=None):
        if reference:
            assert runner.decode_wire_result(client.simple_query(sock, 'SAVEPOINT update_reference_statement;'))[1] is None
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        if reference:
            if result[1] is not None:
                assert runner.decode_wire_result(client.simple_query(sock, 'ROLLBACK TO SAVEPOINT update_reference_statement;'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'RELEASE SAVEPOINT update_reference_statement;'))[1] is None
        print('UPDATE_MATRIX', sql, result, flush=True)
        if result[1] != state or rows is not None and result[0] != rows:
            failures.append((sql, result, state, rows))
        return result

    try:
        query('CREATE TABLE update_matrix(id INT PRIMARY KEY,a INT,b INT,v TEXT);')
        query('CREATE TABLE update_matrix_effects(v INT);')
        for index, (assignment, predicate, state, rows, calls) in enumerate((
            ('a=WRITER(9)', 'NULL', None, [['1','1','10',''],['2',None,'20',None]], 0),
            ('a=WRITER(9)', '1', '42804', [['1','1','10',''],['2',None,'20',None]], 0),
            ('a=WRITER(9)', 'CAST(NULL AS TEXT)', '42804', [['1','1','10',''],['2',None,'20',None]], 0),
            ('a=WRITER(9)', "id=CAST('bad' AS INT)", '22P02', [['1','1','10',''],['2',None,'20',None]], 0),
            ('a=CASE WHEN false THEN missing ELSE WRITER(9) END', 'false', '42703', [['1','1','10',''],['2',None,'20',None]], 0),
            ('a=b,b=a', 'true', None, [['1','10','1',''],['2','20',None,None]], 0),
            ('a=(SELECT x.a+10)', 'id=1', None, [['1','11','10',''],['2',None,'20',None]], 0),
            ('a=WRITER(9)', '(SELECT x.id)=1', None, [['1','9','10',''],['2',None,'20',None]], 1),
            ('a=WRITER(9)', "v=''", None, [['1','9','10',''],['2',None,'20',None]], 1),
            ('a=WRITER(9)', "'yes'", None, [['1','9','10',''],['2','9','20',None]], 2),
            ('a=WRITER(9)', "'not_boolean'", '22P02', [['1','1','10',''],['2',None,'20',None]], 0),
        )):
            query('DELETE FROM update_matrix;')
            query('DELETE FROM update_matrix_effects;')
            query("INSERT INTO update_matrix VALUES(1,1,10,''),(2,NULL,20,NULL);")
            sequence = 'update_matrix_seq_%s' % index
            function = 'update_matrix_writer_%s' % index
            query('CREATE SEQUENCE %s;' % sequence)
            query("CREATE FUNCTION %s(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO update_matrix_effects VALUES(arg); PERFORM nextval('%s'); RETURN arg; END; $$;" % (function, sequence))
            query('UPDATE update_matrix AS x SET %s WHERE %s;' % (assignment.replace('WRITER', function), predicate), state=state)
            query('SELECT id,a,b,v FROM update_matrix ORDER BY id;', rows=rows)
            query('SELECT v FROM update_matrix_effects;', rows=[['9']]*calls)
            query("SELECT currval('%s');" % sequence,
                  state=None if calls else '55000', rows=[[str(calls)]] if calls else [])

        # An error after a previous matched row's SET must roll back both
        # updates and transactional writer effects. Sequences are not undoable.
        query('DELETE FROM update_matrix;')
        query('DELETE FROM update_matrix_effects;')
        query("INSERT INTO update_matrix VALUES(1,1,10,''),(2,2,20,NULL);")
        query('CREATE SEQUENCE update_matrix_error_seq;')
        query("CREATE FUNCTION update_matrix_error_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO update_matrix_effects VALUES(arg); PERFORM nextval('update_matrix_error_seq'); IF arg=2 THEN RAISE EXCEPTION 'late failure'; END IF; RETURN arg+10; END; $$;")
        query('UPDATE update_matrix SET a=update_matrix_error_writer(a);', state='P0001')
        query('SELECT id,a FROM update_matrix ORDER BY id;', rows=[['1','1'],['2','2']])
        query('SELECT v FROM update_matrix_effects;', rows=[])
        query("SELECT currval('update_matrix_error_seq');", rows=[['2']])
        query('UPDATE update_matrix SET a=a+1 WHERE id=1 RETURNING id,a;', rows=[['1','2']])
        query('SELECT id,a FROM update_matrix ORDER BY id;', rows=[['1','2'],['2','2']])
        query("UPDATE update_matrix SET a='bad' WHERE false;", state='22P02')
        query('UPDATE update_matrix SET id=NULL WHERE false;')
        query('SELECT id,a FROM update_matrix ORDER BY id;', rows=[['1','2'],['2','2']])
        query('DELETE FROM update_matrix;')
        query("UPDATE update_matrix SET a=update_matrix_writer_0(9) WHERE 'not_boolean';", state='22P02')
        query('SELECT v FROM update_matrix_effects;', rows=[])
        query("SELECT currval('update_matrix_seq_0');", state='55000', rows=[])
        assert not failures, '%s typed UPDATE matrix failures: %r' % (len(failures), failures)
        print('[UPDATE TYPED EXECUTION MATRIX] all controls passed')
    finally:
        if reference:
            try:
                client.simple_query(sock, 'ROLLBACK;')
            finally:
                sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
