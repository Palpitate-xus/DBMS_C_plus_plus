#!/usr/bin/env python3
"""Duplicate UPDATE targets reject without hiding RHS binding errors/effects."""
import importlib.util
from pathlib import Path
import socket
import sys
import uuid


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('update_duplicate_runner', root/'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference18 = '--reference18' in sys.argv[1:]
    if reference18 and '--reference' in sys.argv[1:]:
        raise ValueError('choose PostgreSQL18.6 reference or PG17.2 diagnostic, not both')
    reference = reference18 or '--reference' in sys.argv[1:]
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=120)
        try:
            client.startup_reference(sock, user, database, password)
            if reference18:
                runner.verify_reference_version(client, sock)
                print('Actual PG18.6 reference 180006', flush=True)
            else:
                version = runner.decode_wire_result(client.simple_query(sock, 'SHOW server_version_num;'))
                assert version[1] is None and version[0] == [['170002']], version
                print('Actual PG17.2 diagnostic reference', version[0], flush=True)
            assert runner.decode_wire_result(client.simple_query(sock, 'BEGIN;'))[1] is None
            namespace = 'duplicate_update_ref_' + uuid.uuid4().hex
            assert runner.decode_wire_result(client.simple_query(sock, 'CREATE SCHEMA ' + namespace + ';'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'SET LOCAL search_path TO ' + namespace + ';'))[1] is None
        except BaseException:
            try: client.simple_query(sock, 'ROLLBACK;')
            finally: sock.close()
            raise
        server = {'sock': sock}
    else:
        server = runner.start_ours(client)
    failures = []

    def query(sql, state=None, rows=None):
        if reference:
            assert runner.decode_wire_result(client.simple_query(sock, 'SAVEPOINT duplicate_update_statement;'))[1] is None
        result = runner.decode_wire_result(client.simple_query(server['sock'], sql), include_types=True)
        if reference:
            if result[1] is not None:
                assert runner.decode_wire_result(client.simple_query(sock, 'ROLLBACK TO SAVEPOINT duplicate_update_statement;'))[1] is None
            assert runner.decode_wire_result(client.simple_query(sock, 'RELEASE SAVEPOINT duplicate_update_statement;'))[1] is None
        print('UPDATE_DUPLICATE', sql, result, flush=True)
        if result[1] != state or rows is not None and result[0] != rows:
            failures.append((sql, result, state, rows))
        return result

    try:
        query('CREATE TABLE duplicate_rows(a INT,"A" INT,b INT);')
        query('CREATE TABLE duplicate_source(v INT);')
        query('INSERT INTO duplicate_source VALUES(77);')
        query('CREATE TABLE duplicate_effects(v INT);')
        query('CREATE SEQUENCE duplicate_seq;')
        query("CREATE FUNCTION duplicate_writer(arg INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN INSERT INTO duplicate_effects VALUES(arg); PERFORM nextval('duplicate_seq'); RETURN arg; END; $$;")
        for statement, state in (
            ('UPDATE duplicate_rows SET a=1,a=2;', '42601'),
            ('UPDATE duplicate_missing_relation SET a=1,a=2;', '42P01'),
            ('UPDATE duplicate_rows SET missing=1,missing=2;', '42703'),
            ('UPDATE duplicate_rows SET a=missing,a=2;', '42703'),
            ('UPDATE duplicate_rows SET a=duplicate_missing_function(1),a=2;', '42883'),
            ("UPDATE duplicate_rows SET a=CAST('bad' AS INT),a=2;", '22P02'),
            ('UPDATE duplicate_rows SET a=1,A=2;', '42601'),
            ('UPDATE duplicate_rows SET a=1,"a"=2;', '42601'),
            ('UPDATE duplicate_rows SET a=duplicate_writer(9),a=duplicate_writer(9);', '42601'),
            ('UPDATE duplicate_rows SET a=DEFAULT,a=2;', '42601'),
            ('UPDATE duplicate_rows SET a=missing,a=DEFAULT;', '42703'),
            ("UPDATE duplicate_rows SET a=CAST('bad' AS INT),a=DEFAULT;", '22P02'),
            ('UPDATE duplicate_rows SET a=s.v,a=2 FROM duplicate_source s;', '42601'),
            ('UPDATE duplicate_rows SET a=missing,a=2 FROM duplicate_source s;', '42703'),
            ("UPDATE duplicate_rows SET a=CAST('bad' AS INT),a=2 FROM duplicate_source s;", '22P02'),
            ('UPDATE duplicate_rows SET a=1,a=2 FROM duplicate_missing_source;', '42P01'),
            ('UPDATE duplicate_rows SET a=1,a=2 RETURNING missing;', '42703'),
            ("UPDATE duplicate_rows SET a=1,a=2 RETURNING nextval('duplicate_seq');", '42601'),
            ('UPDATE duplicate_rows SET a=1/0,a=2;', '42601'),
            ('UPDATE duplicate_rows SET a=1,a=2 WHERE 1/0=1;', '42601'),
            ('UPDATE duplicate_rows SET a=CAST(2147483648 AS INT),a=2;', '42601'),
            ("UPDATE duplicate_rows SET a=1,a=2 WHERE CAST('bad' AS INT)=1;", '22P02'),
            ("UPDATE duplicate_rows SET a=1,a=2 RETURNING CAST('bad' AS INT);", '22P02'),
            ('UPDATE duplicate_rows SET a=1,a=2 WHERE 1;', '42804'),
            ("UPDATE duplicate_rows SET a=1,a=2 FROM (SELECT CAST('bad' AS INT) AS v) s;", '22P02'),
            ('UPDATE duplicate_rows SET a=1,a=2 FROM (SELECT 1/0 AS v) s;', '42601'),
            ("UPDATE duplicate_rows SET a=1,a=2 FROM duplicate_source s JOIN duplicate_source t ON CAST('bad' AS INT)=1;", '22P02'),
            ("UPDATE duplicate_rows SET a=1,a=2 FROM (SELECT 1 AS v FROM (SELECT CAST('bad' AS INT) AS w) nested) s;", '22P02'),
            ("UPDATE duplicate_rows SET a=1,a=2 FROM (SELECT DISTINCT ON (CAST('bad' AS INT)) 1 AS v FROM duplicate_source) s;", '22P02'),
            ("UPDATE duplicate_rows SET a=1,a=2 FROM (SELECT 1 AS v FROM duplicate_source GROUP BY CAST('bad' AS INT)) s;", '22P02'),
            ("UPDATE duplicate_rows SET a=1,a=2 FROM (SELECT row_number() OVER (ORDER BY CAST('bad' AS INT)) AS v FROM duplicate_source) s;", '22P02'),
            ("UPDATE duplicate_rows SET a=1,a=2 FROM (WITH x AS (SELECT CAST('bad' AS INT) AS w) SELECT w AS v FROM x) s;", '22P02'),
        ):
            query('DELETE FROM duplicate_rows;')
            query('INSERT INTO duplicate_rows VALUES(1,10,20);')
            query(statement, state=state)
            query('SELECT a,"A",b FROM duplicate_rows;', rows=[['1','10','20']])
            query('SELECT v FROM duplicate_effects;', rows=[])
            query("SELECT currval('duplicate_seq');", state='55000', rows=[])
        query('UPDATE duplicate_rows SET a=1,"A"=2;')
        query('SELECT a,"A",b FROM duplicate_rows;', rows=[['1','2','20']])
        assert not failures, '%s duplicate UPDATE assertions failed: %r' % (len(failures), failures)
        print('[UPDATE DUPLICATE TARGET PROTOCOL E2E] all controls passed')
    finally:
        if reference:
            try: client.simple_query(sock, 'ROLLBACK;')
            finally: sock.close()
        else:
            runner.stop_ours(server)


if __name__ == '__main__':
    main()
