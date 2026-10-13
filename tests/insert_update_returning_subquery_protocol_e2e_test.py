#!/usr/bin/env python3
"""Bound INSERT/UPDATE RETURNING children and their atomic statement owner."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('returning_child_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
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
        server = runner.start_ours(client)
        sock = server['sock']
    table = 'returning_child_' + uuid.uuid4().hex[:12]
    sequence = table + '_seq'
    failures = []
    checks = 0

    def query(sql, rows=None, oids=None, state=None, tag=None, ready=b'I', names=None):
        nonlocal checks
        checks += 1
        messages = client.simple_query(sock, sql)
        actual = runner.decode_wire_result(messages, include_types=True)
        statuses = [body for kind, body in messages if kind == b'Z']
        print('RETURNING_CHILD', sql, actual, 'READY', statuses, flush=True)
        good = actual[1] == state and statuses == [ready]
        if rows is not None:
            good = good and actual[0] == rows
        if oids is not None:
            good = good and actual[5] == oids
        if tag is not None:
            good = good and actual[4] == tag
        if names is not None:
            good = good and actual[3] == names
        if not good:
            failures.append((sql, actual, statuses, rows, oids, state, tag, ready, names))
        return actual

    seed = [['1', '10'], ['2', '20'], ['3', '30']]
    read = f'SELECT id,v FROM {table} ORDER BY id;'
    try:
        query(f'CREATE TABLE {table}(id INT PRIMARY KEY,v INT);')
        query(f'CREATE SEQUENCE {sequence};')
        query(f'INSERT INTO {table} VALUES(1,10),(2,20),(3,30);')
        for sql, state in [
            (f'INSERT INTO {table} VALUES(4,40) RETURNING id,(SELECT 1/0);', '22012'),
            (f'UPDATE {table} SET v=99 WHERE id=1 RETURNING id,(SELECT 1/0);', '22012'),
            (f'INSERT INTO {table} VALUES(5,50) RETURNING (SELECT v FROM {table});', '21000'),
            (f'UPDATE {table} SET v=99 WHERE id=1 RETURNING (SELECT v FROM {table});', '21000'),
            (f'INSERT INTO {table} VALUES(4,40),(5,50) RETURNING id,(SELECT 100/(id-5));', '22012'),
            (f'UPDATE {table} SET v=v+1 WHERE id<=2 RETURNING id,(SELECT 100/(id-2));', '22012'),
        ]:
            query(sql, rows=[], state=state)
            query(read, rows=seed, oids=[23, 23])
        query(f'INSERT INTO {table} VALUES(4,40) RETURNING id,(SELECT id+1) AS child,(SELECT NULL::INT) AS nullable;',
              rows=[['4', '5', None]], oids=[23, 23, 23], names=['id', 'child', 'nullable'], tag='INSERT 0 1')
        query(f'UPDATE {table} SET v=99 WHERE id=1 RETURNING old.v,new.v,(SELECT v) AS child,(SELECT old.v) AS prior;',
              rows=[['10', '99', '99', '10']], oids=[23, 23, 23, 23], tag='UPDATE 1')
        query(f'INSERT INTO {table} VALUES(5,NULL) RETURNING id,(SELECT v FROM {table} WHERE FALSE) AS empty;',
              rows=[['5', None]], oids=[23, 23], tag='INSERT 0 1')
        query(f"INSERT INTO {table} VALUES(6,60),(7,70) RETURNING id,(SELECT nextval('{sequence}')) AS once;",
              rows=[['6', '1'], ['7', '1']], oids=[23, 20], tag='INSERT 0 2')
        query(f"SELECT nextval('{sequence}');", rows=[['2']], oids=[20])
        query(f'INSERT INTO {table} VALUES(8,80) RETURNING old.*,new.*;',
              rows=[[None, None, '8', '80']], oids=[23, 23, 23, 23], tag='INSERT 0 1')
        query(f'INSERT INTO {table} DEFAULT VALUES RETURNING (SELECT 1/0);', rows=[], state='22012')
        # SQL PREPARE retains the child descriptor instead of returning the SQL
        # wrapper text. Each EXECUTE receives a fresh scalar-query carrier.
        query(f'PREPARE child_insert(INT,INT) AS INSERT INTO {table} VALUES($1,$2) RETURNING id,(SELECT v+1) AS child;')
        query('EXECUTE child_insert(9,90);', rows=[['9', '91']], oids=[23, 23], tag='INSERT 0 1')
        query('EXECUTE child_insert(10,100);', rows=[['10', '101']], oids=[23, 23], tag='INSERT 0 1')
        query('DEALLOCATE child_insert;')
        query('BEGIN;', ready=b'T')
        query(f'INSERT INTO {table} VALUES(11,110);', ready=b'T')
        query('SAVEPOINT parent_work;', ready=b'T')
        query(f'UPDATE {table} SET v=999 WHERE id=1 RETURNING (SELECT v FROM {table});', rows=[], state='21000', ready=b'E')
        query('ROLLBACK TO parent_work;', ready=b'T')
        query(f'SELECT id,v FROM {table} WHERE id IN(1,11) ORDER BY id;', rows=[['1', '99'], ['11', '110']], oids=[23, 23], ready=b'T')
        query('RELEASE parent_work;', ready=b'T')
        query('COMMIT;')
        # DELETE's already-bound owner remains a positive neighbour.
        query(f'DELETE FROM {table} WHERE id=11 RETURNING id,(SELECT v) AS child;',
              rows=[['11', '110']], oids=[23, 23], tag='DELETE 1')
        print('RETURNING_CHILD_CHECKS', checks, 'FAILURES', failures, flush=True)
        assert not failures, failures
    finally:
        try:
            client.simple_query(sock, 'ROLLBACK;')
            client.simple_query(sock, 'DEALLOCATE ALL;')
            client.simple_query(sock, f'DROP TABLE IF EXISTS {table};')
            client.simple_query(sock, f'DROP SEQUENCE IF EXISTS {sequence};')
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == '__main__':
    main()
