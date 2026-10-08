#!/usr/bin/env python3
"""Plain OVER admission: actual rows/NULL/OIDs, not unspecified heap order."""
import argparse
import importlib.util
from pathlib import Path
import socket
import uuid

FUNCTIONS = (
    ('row_number()', 20, ['1', '2', '3']),
    ('rank()', 20, ['1', '1', '1']),
    ('dense_rank()', 20, ['1', '1', '1']),
    ('percent_rank()', 701, ['0', '0', '0']),
    ('cume_dist()', 701, ['1', '1', '1']),
    ('ntile(2)', 23, ['1', '1', '2']),
    ('lag(v)', 23, [None, '10', '10']),
    ('lead(v)', 23, [None, '10', '10']),
    ('first_value(v)', 23, ['10', '10', '10']),
    ('last_value(v)', 23, ['10', '10', '10']),
    ('nth_value(v,2)', 23, ['10', '10', '10']),
    ('sum(v)', 20, ['30', '30', '30']),
)
INVALID_CALLS = (
    ('row_number(1)', '42883'),
    ('lag()', '42883'),
    ('first_value()', '42883'),
    ('nth_value(v)', '42883'),
    ('ntile()', '42883'),
    ('lag(missing_window_column)', '42703'),
)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reference18', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('unordered_window_runner', root/'tests/compat/pg_diff_runner.py')
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)
    c = r.load_protocol_client()
    server = None
    if args.reference18:
        host, port, user, database, password = r._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=r.wire_timeout())
        c.startup_reference(sock, user, database, password)
        r.verify_reference_version(c, sock)
    else:
        server = r.start_ours(c)
        sock = server['sock']
    def query(sql):
        return r.decode_wire_result(c.simple_query(sock, sql), include_types=True)
    def ok(sql):
        actual = query(sql)
        assert actual[1] is None, (sql, actual)
        return actual
    schema = 'unordered_window_' + uuid.uuid4().hex[:14]
    qs = '"'+schema+'"'
    failures = []
    controls = 0
    try:
        if args.reference18:
            ok('BEGIN')
        ok('CREATE SCHEMA '+qs)
        for name in ('rows', 'null_rows', 'empty_rows'):
            ok('CREATE TABLE '+qs+'.'+name+'(id INT,v INT)')
        ok('INSERT INTO '+qs+'.rows VALUES(1,10),(2,10),(3,10)')
        ok('INSERT INTO '+qs+'.null_rows VALUES(1,NULL),(2,NULL),(3,NULL)')
        # All identical values avoid imposing an ordering PostgreSQL does not
        # promise for OVER(). IDs and value multisets are checked independently.
        key = lambda value: (value is not None, value or '')
        for function, oid, populated in FUNCTIONS:
            for source, suffix in (('rows',''), ('null_rows',''), ('empty_rows',''),
                                   ('rows',' WHERE FALSE'), ('rows',' LIMIT 0')):
                sql = 'SELECT id,'+function+' OVER () AS value FROM '+qs+'.'+source+suffix
                actual = query(sql)
                expected = [] if source == 'empty_rows' or suffix else (
                    [None]*3 if source == 'null_rows' and 'v' in function else populated)
                valid = actual[1] is None and actual[3] == ['id','value'] and actual[5] == [23,oid]
                valid = valid and actual[4] == 'SELECT '+str(len(expected))
                valid = valid and all(len(row) == 2 for row in actual[0])
                valid = valid and sorted(row[0] for row in actual[0]) == ([] if not expected else ['1','2','3'])
                valid = valid and sorted((row[1] for row in actual[0]), key=key) == sorted(expected, key=key)
                controls += 1
                print('UNORDERED_WINDOW', function, source, suffix, 'actual', actual, 'expected', expected, flush=True)
                if not valid:
                    failures.append((sql,actual,expected,oid))
        assert controls == 60
        if args.reference18:
            ok('SAVEPOINT window_invalid')
        for function, state in INVALID_CALLS:
            for source, suffix in (('rows',''), ('empty_rows',''),
                                   ('rows',' WHERE FALSE'), ('rows',' LIMIT 0')):
                sql = 'SELECT '+function+' OVER () AS value FROM '+qs+'.'+source+suffix
                actual = query(sql)
                controls += 1
                print('UNORDERED_WINDOW_INVALID', function, source, suffix,
                      'actual', actual, 'expected_state', state, flush=True)
                if actual[1] != state:
                    failures.append((sql, actual, state))
                if args.reference18:
                    # Each deliberately invalid statement needs a real
                    # PostgreSQL savepoint, not an aborted-transaction oracle.
                    ok('ROLLBACK TO SAVEPOINT window_invalid')
        assert controls == 84
        print('[UNORDERED WINDOW] complete controls=84 failures='+str(len(failures)), flush=True)
        assert not failures, failures
    finally:
        if args.reference18:
            try:
                query('ROLLBACK')
            finally:
                sock.close()
        else:
            try:
                query('DROP SCHEMA '+qs+' CASCADE')
            finally:
                r.stop_ours(server)

if __name__ == '__main__':
    main()
