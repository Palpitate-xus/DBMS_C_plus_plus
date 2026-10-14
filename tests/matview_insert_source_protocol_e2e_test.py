#!/usr/bin/env python3
"""Prepared INSERT readers resolve actual materialized-view backing storage."""
import importlib.util
import socket
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('matview_insert_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server = None
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        sock = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(sock, user, database, password)
        runner.verify_reference_version(client, sock)
    else:
        server = runner.start_ours(client)
        sock = server['sock']
    schema = 'r49_' + uuid.uuid4().hex[:16]
    base, view, sink = (schema + '.' + name for name in ('base', 'view_rows', 'sink'))
    checks, failures = 0, []
    expected = [['1', ''], ['2', None], ['3', 'NULL'], ['4', 'two words']]

    def query(sql):
        return runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)

    def control(sql, rows=None, state=None, tag=None):
        nonlocal checks
        result = query(sql)
        checks += 1
        valid = result[1] == state
        if state:
            valid = valid and not result[0] and result[4] is None
        if rows is not None:
            valid = valid and result[0] == rows and result[3] == ['id', 'note'] and result[5] == [23, 25]
        if tag is not None:
            valid = valid and result[4] == tag
        print('MATVIEW_INSERT_SOURCE', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, tag))
        return valid

    try:
        assert control('CREATE SCHEMA ' + schema)
        assert control('CREATE TABLE ' + base + '(id INT,note TEXT)')
        assert control("INSERT INTO " + base + " VALUES(1,''),(2,NULL),(3,'NULL'),(4,'two words')", tag='INSERT 0 4')
        assert control('CREATE MATERIALIZED VIEW ' + view + ' AS SELECT id,note FROM ' + base + ' WITH NO DATA')
        assert control('CREATE TABLE ' + sink + '(id INT PRIMARY KEY,note TEXT)')
        control('INSERT INTO ' + sink + ' SELECT id,note FROM ' + view, state='55000')
        control('INSERT INTO ' + sink + ' SELECT missing,note FROM ' + view, state='42703')
        control('SELECT id,note FROM ' + sink, rows=[], tag='SELECT 0')
        assert control('REFRESH MATERIALIZED VIEW ' + view)
        sources = [
            'SELECT id,note FROM ' + view,
            'SELECT m.id,m.note FROM ' + view + ' m',
            'SELECT id,note FROM (SELECT id,note FROM ' + view + ') q',
            'WITH q AS (SELECT id,note FROM ' + view + ') SELECT id,note FROM q',
            'SELECT m.id,m.note FROM ' + view + ' m JOIN ' + base + ' b ON m.id=b.id',
            'SELECT id,note FROM ' + view + ' ORDER BY id DESC',
        ]
        for source in sources:
            assert control('TRUNCATE ' + sink)
            control('INSERT INTO ' + sink + ' ' + source, tag='INSERT 0 4')
            control('SELECT id,note FROM ' + sink + ' ORDER BY id', rows=expected, tag='SELECT 4')
        control('INSERT INTO ' + sink + ' SELECT id,note FROM ' + view + ' ON CONFLICT(id) DO NOTHING', tag='INSERT 0 0')
        control('INSERT INTO ' + sink + ' SELECT id,note FROM ' + view + ' ON CONFLICT(id) DO UPDATE SET note=excluded.note', tag='INSERT 0 4')
        control('SELECT id,note FROM ' + sink + ' ORDER BY id', rows=expected, tag='SELECT 4')
        assert control('INSERT INTO ' + base + ' VALUES(5,NULL)', tag='INSERT 0 1')
        assert control('TRUNCATE ' + sink)
        control('INSERT INTO ' + sink + ' SELECT id,note FROM ' + view, tag='INSERT 0 4')
        control('SELECT id,note FROM ' + sink + ' ORDER BY id', rows=expected, tag='SELECT 4')
        assert control('REFRESH MATERIALIZED VIEW ' + view)
        assert control('TRUNCATE ' + sink)
        control('INSERT INTO ' + sink + ' SELECT id,note FROM ' + view, tag='INSERT 0 5')
        control('SELECT id,note FROM ' + sink + ' ORDER BY id', rows=expected + [['5', None]], tag='SELECT 5')
        assert control('REFRESH MATERIALIZED VIEW ' + view + ' WITH NO DATA')
        control('INSERT INTO ' + sink + ' SELECT id,note FROM ' + view, state='55000')
        control('SELECT id,note FROM ' + sink + ' ORDER BY id', rows=expected + [['5', None]], tag='SELECT 5')
        print('MATVIEW_INSERT_SOURCE_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks >= 40 and not failures, failures
    finally:
        try:
            query('ROLLBACK')
            query('DROP SCHEMA IF EXISTS ' + schema + ' CASCADE')
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == '__main__':
    main()
