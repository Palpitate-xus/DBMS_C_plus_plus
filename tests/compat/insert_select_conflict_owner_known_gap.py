#!/usr/bin/env python3
"""INSERT owns ON CONFLICT/RETURNING after a SELECT source."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location('insert_source_conflict_runner', root / 'tests/compat/pg_diff_runner.py')
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
    table = 'insert_source_conflict_' + uuid.uuid4().hex[:12]
    source = table + '_src'
    sequence = table + '_seq'
    failures = []
    checks = 0

    def query(sql, rows=None, oids=None, state=None):
        nonlocal checks
        checks += 1
        messages = client.simple_query(sock, sql)
        actual = runner.decode_wire_result(messages, include_types=True)
        ready = [body for kind, body in messages if kind == b'Z']
        good = actual[1] == state and ready == [b'I']
        if rows is not None:
            good = good and actual[0] == rows
        if oids is not None:
            good = good and actual[5] == oids
        print('INSERT_SOURCE_CONFLICT', sql, actual, 'PASS', good, flush=True)
        if not good:
            failures.append((sql, actual, ready, rows, oids, state))

    try:
        query(f'CREATE TABLE {table}(id INT PRIMARY KEY,v INT);')
        query(f'CREATE TABLE {source}(id INT,v INT);')
        query(f'CREATE SEQUENCE {sequence};')
        query(f'INSERT INTO {table} VALUES(1,10);')
        query(f'INSERT INTO {source} VALUES(1,20),(2,30);')
        query(f'INSERT INTO {table} SELECT 1,99 WHERE FALSE ON CONFLICT DO NOTHING RETURNING id,v;', rows=[], oids=[23, 23])
        query(f'INSERT INTO {table} SELECT 1,20 ON CONFLICT DO NOTHING RETURNING id,v;', rows=[], oids=[23, 23])
        query(f'INSERT INTO {table} SELECT 1,20 ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v;', rows=[['1', '20']], oids=[23, 23])
        query(f'INSERT INTO {table} SELECT s.id,s.v FROM {source} s WHERE s.id=2 ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE TRUE RETURNING id,v;', rows=[['2', '30']], oids=[23, 23])
        query(f'INSERT INTO {table} SELECT (SELECT 2),90 WHERE FALSE ON CONFLICT(id) DO UPDATE SET v=excluded.v RETURNING id,v;', rows=[], oids=[23, 23])
        query(f'INSERT INTO {table} SELECT s.id,s.v FROM {source} s JOIN {source} p ON s.id=p.id WHERE TRUE ON CONFLICT DO NOTHING RETURNING id,v;', rows=[], oids=[23, 23])
        query(f"INSERT INTO {table} SELECT 3,length('ON CONFLICT RETURNING') ON /* clause */ CONFLICT(id) DO NOTHING RETURNING id,v;", rows=[['3', str(len('ON CONFLICT RETURNING'))]], oids=[23, 23])
        query(f'INSERT INTO {table} SELECT 1,99 ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE FALSE RETURNING id,v;', rows=[], oids=[23, 23])
        query(f'SELECT id,v FROM {table} ORDER BY id;', rows=[['1', '20'], ['2', '30'], ['3', '21']], oids=[23, 23])
        for suffix in ['ON CONFLICT', 'ON CONFLICT(id) DO UPDATE',
                       'ON CONFLICT(id) DO NOTHING garbage', 'ON CONFLICT(id) DO UPDATE SET']:
            query(f"INSERT INTO {table} SELECT nextval('{sequence}'),10 {suffix};", rows=[], state='42601')
        query(f"SELECT currval('{sequence}');", rows=[], state='55000')
        for index, suffix in enumerate(['ON CONFLICT DO NOTHING', 'ON CONFLICT(id) DO UPDATE SET v=excluded.v']):
            checks += 1
            name = f'insert_source_shape_{index}'.encode()
            sql = f"INSERT INTO {table} SELECT nextval('{sequence}')::INT,10 {suffix} RETURNING id,v"
            body = name + b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
            sock.sendall(client.typed(b'P', body) + client.typed(b'D', b'S' + name + b'\0') + client.typed(b'S'))
            messages = client.read_until_ready(sock)
            types = [field[3] for field in client.row_description_fields(messages)]
            good = types == [23, 23] and not any(kind in (b'E', b'D', b'C') for kind, _ in messages)
            print('INSERT_SOURCE_CONFLICT_DESCRIBE', sql, types, 'PASS', good, flush=True)
            if not good:
                failures.append(('describe', sql, messages, types))
        query(f"SELECT currval('{sequence}');", rows=[], state='55000')
        query(f'SELECT id,v FROM {table} ORDER BY id;', rows=[['1', '20'], ['2', '30'], ['3', '21']], oids=[23, 23])
        print('INSERT_SOURCE_CONFLICT_CHECKS', checks, 'FAILURES', failures, flush=True)
        assert not failures, failures
    finally:
        try:
            client.simple_query(sock, 'ROLLBACK;')
            client.simple_query(sock, f'DROP TABLE IF EXISTS {table};')
            client.simple_query(sock, f'DROP TABLE IF EXISTS {source};')
            client.simple_query(sock, f'DROP SEQUENCE IF EXISTS {sequence};')
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == '__main__':
    main()
