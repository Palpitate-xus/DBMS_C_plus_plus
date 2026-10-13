#!/usr/bin/env python3
"""HAVING uses static BOOLEAN context before source/aggregate execution."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('having_boolean_runner', root / 'tests/compat/pg_diff_runner.py')
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
    table = 'having_boolean_' + uuid.uuid4().hex[:12]
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
        if not good:
            failures.append((sql, actual, ready, rows, oids, state))
        print('HAVING_BOOLEAN', sql, actual, 'PASS', good, flush=True)

    try:
        query(f'CREATE TABLE {table}(id INT,b BOOL);')
        query(f'INSERT INTO {table} VALUES(1,TRUE),(2,FALSE),(3,NULL);')
        query(f'CREATE SEQUENCE {sequence};')
        for condition, state in [('1', '42804'), ('NULL::INT', '42804'),
                                 ("'notboolean'", '22P02'), ("'true'::TEXT", '42804'),
                                 ('NULL::TEXT', '42804'), ('(SELECT 1)', '42804')]:
            for where in ['', ' WHERE FALSE']:
                query(f"SELECT count(nextval('{sequence}')) FROM {table}{where} HAVING {condition};", rows=[], state=state)
            query(f'SELECT id FROM {table} GROUP BY id HAVING {condition};', rows=[], state=state)
        query(f"SELECT currval('{sequence}');", rows=[], state='55000')
        for index, (condition, state) in enumerate([('1', '42804'), ('NULL::INT', '42804'),
                                                   ("'notboolean'", '22P02'), ("'true'::TEXT", '42804')]):
            checks += 1
            sql = f"SELECT count(nextval('{sequence}')) FROM {table} WHERE FALSE HAVING {condition}"
            body = f'having_bad_{index}'.encode() + b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
            sock.sendall(client.typed(b'P', body) + client.typed(b'S'))
            messages = client.read_until_ready(sock)
            states = [client.diagnostic_fields(value).get(b'C', b'').decode() for kind, value in messages if kind == b'E']
            good = states == [state] and not any(kind in (b'1', b'D', b'C') for kind, _ in messages)
            print('HAVING_BOOLEAN_PARSE', sql, states, 'PASS', good, flush=True)
            if not good:
                failures.append(('parse', sql, states, state))
        for index, (sql, oids) in enumerate([(f'SELECT count(*) FROM {table} HAVING TRUE', []),
                                              (f'SELECT count(*) FROM {table} HAVING $1', [16])]):
            checks += 1
            name = f'having_shape_{index}'.encode()
            body = name + b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(oids))
            body += b''.join(struct.pack('!I', oid) for oid in oids)
            sock.sendall(client.typed(b'P', body) + client.typed(b'D', b'S' + name + b'\0') + client.typed(b'S'))
            messages = client.read_until_ready(sock)
            types = [field[3] for field in client.row_description_fields(messages)]
            good = types == [20] and not any(kind in (b'E', b'D', b'C') for kind, _ in messages)
            print('HAVING_BOOLEAN_DESCRIBE', sql, types, 'PASS', good, flush=True)
            if not good:
                failures.append(('describe', sql, messages, types))
        query(f"SELECT currval('{sequence}');", rows=[], state='55000')
        for condition, passes in [('TRUE', True), ('FALSE', False), ('NULL', False),
                                  ("'t'", True), ("'f'", False), ("'yes'", True), ("'off'", False)]:
            query(f'SELECT count(*) AS n FROM {table} HAVING {condition};',
                  rows=[['3']] if passes else [], oids=[20])
            query(f'SELECT 1 AS n HAVING {condition};', rows=[['1']] if passes else [], oids=[23])
        query(f'SELECT count(*) FROM {table} WHERE FALSE HAVING TRUE;', rows=[['0']], oids=[20])
        query(f'SELECT count(*) FROM {table} HAVING count(*)>2;', rows=[['3']], oids=[20])
        query(f'SELECT count(*) FROM {table} HAVING count(*)>3;', rows=[], oids=[20])
        query(f'SELECT b FROM {table} GROUP BY b HAVING b;', rows=[['t']], oids=[16])
        query(f'SELECT h.b FROM {table} h GROUP BY h.b HAVING h.b;', rows=[['t']], oids=[16])
        query(f'SELECT id FROM {table} WHERE FALSE GROUP BY id HAVING id;', rows=[], state='42804')
        print('HAVING_BOOLEAN_CHECKS', checks, 'FAILURES', failures, flush=True)
        assert not failures, failures
    finally:
        try:
            client.simple_query(sock, 'ROLLBACK;')
            client.simple_query(sock, f'DROP TABLE IF EXISTS {table};')
            client.simple_query(sock, f'DROP SEQUENCE IF EXISTS {sequence};')
        finally:
            if server:
                runner.stop_ours(server)
            else:
                sock.close()


if __name__ == '__main__':
    main()
