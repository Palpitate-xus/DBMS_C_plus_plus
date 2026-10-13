#!/usr/bin/env python3
"""ON CONFLICT WHERE is prepared and evaluated as typed BOOLEAN."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('conflict_boolean_runner', root / 'tests/compat/pg_diff_runner.py')
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
    table = 'conflict_boolean_' + uuid.uuid4().hex[:12]
    sequence = table + '_seq'
    failures = []
    checks = 0

    def query(sql, rows=None, oids=None, state=None, tag=None, ready=b'I'):
        nonlocal checks
        checks += 1
        messages = client.simple_query(sock, sql)
        actual = runner.decode_wire_result(messages, include_types=True)
        statuses = [body for kind, body in messages if kind == b'Z']
        good = actual[1] == state and statuses == [ready]
        if rows is not None:
            good = good and actual[0] == rows
        if oids is not None:
            good = good and actual[5] == oids
        if tag is not None:
            good = good and actual[4] == tag
        print('CONFLICT_BOOLEAN', sql, actual, 'PASS', good, flush=True)
        if not good:
            failures.append((sql, actual, statuses, rows, oids, state, tag, ready))

    def upsert(input_sql, condition, returning=True):
        return (f'INSERT INTO {table}(id,v) {input_sql} ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE {condition}'
                + (' RETURNING id,v' if returning else '') + ';')

    try:
        query(f'CREATE TABLE {table}(id INT PRIMARY KEY,v INT);')
        query(f'INSERT INTO {table} VALUES(1,10);')
        query(f'CREATE SEQUENCE {sequence};')
        for condition, state in [('1', '42804'), ('NULL::INT', '42804'),
                                 ("'notboolean'", '22P02'), ("'f'::TEXT", '42804'),
                                 ('NULL::TEXT', '42804')]:
            for source in ['VALUES(1,99)', 'VALUES(5,99)',
                           f"SELECT nextval('{sequence}')::INT,99 WHERE FALSE"]:
                query(upsert(source, condition), rows=[], state=state)
            query(f'SELECT id,v FROM {table} ORDER BY id;', rows=[['1', '10']], oids=[23, 23])
        for index, (condition, state) in enumerate([('1', '42804'), ("'notboolean'", '22P02'), ('NULL::INT', '42804')]):
            checks += 1
            sql = upsert('VALUES(1,99)', condition)
            body = f'conflict_bad_{index}'.encode() + b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
            sock.sendall(client.typed(b'P', body) + client.typed(b'S'))
            messages = client.read_until_ready(sock)
            states = [client.diagnostic_fields(value).get(b'C', b'').decode() for kind, value in messages if kind == b'E']
            good = states == [state] and not any(kind in (b'1', b'D', b'C') for kind, _ in messages)
            print('CONFLICT_BOOLEAN_PARSE', sql, states, 'PASS', good, flush=True)
            if not good:
                failures.append(('parse', sql, states, state))
        query(f"SELECT currval('{sequence}');", rows=[], state='55000')
        for condition, passes in [("'f'", False), ("'off'", False), ('FALSE', False),
                                  ('NULL', False), ('NULL::BOOL', False),
                                  ("'t'", True), ("'yes'", True), ('TRUE', True),
                                  (f'excluded.v>{table}.v', True),
                                  (f'(SELECT excluded.v>{table}.v)', True),
                                  ('(SELECT FALSE)', False)]:
            query(f'UPDATE {table} SET v=10 WHERE id=1;')
            query(upsert('VALUES(1,99)', condition), rows=[['1', '99']] if passes else [],
                  oids=[23, 23], tag='INSERT 0 1' if passes else 'INSERT 0 0')
            query(f'SELECT id,v FROM {table} ORDER BY id;', rows=[['1', '99' if passes else '10']], oids=[23, 23])
        query(upsert('VALUES(2,20)', f"nextval('{sequence}')>0"), rows=[['2', '20']], oids=[23, 23], tag='INSERT 0 1')
        query(f"SELECT currval('{sequence}');", rows=[], state='55000')
        query(upsert('VALUES(2,21)', f"nextval('{sequence}')>0"), rows=[['2', '21']], oids=[23, 23], tag='INSERT 0 1')
        query(f"SELECT currval('{sequence}');", rows=[['1']], oids=[20])
        query(f'PREPARE conflict_good(INT,INT) AS {upsert("VALUES($1,$2)", "\'f\'")}')
        query('EXECUTE conflict_good(1,888);', rows=[], oids=[23, 23], tag='INSERT 0 0')
        query('DEALLOCATE conflict_good;')
        query('BEGIN;', ready=b'T')
        query(f'INSERT INTO {table} VALUES(3,30);', ready=b'T')
        query('SAVEPOINT parent_work;', ready=b'T')
        query(upsert('VALUES(1,999)', '1'), rows=[], state='42804', ready=b'E')
        query('ROLLBACK TO parent_work;', ready=b'T')
        query(f'SELECT id,v FROM {table} ORDER BY id;', rows=[['1', '10'], ['2', '21'], ['3', '30']], oids=[23, 23], ready=b'T')
        query('RELEASE parent_work;', ready=b'T')
        query('COMMIT;')
        print('CONFLICT_BOOLEAN_CHECKS', checks, 'FAILURES', failures, flush=True)
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
