#!/usr/bin/env python3
"""Pure extended inputs retain values/types without a foreign DDL lock."""
import importlib.util
import socket
import struct
import sys
import uuid
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('independent_input_runner', root / 'tests/compat/pg_diff_runner.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    client = runner.load_protocol_client()
    reference = sys.argv[1:] == ['--reference18']
    assert not sys.argv[1:] or reference
    server, second, created = None, None, False
    if reference:
        host, port, user, database, password = runner._reference_connection_settings()
        first = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(first, user, database, password=password)
        runner.verify_reference_version(client, first)
        second = socket.create_connection((host, port), timeout=runner.wire_timeout())
        client.startup_reference(second, user, database, password=password)
    else:
        server = runner.start_ours(client)
        first = server['sock']
        second = socket.create_connection(('127.0.0.1', server['port']), timeout=runner.wire_timeout())
        client.startup(second, 'alice', 'info')
    schema = 'independent_input_' + uuid.uuid4().hex[:12]
    checks, failures = 0, []

    def check(sock, sql, rows=None, state=None, oid=None, extended=False, ready=b'I'):
        nonlocal checks
        if extended:
            name = ('input_s' + str(checks)).encode()
            portal = ('input_p' + str(checks)).encode()
            sock.sendall(client.typed(b'P', name + b'\0' + sql.encode() + b'\0\0\0') +
                         client.typed(b'B', portal + b'\0' + name + b'\0' + struct.pack('!HHH', 0, 0, 0)) +
                         client.typed(b'D', b'P' + portal + b'\0') +
                         client.typed(b'E', portal + b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        else:
            messages = client.simple_query(sock, sql)
        result = runner.decode_wire_result(messages, include_types=True)
        valid = result[1] == state and messages[-1] == (b'Z', ready)
        if rows is not None:
            valid = valid and result[0] == rows
        if oid is not None:
            valid = valid and result[5] == [oid]
        if state:
            valid = valid and not result[0] and result[4] is None
        checks += 1
        print('DATABASE_INDEPENDENT_INPUT', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, rows, state, oid, messages[-1]))
        return valid

    try:
        assert check(first, 'CREATE SCHEMA ' + schema)
        created = True
        assert check(first, 'CREATE TABLE ' + schema + '.rows(id INT)')
        assert check(first, 'BEGIN', ready=b'T')
        assert check(first, 'ALTER TABLE ' + schema + '.rows ADD COLUMN v INT', ready=b'T')
        assert check(second, 'SET lock_timeout=50')
        for sql, rows, oid in (
                ('SELECT 1', [['1']], 23), ('SELECT 1+2*3', [['7']], 23),
                ("SELECT 'a'||'b'", [['ab']], 25), ('SELECT 1=1 AND 2<3', [['t']], 16),
                ('SELECT 1 WHERE false', [], 23), ('SELECT 1 LIMIT 0', [], 23),
                ('SELECT DISTINCT 1 ORDER BY 1 OFFSET 1', [], 23), ('SELECT NULL', [[None]], 25),
                ('SELECT NULL+1', [[None]], 23), ('SELECT 0.1+0.2', [['0.3']], 1700),
                ('SELECT NOT false', [['t']], 16), ("SELECT B'101'", [['101']], 1560),
                ("SELECT 1+'2'", [['3']], 23), ("SELECT 1='1'", [['t']], 16),
                ('SELECT NULL=1', [[None]], 16), ("SELECT 1 IN('1',2)", [['t']], 16),
                ('SELECT 1.5+NULL', [[None]], 1700), ("SELECT NULL||'x'", [[None]], 25),
                ("SELECT B'101'=B'101'", [['t']], 16), ("SELECT 'foo'||NULL", [[None]], 25)):
            check(second, sql, rows=rows, oid=oid, extended=True)
        check(second, "SELECT 1='bad'", state='22P02', extended=True)
        check(second, "SELECT 1='2147483648'", state='22003', extended=True)
        check(second, 'SELECT id FROM ' + schema + '.rows', state='55P03', extended=True)
        check(second, 'SELECT 7', rows=[['7']], oid=23, extended=True)
        print('DATABASE_INDEPENDENT_INPUT_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 29 and not failures, failures
    finally:
        if second:
            second.close()
        try:
            client.simple_query(first, 'ROLLBACK')
            if created:
                client.simple_query(first, 'DROP SCHEMA ' + schema + ' CASCADE')
        finally:
            if server:
                runner.stop_ours(server)
            else:
                first.close()


if __name__ == '__main__':
    main()
