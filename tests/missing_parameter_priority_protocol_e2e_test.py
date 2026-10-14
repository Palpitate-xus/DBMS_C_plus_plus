#!/usr/bin/env python3
"""Bind actual simple-query parameters before legacy result-type inference."""
import importlib.util
import socket
import struct
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('missing_parameter_runner', root / 'tests/compat/pg_diff_runner.py')
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
    checks, failures = 0, []

    def check(sql, state=None, rows=None):
        nonlocal checks
        result = runner.decode_wire_result(client.simple_query(sock, sql), include_types=True)
        valid = result[1] == state and (rows is None or result[0] == rows)
        if state:
            valid = valid and result[0] == [] and result[4] is None
        checks += 1
        print('MISSING_PARAMETER', sql, result, 'PASS', valid, flush=True)
        if not valid:
            failures.append((sql, result, state, rows))
        return valid

    try:
        assert check('CREATE TEMP TABLE missing_parameter_rows(v INT)')
        for populated in (False, True):
            if populated:
                assert check('INSERT INTO missing_parameter_rows VALUES(1),(NULL),(2)')
            for expression in ('sum($1)+1', 'sum(CAST($1 AS INT))+1', 'min($1)+1',
                               'count($1)+1', 'sum($2)+1', 'sum(v)+$1',
                               'CASE WHEN false THEN sum($1)+1 ELSE 0 END'):
                for tail in ('', ' WHERE false', ' LIMIT 0'):
                    check('SELECT ' + expression + ' FROM missing_parameter_rows' + tail, state='42P02')
        check('SELECT sum($1)+1 FROM missing_parameter_absent', state='42P01')
        check('SELECT sum(missing)+$1 FROM missing_parameter_rows', state='42703')
        check('SELECT $0 FROM missing_parameter_rows', state='42P02')
        check("SELECT '$1' AS value", rows=[['$1']])
        check('SELECT $$sum($1)+1$$ AS value', rows=[['sum($1)+1']])
        check('SELECT sum(v)+1 FROM missing_parameter_rows /* $1 */', rows=[['4']])
        assert check('PREPARE missing_parameter_valid(INT) AS SELECT sum($1)+1 FROM missing_parameter_rows')
        check('EXECUTE missing_parameter_valid(2)', rows=[['7']])
        check('DEALLOCATE missing_parameter_valid')
        sql = 'SELECT sum($1)+1 AS value FROM missing_parameter_rows'
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!HI', 1, 23)
        value = b'2'
        bind = b'\0\0' + struct.pack('!HHI', 0, 1, len(value)) + value + struct.pack('!H', 0)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
                     client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        result = runner.decode_wire_result(messages, include_types=True)
        checks += 1
        valid = result[0] == [['7']] and result[1] is None and result[5] == [20]
        print('MISSING_PARAMETER_EXTENDED_VALID', result, 'PASS', valid, flush=True)
        if not valid:
            failures.append(('extended valid', result))
        print('MISSING_PARAMETER_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 54 and not failures, failures
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
