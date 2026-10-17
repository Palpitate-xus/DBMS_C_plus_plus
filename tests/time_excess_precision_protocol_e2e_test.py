#!/usr/bin/env python3
"""Direct SELECT TIME precision clamps and warns once at Q/Parse, not per row.

Named SQL PREPARE, nested/other expression roles, DDL, other temporal types,
message-filter GUCs, and complete declaration analysis remain separate scopes.
"""
import importlib.util
from pathlib import Path
import socket
import struct
import sys

from time_binary_result_protocol_e2e_test import array, time as time_binary


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('time_excess_runner', root / 'tests/compat/pg_diff_runner.py')
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
    checks, failures = 0, []

    def warning(precision):
        return (b'WARNING', b'WARNING', b'22023',
                f'TIME({precision}) precision reduced to maximum allowed, 6'.encode())

    def diagnostics(messages):
        result = []
        for kind, body in messages:
            if kind == b'N':
                fields = client.diagnostic_fields(body)
                # Compare wire semantics, never forge PostgreSQL F/L/R internals.
                result.append(tuple(fields.get(key) for key in (b'S', b'V', b'C', b'M')))
        return result

    def report(good, label, detail):
        nonlocal checks
        checks += 1
        print('TIME_EXCESS_PRECISION', checks, label, 'match', good, repr(detail), flush=True)
        if not good:
            failures.append((label, detail))

    def exchange(packet):
        sock.sendall(packet + client.typed(b'S'))
        return client.read_until_ready(sock)

    def check(sql, mode, precision=6, requested=None, text=None, binary=None,
              is_array=False, rows=1, oid=None, size=None, state=None, name='v'):
        code = int(mode == 1)
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0))
            if mode == 'describe':
                packet += client.typed(b'D', b'S\0')
            else:
                packet += client.typed(b'B', b'\0\0' + struct.pack('!HHHH', 0, 0, 1, code))
                packet += client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0))
            messages = exchange(packet)
        notices = diagnostics(messages)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        values = client.data_row_values(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        expected_notices = [] if requested is None else [warning(requested)]
        good = messages[-1] == (b'Z', b'I') and notices == expected_notices
        if state:
            good = good and [item.get(b'C', b'').decode() for item in errors] == [state]
            good = good and not fields and not values and not tags and not any(kind in (b'1', b'2') for kind, _ in messages)
        else:
            expected_fields = [(name.encode(), 0, 0, oid or (1183 if is_array else 1083),
                                size if size is not None else (-1 if is_array else 8), precision, code)]
            good = good and not errors and fields == expected_fields
            if mode == 'describe':
                good = good and not values and not tags and [body for kind, body in messages if kind == b't'] == [b'\0\0']
            else:
                value = binary if code else None if text is None else text.encode()
                good = good and values == [[value] for _ in range(rows)] and tags == ['SELECT ' + str(rows)]
            if mode != 'simple':
                good = good and sum(kind == b'1' for kind, _ in messages) == 1
                good = good and sum(kind == b'2' for kind, _ in messages) == int(mode != 'describe')
            if expected_notices:
                kinds = [kind for kind, _ in messages]
                boundary = b'T' if mode == 'simple' else b'1'
                good = good and boundary in kinds and kinds.index(b'N') < kinds.index(boundary)
        report(good, str(mode) + ' ' + sql, (notices, errors, fields, values, tags))

    try:
        modes = ('simple', 'describe', 0, 1)
        for modifier, requested in (('7', 7), ('30', 30), ('2147483647', 2147483647),
                                    ('0x7', 7), ('0b111', 7), ('0o7', 7), ('3_0', 30)):
            target = 'TIME(' + modifier + ')'
            array_text = '{00:00:00.123457,NULL}'
            array_binary = array([time_binary(123457), None])
            forms = [
                ("SELECT CAST('00:00:00.1234567' AS " + target + ') AS v', '00:00:00.123457', time_binary(123457), False, 1),
                ("SELECT '00:00:00.1234567'::" + target + ' AS v', '00:00:00.123457', time_binary(123457), False, 1),
                ('SELECT ' + target + " '00:00:00.1234567' AS v", '00:00:00.123457', time_binary(123457), False, 1),
                ('SELECT CAST(NULL AS ' + target + ') AS v', None, None, False, 1),
                ('SELECT CAST(NULL AS ' + target + '[]) AS v', None, None, True, 1),
                ("SELECT CAST('00:00:00.1234567' AS " + target + ') AS v WHERE false', None, None, False, 0),
                ("SELECT CAST('{00:00:00.1234567,NULL}' AS " + target + '[]) AS v', array_text, array_binary, True, 1),
                ("SELECT CAST('[-3:-2]={00:00:00.1234567,NULL}' AS " + target + '[]) AS v',
                 '[-3:-2]=' + array_text, array([time_binary(123457), None], [(2, -3)]), True, 1),
            ]
            for sql, text, binary, is_array, rows in forms:
                for mode in modes:
                    check(sql, mode, requested=requested, text=text, binary=binary, is_array=is_array, rows=rows)
        for target in ('pg_catalog.time(7)', 'pg_catalog."time"(7)', 'pg_catalog._time(7)', 'TIME(7) WITHOUT TIME ZONE'):
            is_array = '_time' in target
            for mode in modes:
                check('SELECT CAST(NULL AS ' + target + ') AS v', mode, requested=7, is_array=is_array)
        for precision in (0, 3, 6):
            for mode in modes:
                check('SELECT CAST(NULL AS TIME(' + str(precision) + ')) AS v', mode, precision=precision)
        for mode in modes:
            check("SELECT 'TIME(7)' AS v", mode, precision=-1, text='TIME(7)', binary=b'TIME(7)', oid=25, size=-1)
            check('SELECT CAST(NULL AS TIME(6)) AS v /* TIME(7) */', mode)
        for modifier in ('-1', '+3', '6.5', '2147483648'):
            for mode in ('simple', 0):
                check('SELECT CAST(NULL AS TIME(' + modifier + ')) AS v', mode, state='42601')

        # Parse notices belong to this statement's declaration, not Describe,
        # Bind, each Execute, or every result row.
        statement = b'r127_notice_once\0'
        sql = 'SELECT CAST(NULL AS TIME(7)) AS v'
        messages = exchange(client.typed(b'P', statement + sql.encode() + b'\0' + struct.pack('!H', 0)))
        kinds = [kind for kind, _ in messages]
        report(messages[-1] == (b'Z', b'I') and diagnostics(messages) == [warning(7)] and
               kinds.count(b'1') == 1 and kinds.index(b'N') < kinds.index(b'1') and b'E' not in kinds,
               'named Parse warns once', messages)
        messages = exchange(client.typed(b'D', b'S' + statement))
        report(messages[-1] == (b'Z', b'I') and not diagnostics(messages) and
               client.row_description_fields(messages) == [(b'v', 0, 0, 1083, 8, 6, 0)] and
               not any(kind == b'E' for kind, _ in messages), 'named Describe no duplicate', messages)
        for _ in range(2):
            messages = exchange(client.typed(b'B', b'\0' + statement + struct.pack('!HHHH', 0, 0, 1, 0)) +
                                client.typed(b'E', b'\0' + struct.pack('!I', 0)))
            report(messages[-1] == (b'Z', b'I') and not diagnostics(messages) and
                   client.data_row_values(messages) == [[None]] and
                   [body for kind, body in messages if kind == b'C'] == [b'SELECT 1\0'] and
                   not any(kind == b'E' for kind, _ in messages), 'named Bind/Execute no duplicate', messages)

        for setup in ('CREATE TEMP TABLE excessive_time_precision_rows(n integer)',
                      'INSERT INTO excessive_time_precision_rows VALUES(1),(2)'):
            assert runner.decode_wire_result(client.simple_query(sock, setup))[1] is None
        for mode in modes:
            check("SELECT CAST('00:00:00.1234567' AS TIME(7)) AS v FROM excessive_time_precision_rows",
                  mode, requested=7, text='00:00:00.123457', binary=time_binary(123457), rows=2)
        print('TIME_EXCESS_PRECISION_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 276, checks
        assert not failures, failures
        print('[TIME EXCESS PRECISION] all 276 controls passed', flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()


if __name__ == '__main__':
    main()
