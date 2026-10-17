#!/usr/bin/env python3
"""Valid TIME(0..6) clock conversion and its real declaration metadata.

Does not claim invalid-typmod grammar/warnings, constant-analysis timing,
all common-expression typmods, binary Bind input, or fractional heap storage.
"""
import importlib.util
from pathlib import Path
import socket
import struct
import sys

from time_binary_result_protocol_e2e_test import array, time as time_binary
from time_input_normalization_protocol_e2e_test import clock_text


def cases():
    # Expected integer clocks are literal vectors, not the product's parser
    # or a second implementation of precision rounding.
    return [
        ('12:34:56.654321', (45297000000, 45296700000, 45296650000, 45296654000, 45296654300, 45296654320, 45296654321)),
        ('00:00:00.500000', (1000000, 500000, 500000, 500000, 500000, 500000, 500000)),
        ('00:00:00.0004996', (0, 0, 0, 1000, 500, 500, 500)),
        ('00:00:00.1234565', (0, 100000, 120000, 123000, 123500, 123460, 123456)),
        ('23:59:59.999999', (86400000000, 86400000000, 86400000000, 86400000000, 86400000000, 86400000000, 86399999999)),
        ('23:59:59.9999995', (86400000000,) * 7),
        ('0:0:0', (0,) * 7),
        ('12:34:56+08:30', (45296000000,) * 7),
    ]


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('time_precision_runner', root / 'tests/compat/pg_diff_runner.py')
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

    def setup(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def check(sql, precision, text=None, binary=None, is_array=False, mode=0,
              empty=False, state=None, parameter=None, name='v'):
        nonlocal checks
        checks += 1
        code = int(mode == 1)
        params = [] if parameter is None else [parameter]
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
        else:
            parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(params))
            parse += b''.join(struct.pack('!I', oid) for oid, _ in params)
            packet = client.typed(b'P', parse)
            if mode == 'describe':
                packet += client.typed(b'D', b'S\0')
            else:
                bind = b'\0\0' + struct.pack('!HH', 0, len(params))
                for _, value in params:
                    raw = None if value is None else value.encode()
                    bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
                bind += struct.pack('!HH', 1, code)
                packet += client.typed(b'B', bind) + client.typed(b'D', b'P\0')
                packet += client.typed(b'E', b'\0' + struct.pack('!I', 0))
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        good = messages[-1] == (b'Z', b'I')
        if state:
            # Selected value conversion/SQLSTATE only: R122 separately owns
            # the remaining analysis-stage and invalid-typmod defects.
            good = good and [error.get(b'C', b'').decode() for error in errors] == [state] and not rows and not tags
        else:
            wanted = [(name.encode(), 0, 0, 1183 if is_array else 1083, -1 if is_array else 8, precision, code)]
            good = good and not errors and fields == wanted
            if mode == 'describe':
                desired_params = struct.pack('!H', len(params)) + b''.join(struct.pack('!I', oid) for oid, _ in params)
                good = good and not rows and not tags and [body for kind, body in messages if kind == b't'] == [desired_params]
            else:
                value = binary if code else None if text is None else text.encode()
                good = good and rows == ([] if empty else [[value]]) and tags == ['SELECT ' + ('0' if empty else '1')]
            if mode != 'simple':
                good = good and sum(kind == b'1' for kind, _ in messages) == 1
                if mode != 'describe':
                    good = good and sum(kind == b'2' for kind, _ in messages) == 1
        print('TIME_PRECISION_VALUE', checks, mode, sql, 'match', good, 'fields', fields, 'errors', errors, flush=True)
        if not good:
            failures.append((sql, mode, fields, rows, tags, errors, precision, state))

    try:
        setup("SET TIME ZONE 'UTC'")
        for value, expected in cases():
            for precision, micros in enumerate(expected):
                target = 'time(' + str(precision) + ')'
                for sql in ("SELECT CAST('" + value + "' AS " + target + ') AS v',
                            "SELECT '" + value + "'::" + target + ' AS v',
                            'SELECT ' + target + " '" + value + "' AS v"):
                    for mode in (0, 1):
                        check(sql, precision, clock_text(micros), time_binary(micros), mode=mode)
                for mode in (0, 1):
                    check("SELECT CAST('{" + value + ",NULL}' AS " + target + '[]) AS v', precision,
                          '{' + clock_text(micros) + ',NULL}', array([time_binary(micros), None]), True, mode)
        for precision in range(7):
            target = 'time(' + str(precision) + ')'
            micros = cases()[0][1][precision]
            text, binary = clock_text(micros), time_binary(micros)
            for mode in (0, 1):
                check('SELECT CAST(NULL AS ' + target + ') AS v', precision, mode=mode)
                check('SELECT CAST(NULL AS ' + target + '[]) AS v', precision, is_array=True, mode=mode)
                check("SELECT CAST('12:34:56.654321' AS " + target + ') AS v WHERE false', precision, mode=mode, empty=True)
                check("SELECT CAST('{12:34:56.654321,NULL}' AS " + target + '[]) AS v WHERE false', precision, is_array=True, mode=mode, empty=True)
            for spelling, is_array in [('pg_catalog.time(' + str(precision) + ')', False),
                    ('pg_catalog."time"(' + str(precision) + ')', False),
                    ('pg_catalog._time(' + str(precision) + ')', True), (target + ' without time zone', False)]:
                for mode in (0, 1):
                    check("SELECT CAST('" + ('{12:34:56.654321,NULL}' if is_array else '12:34:56.654321') + "' AS " + spelling + ') AS v',
                          precision, '{' + text + ',NULL}' if is_array else text,
                          array([binary, None]) if is_array else binary, is_array, mode)
            for is_array in (False, True):
                sql = 'SELECT CAST($1 AS ' + target + ('[]' if is_array else '') + ') AS v'
                parameter = (1183, '{12:34:56.654321,NULL}') if is_array else (1083, '12:34:56.654321')
                for mode in (0, 1):
                    check(sql, precision, '{' + text + ',NULL}' if is_array else text,
                          array([binary, None]) if is_array else binary, is_array, mode, parameter=parameter)
                    check(sql, precision, is_array=is_array, mode=mode, parameter=(parameter[0], None))
                check(sql, precision, is_array=is_array, mode='describe', parameter=(parameter[0], None))
            shapes = [('[-3:-2]={12:34:56.654321,NULL}', '[-3:-2]={' + text + ',NULL}', [binary, None], [(2, -3)]),
                      ('{{12:34:56.654321,NULL},{0:0:0,24:00:00}}', '{{' + text + ',NULL},{00:00:00,24:00:00}}',
                       [binary, None, time_binary(0), time_binary(86400000000)], [(2, 1), (2, 1)]),
                      ('{{{{{{12:34:56.654321}}}}}}', '{{{{{{' + text + '}}}}}}', [binary], [(1, 1)] * 6)]
            for value, expected, values, dimensions in shapes:
                for mode in (0, 1):
                    check("SELECT CAST('" + value + "' AS " + target + '[]) AS v', precision, expected,
                          array(values, dimensions), True, mode)
            for mode in (0, 1):
                value = cases()[1][1][precision]
                check("SELECT CAST(TIME '00:00:00.500000' AS " + target + ') AS v', precision,
                      clock_text(value), time_binary(value), mode=mode)
            for sql, is_array, empty, value, raw in [
                    ("SELECT CAST('12:34:56.654321' AS " + target + ') AS v', False, False, text, binary),
                    ("SELECT '12:34:56.654321'::" + target + ' AS v', False, False, text, binary),
                    ('SELECT ' + target + " '12:34:56.654321' AS v", False, False, text, binary),
                    ("SELECT CAST('{12:34:56.654321,NULL}' AS " + target + '[]) AS v', True, False, '{' + text + ',NULL}', array([binary, None])),
                    ('SELECT CAST(NULL AS ' + target + ') AS v', False, False, None, None),
                    ('SELECT CAST(NULL AS ' + target + '[]) AS v', True, False, None, None),
                    ("SELECT CAST('12:34:56.654321' AS " + target + ') AS v WHERE false', False, True, None, None),
                    ("SELECT CAST('{12:34:56.654321,NULL}' AS " + target + '[]) AS v WHERE false', True, True, None, None)]:
                check(sql, precision, value, raw, is_array, 'simple', empty)
        for value, state in [('bad', '22007'), ('25:00:00', '22008'), ('12:60:00', '22008'), ('12:00:61', '22008'),
                             ('-01:00:00', '22007'), ('infinity', '22007'), ('24:00:00.000001', '22008')]:
            for precision in (0, 3, 6):
                for is_array in (False, True):
                    for mode in (0, 1):
                        check("SELECT CAST('" + ('{' + value + ',NULL}' if is_array else value) + "' AS time(" + str(precision) + ')' +
                              ('[]' if is_array else '') + ') AS v', precision, is_array=is_array, mode=mode, state=state)
        setup('CREATE TEMP TABLE time_precision_alias(t TIME(6),a TIME(1)[])')
        setup('INSERT INTO time_precision_alias VALUES(NULL,NULL)')
        for mode in (0, 1):
            check("SELECT CAST('00:00:00.5' AS TIME(0)) AS t FROM time_precision_alias", 0, '00:00:01', time_binary(1000000), mode=mode, name='t')
            check("SELECT CAST('{00:00:00.5,NULL}' AS TIME(0)[]) AS a FROM time_precision_alias", 0, '{00:00:01,NULL}',
                  array([time_binary(1000000), None]), True, mode, name='a')
        print('TIME_PRECISION_VALUE_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 830, checks
        assert not failures, failures
        print('[TIME PRECISION VALUE] all 830 controls passed', flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close()  # All reference objects are session-owned TEMP objects.


if __name__ == '__main__':
    main()
