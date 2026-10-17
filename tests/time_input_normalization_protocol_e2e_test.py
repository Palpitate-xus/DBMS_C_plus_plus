#!/usr/bin/env python3
"""Default TIME input validates text and normalizes its microsecond clock."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys

from time_binary_result_protocol_e2e_test import cases


def clock_text(micros):
    seconds, fraction = divmod(micros, 1000000)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f'{hours:02d}:{minutes:02d}:{seconds:02d}' + (f'.{fraction:06d}'.rstrip('0') if fraction else '')


def array_text(values):
    return '{' + ','.join('NULL' if value is None else value for value in values) + '}'


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('time_input_runner', root/'tests/compat/pg_diff_runner.py')
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
    controls, failures = 0, []

    def setup(sql):
        result = runner.decode_wire_result(client.simple_query(sock, sql))
        assert result[1] is None, (sql, result)

    def check(sql, oids, values, outputs, expected, tag='SELECT 1', names=None,
              state=None, describe=False):
        nonlocal controls
        controls += 1
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(oids))
        parse += b''.join(struct.pack('!I', oid) for oid in oids)
        packet = client.typed(b'P', parse)
        if describe:
            packet += client.typed(b'D', b'S\0')
        else:
            bind = b'\0\0' + struct.pack('!HH', 0, len(values))
            for value in values:
                raw = None if value is None else value.encode()
                bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
            bind += struct.pack('!HH', 1, 0)
            packet += client.typed(b'B', bind) + client.typed(b'D', b'P\0') + client.typed(b'E', b'\0\0\0\0\0')
        sock.sendall(packet + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        good = messages[-1] == (b'Z', b'I')
        if state is not None:
            good = good and [error.get(b'C', b'').decode() for error in errors] == [state] and not rows and not tags
            if oids:
                # Declared text input fails at Bind, before portal description.
                good = good and not fields and not any(kind == b'2' for kind, _ in messages)
        else:
            desired = [(oid, 8 if oid == 1083 else -1, -1, 0) for oid in outputs]
            names = ['v'] if names is None else names
            good = good and not errors and rows == expected and [(f[3], f[4], f[5], f[6]) for f in fields] == desired
            good = good and [f[0] for f in fields] == [name.encode() for name in names]
            if describe:
                wanted = struct.pack('!H', len(oids)) + b''.join(struct.pack('!I', oid) for oid in oids)
                good = good and not tags and [body for kind, body in messages if kind == b't'] == [wanted]
            else:
                good = good and tags == [tag] and sum(kind == b'1' for kind, _ in messages) == 1 and sum(kind == b'2' for kind, _ in messages) == 1
        print('TIME_INPUT_NORMALIZATION', controls, sql, oids, 'match', good, 'errors', errors, flush=True)
        if not good:
            failures.append((sql, oids, rows, expected, fields, tags, errors, state))

    def four_paths(value, expected=None, state=None):
        row = [] if state else [[expected.encode()]]
        array_row = [] if state else [[array_text([expected, None]).encode()]]
        check('SELECT $1 AS v', [1083], [value], [1083], row, state=state)
        check('SELECT $1 AS v', [1183], ['{"' + value + '",NULL}'], [1183], array_row, state=state)
        check("SELECT TIME '" + value + "' AS v", [], [], [1083], row, state=state)
        check("SELECT ARRAY[TIME '" + value + "',NULL::time] AS v", [], [], [1183], array_row, state=state)

    try:
        # Errors are intentional; reference queries stay in autocommit.
        setup("SET TIME ZONE 'UTC'")
        assert len(cases()) == 30
        for value, micros in cases():
            four_paths(value, clock_text(micros))
        four_paths('12:34:56+08', '12:34:56')
        four_paths('12:34:56.000001+08:30', '12:34:56.000001')
        for value, state in [('bad', '22007'), ('25:00:00', '22008'), ('12:60:00', '22008'),
                             ('12:00:61', '22008'), ('-01:00:00', '22007'), ('infinity', '22007'),
                             ('24:00:00.000001', '22008')]:
            four_paths(value, state=state)
        for oid, declaration in [(1083, 'time'), (1183, 'time[]')]:
            check('SELECT $1 AS v', [oid], [None], [oid], [[None]])
            check('SELECT NULL::' + declaration + ' AS v', [], [], [oid], [[None]])
        check('SELECT $1 AS v', [1183], ['{}'], [1183], [[b'{}']])
        check('SELECT $1 AS v', [1183], ['{NULL,NULL}'], [1183], [[b'{NULL,NULL}']])
        check('SELECT $1 AS v', [1183], ['[-3:-1]={0:0:0,NULL,12:34:56.9999995}'], [1183], [[b'[-3:-1]={00:00:00,NULL,12:34:57}']])
        check('SELECT $1 AS v', [1183], ['{{0:0:0,NULL},{24:00:00,12:34:56.0000015}}'], [1183], [[b'{{00:00:00,NULL},{24:00:00,12:34:56.000002}}']])
        check('SELECT $1 AS v', [1183], ['{{{{{{0:0:0}}}}}}'], [1183], [[b'{{{{{{00:00:00}}}}}}']])
        check("SELECT CAST(' 12:34 ' AS time) AS v", [], [], [1083], [[b'12:34:00']])
        check("SELECT '0:0:0'::time without time zone AS v", [], [], [1083], [[b'00:00:00']])
        check('SELECT $1 AS s,$2 AS a WHERE false', [1083, 1183], ['12:34', '{0:0:0}'], [1083, 1183], [], 'SELECT 0', names=['s', 'a'])
        for oid in (1083, 1183):
            check('SELECT $1 AS v', [oid], [], [oid], [], describe=True)
        check('SELECT $1 AS s,$2 AS a', [1083, 1183], ['12:34:56.0000015', '{12:34:56.9999995,NULL}'],
              [1083, 1183], [[b'12:34:56.000002', b'{12:34:57,NULL}']], names=['s', 'a'])
        setup('CREATE TEMP TABLE time_input_rows(id INTEGER PRIMARY KEY,t TIME,a TIME[])')
        values = [[b'12:34:00', b'{12:34:57,NULL}']]
        check('INSERT INTO time_input_rows VALUES(1,$1,$2) RETURNING t,a', [1083, 1183], ['12:34', '{12:34:56.9999995,NULL}'],
              [1083, 1183], values, 'INSERT 0 1', names=['t', 'a'])
        check('SELECT t,a FROM time_input_rows WHERE id=1', [], [], [1083, 1183], values, names=['t', 'a'])
        check('INSERT INTO time_input_rows VALUES(2,NULL,NULL) RETURNING t,a', [], [], [1083, 1183], [[None, None]], 'INSERT 0 1', names=['t', 'a'])
        check('SELECT t,a FROM time_input_rows WHERE false', [], [], [1083, 1183], [], 'SELECT 0', names=['t', 'a'])
        check('SELECT $1 AS v WHERE false', [1083], ['bad'], [1083], [], state='22007')
        check('SELECT $1 AS v WHERE false', [1183], ['{bad}'], [1183], [], state='22007')
        print('TIME_INPUT_NORMALIZATION_COMPLETE', controls, 'FAILURES', len(failures), flush=True)
        assert controls == 177, controls
        assert not failures, failures
        print('[TIME INPUT NORMALIZATION] all 177 controls passed', flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            sock.close() # TEMP table is session-owned; no lasting reference objects.


if __name__ == '__main__':
    main()
