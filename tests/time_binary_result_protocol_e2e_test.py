#!/usr/bin/env python3
"""TIME binary output is a bounded int64 microsecond clock, including 24h."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def cases():
    return [
        ('00:00:00', 0), ('00:00:00.000001', 1), ('00:00:00.0000015', 2),
        ('00:00:00.9999995', 1000000), ('00:00:59.9999995', 60000000),
        ('00:59:59.9999995', 3600000000), ('23:59:59.9999995', 86400000000),
        ('12:34:56', 45296000000), ('12:34:56.123456', 45296123456),
        ('12:34:56.0000015', 45296000002), ('12:34:56.9999995', 45297000000),
        ('01:02:03.1', 3723100000), ('01:02:03.000001', 3723000001),
        ('23:59:59.999999', 86399999999), ('24:00:00', 86400000000),
        ('24:00:00.0000001', 86400000000), ('00:00:00.0000004', 0),
        ('00:00:00.0000005', 0), ('00:00:00.0000025', 2), ('00:00:00.0000035', 4),
        ('00:00:00.1234564', 123456), ('00:00:00.1234565', 123456),
        ('00:00:00.1234575', 123458), ('03:04:05.000006', 11045000006),
        ('0:0:0', 0), ('12:34', 45240000000), ('24:00', 86400000000),
        ('23:59:60', 86400000000), ('12:34:60', 45300000000),
        (' 12:34:56.123456 ', 45296123456),
    ]


def time(micros):
    return struct.pack('!q', micros)


def array(values, dimensions=None):
    if dimensions is None:
        dimensions = [(len(values), 1)] if values else []
    result = struct.pack('!iii', len(dimensions), int(None in values), 1083)
    for length, lower in dimensions:
        result += struct.pack('!ii', length, lower)
    for value in values:
        result += struct.pack('!i', -1 if value is None else len(value)) + (value or b'')
    return result


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('time_binary_runner', root/'tests/compat/pg_diff_runner.py')
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

    def parse(sql, oids):
        return b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(oids)) + b''.join(struct.pack('!I', oid) for oid in oids)

    def check(sql, oids, values, outputs, expected, tag='SELECT 1', formats=None, describe=False):
        nonlocal controls
        controls += 1
        formats = ([0] * len(outputs) if describe else [1] * len(outputs)) if formats is None else formats
        packet = client.typed(b'P', parse(sql, oids))
        if describe:
            packet += client.typed(b'D', b'S\0')
        else:
            bind = b'\0\0' + struct.pack('!HH', 0, len(values))
            for value in values:
                raw = None if value is None else value.encode()
                bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
            bind += struct.pack('!H', len(formats)) + b''.join(struct.pack('!H', code) for code in formats)
            packet += client.typed(b'B', bind) + client.typed(b'D', b'P\0') + client.typed(b'E', b'\0\0\0\0\0')
        sock.sendall(packet + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        metadata = [(f[3], f[4], f[5], f[6]) for f in fields]
        desired = [(oid, 8 if oid == 1083 else -1, -1, code) for oid, code in zip(outputs, formats)]
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        good = not errors and metadata == desired and rows == expected and messages[-1][0] == b'Z'
        if describe:
            parameters = [body for kind, body in messages if kind == b't']
            wanted = struct.pack('!H', len(oids)) + b''.join(struct.pack('!I', oid) for oid in oids)
            good = good and not tags and parameters == [wanted]
        else:
            good = good and tags == [tag] and sum(k == b'1' for k, _ in messages) == 1 and sum(k == b'2' for k, _ in messages) == 1
        print('TIME_BINARY_RESULT', controls, sql, oids, 'match', good, 'errors', errors, flush=True)
        if not good:
            failures.append((sql, oids, rows, expected, metadata, desired, tags, errors))

    try:
        if reference:
            setup('BEGIN')
            schema = 'time_binary_' + uuid.uuid4().hex[:12]
            setup('CREATE SCHEMA ' + schema)
            setup('SET search_path TO ' + schema)
        assert len(cases()) == 30
        for value, micros in cases():
            payload = time(micros)
            check('SELECT $1 AS v', [1083], [value], [1083], [[payload]])
            check('SELECT $1 AS v', [1183], ['{"' + value + '",NULL}'], [1183], [[array([payload, None])]])
            check("SELECT TIME '" + value + "' AS v", [], [], [1083], [[payload]])
            check("SELECT ARRAY[TIME '" + value + "',NULL::time] AS v", [], [], [1183], [[array([payload, None])]])
        for oid, declaration in [(1083, 'time'), (1183, 'time[]')]:
            check('SELECT $1 AS v', [oid], [None], [oid], [[None]])
            check('SELECT NULL::' + declaration + ' AS v', [], [], [oid], [[None]])
        check('SELECT $1 AS v', [1183], ['{}'], [1183], [[array([])]])
        check('SELECT $1 AS v', [1183], ['{NULL,NULL}'], [1183], [[array([None, None])]])
        check('SELECT $1 AS v', [1183], ['[-3:-1]={00:00:00.000001,NULL,24:00:00}'], [1183], [[array([time(1), None, time(86400000000)], [(3, -3)])]])
        check('SELECT $1 AS v', [1183], ['{{00:00:00.000001,NULL},{24:00:00,12:34:56.123456}}'], [1183], [[array([time(1), None, time(86400000000), time(45296123456)], [(2, 1), (2, 1)])]])
        check('SELECT $1 AS v', [1183], ['{{{{{{00:00:00.000001}}}}}}'], [1183], [[array([time(1)], [(1, 1)] * 6)]])
        check('SELECT $1 AS s,$2 AS a WHERE false', [1083, 1183], ['00:00:00.000001', '{24:00:00}'], [1083, 1183], [], 'SELECT 0')
        for value in ['00:00:00', '00:00:00.000001', '12:34:56.123456', '24:00:00']:
            check('SELECT $1 AS v', [1083], [value], [1083], [[value.encode()]], formats=[0])
        check('SELECT $1 AS v', [1183], ['{00:00:00.000001,NULL}'], [1183], [[b'{00:00:00.000001,NULL}']], formats=[0])
        for sql, oids, outputs in [('SELECT $1 AS v', [1083], [1083]), ('SELECT $1 AS v', [1183], [1183]),
                                    ('SELECT $1 AS s,$2 AS a', [1083, 1183], [1083, 1183]), ('SELECT NULL::time AS v', [], [1083])]:
            check(sql, oids, [], outputs, [], describe=True)
        check('SELECT $1 AS s,$2 AS a', [1083, 1183], ['00:00:00.000001', '{24:00:00,NULL}'], [1083, 1183], [[b'00:00:00.000001', array([time(86400000000), None])]], formats=[0, 1])
        setup('CREATE TABLE time_binary_rows(id INTEGER PRIMARY KEY,t TIME,a TIME[])')
        expected = [[time(45296000000), array([time(1), None, time(86400000000)])]]
        check('INSERT INTO time_binary_rows VALUES(1,$1,$2) RETURNING t,a', [1083, 1183], ['12:34:56', '{00:00:00.000001,NULL,24:00:00}'], [1083, 1183], expected, 'INSERT 0 1')
        check('SELECT t,a FROM time_binary_rows WHERE id=1', [], [], [1083, 1183], expected)
        check('INSERT INTO time_binary_rows VALUES(2,NULL,NULL) RETURNING t,a', [], [], [1083, 1183], [[None, None]], 'INSERT 0 1')
        check('SELECT t,a FROM time_binary_rows WHERE false', [], [], [1083, 1183], [], 'SELECT 0')
        assert controls == 144, controls
        assert not failures, failures
        print('[TIME BINARY RESULT] all 144 controls passed', flush=True)
    finally:
        if server:
            runner.stop_ours(server)
        else:
            try:
                client.simple_query(sock, 'ROLLBACK')
            finally:
                sock.close()


if __name__ == '__main__':
    main()
