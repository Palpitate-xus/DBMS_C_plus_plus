#!/usr/bin/env python3
"""Timestamp binary results preserve signed microseconds relative to 2000."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def cases():
    # Independent civil-calendar values, verified against the real PG18 wire.
    return [
        ('1970-01-01 00:00:00', -946684800000000),
        ('1969-12-31 23:59:59.999999', -946684800000001),
        ('2000-01-01 00:00:00', 0), ('2000-01-01 00:00:00.000001', 1),
        ('1999-12-31 23:59:59.999999', -1),
        ('2000-02-29 03:04:05.123456', 5108645123456),
        ('0001-01-01 00:00:00.000001', -63082281599999999),
        ('9999-12-31 23:59:59.999999', 252455615999999999),
        ('1900-03-01 12:34:56.654321', -3150530703345679),
        ('2024-02-29 12:34:56.000006', 762525296000006),
        ('2000-01-01 00:00:00.0000004', 0),
        ('2000-01-01 00:00:00.0000005', 0),
        ('2000-01-01 00:00:00.0000015', 2),
        ('2000-01-01 00:00:00.0000025', 2),
        ('2000-01-01 00:00:00.0000035', 4),
        ('2000-01-01 00:00:00.1234565', 123456),
        ('2000-01-01 00:00:00.1234575', 123458),
        ('2000-01-01 00:00:00.9999995', 1000000),
        ('1999-12-31 23:59:59.9999995', 0),
        ('2000-12-31 23:59:59.9999995', 31622400000000),
        ('infinity', 2**63 - 1), ('-infinity', -(2**63)),
    ]


def zones():
    # Local clock is 1us after the epoch except for the third item (-1us).
    return [
        ('2000-01-01 00:00:00.000001+08:00', 1, -28799999999),
        ('2000-01-01 00:00:00.000001-05:30', 1, 19800000001),
        ('1999-12-31 23:59:59.999999+15:59', -1, -57540000001),
        ('2000-01-01 00:00:00.000001-15:59', 1, 57540000001),
        ('2000-01-01 00:00:00.000001Z', 1, 1),
        ('2000-01-01 00:00:00.000001+05', 1, -17999999999),
    ]


def timestamp(micros):
    return struct.pack('!q', micros)


def array(oid, values, dimensions=None):
    if dimensions is None:
        dimensions = [(len(values), 1)] if values else []
    result = struct.pack('!iii', len(dimensions), int(None in values), oid)
    for length, lower in dimensions:
        result += struct.pack('!ii', length, lower)
    for value in values:
        result += struct.pack('!i', -1 if value is None else len(value)) + (value or b'')
    return result


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('timestamp_binary_runner', root/'tests/compat/pg_diff_runner.py')
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

    def check(sql, oids, values, outputs, expected, tag='SELECT 1', formats=None,
              describe=False, names=None):
        nonlocal controls
        controls += 1
        formats = ([0] * len(outputs) if describe else [1] * len(outputs)) if formats is None else formats
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
            bind += struct.pack('!H', len(formats)) + b''.join(struct.pack('!H', code) for code in formats)
            packet += client.typed(b'B', bind) + client.typed(b'D', b'P\0') + client.typed(b'E', b'\0\0\0\0\0')
        sock.sendall(packet + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        metadata = [(f[3], f[4], f[5], f[6]) for f in fields]
        desired = [(oid, 8 if oid in (1114, 1184) else -1, -1, code) for oid, code in zip(outputs, formats)]
        rows = client.data_row_values(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        names = [b'v'] if names is None else [name.encode() for name in names]
        good = (not errors and metadata == desired and rows == expected and
                [field[0] for field in fields] == names and messages[-1] == (b'Z', b'I' if not reference else b'T'))
        if describe:
            parameters = [body for kind, body in messages if kind == b't']
            wanted = struct.pack('!H', len(oids)) + b''.join(struct.pack('!I', oid) for oid in oids)
            good = good and not tags and parameters == [wanted]
        else:
            good = good and tags == [tag] and sum(k == b'1' for k, _ in messages) == 1 and sum(k == b'2' for k, _ in messages) == 1
        print('TIMESTAMP_BINARY_RESULT', controls, sql, oids, 'match', good, 'errors', errors, flush=True)
        if not good:
            failures.append((sql, oids, rows, expected, metadata, desired, tags, fields, errors))

    try:
        if reference:
            setup('BEGIN')
            schema = 'timestamp_binary_' + uuid.uuid4().hex[:12]
            setup('CREATE SCHEMA ' + schema)
            setup('SET search_path TO ' + schema)
        setup("SET TIME ZONE 'UTC'")
        assert len(cases()) == 22 and len(zones()) == 6
        for oid, array_oid, declaration in [(1114, 1115, 'timestamp'), (1184, 1185, 'timestamptz')]:
            for value, micros in cases():
                datum = value + '+00' if oid == 1184 and 'infinity' not in value else value
                payload = timestamp(micros)
                check('SELECT $1 AS v', [oid], [datum], [oid], [[payload]])
                check('SELECT $1 AS v', [array_oid], ['{"' + datum + '",NULL}'], [array_oid], [[array(oid, [payload, None])]])
                check("SELECT " + declaration + " '" + datum + "' AS v", [], [], [oid], [[payload]])
                check("SELECT ARRAY[" + declaration + " '" + datum + "',NULL::" + declaration + "] AS v", [], [], [array_oid], [[array(oid, [payload, None])]])
            for value, local, utc in zones():
                payload = timestamp(local if oid == 1114 else utc)
                check('SELECT $1 AS v', [oid], [value], [oid], [[payload]])
                check('SELECT $1 AS v', [array_oid], ['{"' + value + '",NULL}'], [array_oid], [[array(oid, [payload, None])]])
            for result_oid, result_type in [(oid, declaration), (array_oid, declaration + '[]')]:
                check('SELECT $1 AS v', [result_oid], [None], [result_oid], [[None]])
                check('SELECT NULL::' + result_type + ' AS v', [], [], [result_oid], [[None]])
                check('SELECT $1 AS v', [result_oid], [], [result_oid], [], describe=True)
            check('SELECT $1 AS v', [array_oid], ['{}'], [array_oid], [[array(oid, [])]])
            check('SELECT $1 AS v', [array_oid], ['{NULL,NULL}'], [array_oid], [[array(oid, [None, None])]])
            check('SELECT $1 AS v', [array_oid], ['[-3:-1]={"2000-01-01 00:00:00.000001+00",NULL,"1999-12-31 23:59:59.999999+00"}'], [array_oid], [[array(oid, [timestamp(1), None, timestamp(-1)], [(3, -3)])]])
            check('SELECT $1 AS v', [array_oid], ['{{"2000-01-01 00:00:00.000001",NULL},{infinity,-infinity}}'], [array_oid], [[array(oid, [timestamp(1), None, timestamp(2**63-1), timestamp(-(2**63))], [(2, 1), (2, 1)])]])
            check('SELECT $1 AS v', [array_oid], ['{{{{{{"2000-01-01 00:00:00.000001"}}}}}}'], [array_oid], [[array(oid, [timestamp(1)], [(1, 1)] * 6)]])
            check('SELECT $1 AS s,$2 AS a WHERE false', [oid, array_oid], ['2000-01-01 00:00:00.000001', '{}'], [oid, array_oid], [], 'SELECT 0', names=['s', 'a'])
            text = '2000-01-01 00:00:00.000001' + ('+00' if oid == 1184 else '')
            check('SELECT $1 AS v', [oid], [text], [oid], [[text.encode()]], formats=[0])
            check('SELECT $1 AS s,$2 AS a', [oid, array_oid], [text, '{infinity,NULL}'], [oid, array_oid], [[text.encode(), array(oid, [timestamp(2**63-1), None])]], formats=[0, 1], names=['s', 'a'])
        setup('CREATE TABLE timestamp_binary_rows(id INTEGER PRIMARY KEY,t TIMESTAMP,z TIMESTAMPTZ,a TIMESTAMP[],b TIMESTAMPTZ[])')
        expected = [[timestamp(1000000), timestamp(1000000), array(1114, [timestamp(1), None]), array(1184, [timestamp(1), None])]]
        check('INSERT INTO timestamp_binary_rows VALUES(1,$1,$2,$3,$4) RETURNING t,z,a,b', [1114, 1184, 1115, 1185],
              ['2000-01-01 00:00:01', '2000-01-01 08:00:01+08', '{"2000-01-01 00:00:00.000001",NULL}', '{"2000-01-01 08:00:00.000001+08",NULL}'],
              [1114, 1184, 1115, 1185], expected, 'INSERT 0 1', names=['t', 'z', 'a', 'b'])
        check('SELECT t,z,a,b FROM timestamp_binary_rows WHERE id=1', [], [], [1114, 1184, 1115, 1185], expected, names=['t', 'z', 'a', 'b'])
        check('INSERT INTO timestamp_binary_rows VALUES(2,NULL,NULL,NULL,NULL) RETURNING t,z,a,b', [], [],
              [1114, 1184, 1115, 1185], [[None, None, None, None]], 'INSERT 0 1', names=['t', 'z', 'a', 'b'])
        check('SELECT t,z,a,b FROM timestamp_binary_rows WHERE false', [], [], [1114, 1184, 1115, 1185], [], 'SELECT 0', names=['t', 'z', 'a', 'b'])
        print('TIMESTAMP_BINARY_RESULT_COMPLETE', controls, 'FAILURES', len(failures), flush=True)
        assert controls == 232, controls
        assert not failures, failures
        print('[TIMESTAMP BINARY RESULT] all 232 controls passed', flush=True)
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
