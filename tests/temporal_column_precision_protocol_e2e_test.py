#!/usr/bin/env python3
"""Temporal declaration precision is metadata, not a physical codec width.

Checks valid precisions 0/3/6 and omitted (-1), without claiming fractional
heap storage, TIMETZ binary values, cast rounding or invalid-precision syntax.
"""
import importlib.util
from pathlib import Path
import socket
import struct
import sys
import uuid


def declarations(array):
    fields = []
    for name, prefix, oid, array_oid, length in [
            ('time', 't', 1083, 1183, 8), ('timetz', 'z', 1266, 1270, 12),
            ('timestamp', 'p', 1114, 1115, 8), ('timestamptz', 'q', 1184, 1185, 8)]:
        for precision in (0, 3, 6, -1):
            field = prefix + ('a' if array else '') + ('d' if precision < 0 else str(precision))
            spelling = name + ('' if precision < 0 else '(' + str(precision) + ')') + ('[]' if array else '')
            fields.append((field, spelling, array_oid if array else oid, -1 if array else length,
                           precision, len(fields) + 1))
    return fields


def main():
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('temporal_precision_runner', root / 'tests/compat/pg_diff_runner.py')
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

    def check(mode, sql, fields, rows, tag=None, physical=True):
        nonlocal checks
        checks += 1
        code = int(mode == 'binary')
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b'P', b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0))
            if mode == 'describe':
                packet += client.typed(b'D', b'S\0')
            else:
                packet += client.typed(b'B', b'\0\0' + struct.pack('!HHHH', 0, 0, 1, code))
                packet += client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0))
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        actual = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        errors = [client.diagnostic_fields(body) for kind, body in messages if kind == b'E']
        values = client.data_row_values(messages)
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        metadata = [(field[0].decode(), field[3], field[4], field[5], field[6]) for field in actual]
        desired = [(name, oid, length, precision, code) for name, _, oid, length, precision, _ in fields]
        good = not errors and metadata == desired and messages[-1] == (b'Z', b'T' if reference else b'I')
        good = good and all((field[1] > 0 and field[2] == expected[5]) if physical else
                            (field[1] == 0 and field[2] == 0) for field, expected in zip(actual, fields))
        if mode == 'describe':
            good = good and not values and not tags and [body for kind, body in messages if kind == b't'] == [b'\0\0']
        else:
            good = good and sorted(values, key=repr) == sorted(rows, key=repr) and tags == [tag or 'SELECT ' + str(len(rows))]
        if mode != 'simple':
            good = good and sum(kind == b'1' for kind, _ in messages) == 1
            if mode != 'describe':
                good = good and sum(kind == b'2' for kind, _ in messages) == 1
        print('TEMPORAL_COLUMN_PRECISION', checks, mode, sql, 'match', good, 'metadata', metadata, 'errors', errors, flush=True)
        if not good:
            failures.append((mode, sql, metadata, desired, values, rows, tags, errors))

    def phase(sql, fields, rows, binary_rows=None, physical=True):
        for mode in ('simple', 'describe', 'text', 'binary'):
            check(mode, sql, fields, binary_rows if mode == 'binary' and binary_rows is not None else rows,
                  physical=physical)

    try:
        if reference:
            setup('BEGIN')
            schema = 'temporal_precision_' + uuid.uuid4().hex[:12]
            setup('CREATE SCHEMA ' + schema)
            setup('SET search_path TO ' + schema)
        tables = []
        for temporary in (False, True):
            for array in (False, True):
                table = ('temporary_' if temporary else 'ordinary_') + ('arrays' if array else 'scalars')
                fields = declarations(array)
                tables.append((table, fields, array, temporary))
                setup('CREATE ' + ('TEMP ' if temporary else '') + 'TABLE ' + table + '(' +
                      ','.join(field[0] + ' ' + field[1] for field in fields) + ')')
                phase('SELECT * FROM ' + table, fields, [])
                check('binary', 'INSERT INTO ' + table + ' DEFAULT VALUES RETURNING *', fields,
                      [[None] * len(fields)], tag='INSERT 0 1')
                phase('SELECT * FROM ' + table, fields, [[None] * len(fields)])
                setup('ALTER TABLE ' + table + ' RENAME COLUMN ' + fields[0][0] + ' TO renamed')
                fields[0] = ('renamed',) + fields[0][1:]
                phase('SELECT * FROM ' + table, fields, [[None] * len(fields)])
                selected = [fields[1], fields[5], fields[9], fields[13]]
                aliases = [(fields[index * 4][0],) + field[1:] for index, field in enumerate(selected)]
                phase('SELECT ' + ','.join(field[0] + ' AS ' + alias[0] for field, alias in zip(selected, aliases)) +
                      ' FROM ' + table, aliases, [[None] * 4])
                # Reusing a real column name as a CAST alias must not borrow its precision or origin.
                expressions = [(field[0], field[1].split('(')[0] + ('[]' if array else ''),
                                field[2], field[3], -1, 0) for field in selected]
                phase('SELECT ' + ','.join('NULL::' + expression[1] + ' AS ' + expression[0] for expression in expressions) +
                      ' FROM ' + table, expressions, [[None] * 4], physical=False)
        for table, fields, array, temporary in tables:
            if temporary:
                continue
            index, precision = (1, 0) if array else (0, 3)
            target = fields[index]
            base = 'time' + '(' + str(precision) + ')' + ('[]' if array else '')
            setup('ALTER TABLE ' + table + ' ALTER COLUMN ' + target[0] + ' TYPE ' + base)
            fields[index] = (target[0], base, target[2], target[3], precision, target[5])
            phase('SELECT * FROM ' + table, fields, [[None] * len(fields)])
            setup('CREATE TABLE ' + table + '_copy (LIKE ' + table + ')')
            setup('INSERT INTO ' + table + '_copy DEFAULT VALUES')
            phase('SELECT * FROM ' + table + '_copy', fields, [[None] * len(fields)])
        table, fields, _, _ = tables[0]
        setup("INSERT INTO " + table + "(t3) VALUES('12:34:56')")
        text_row, binary_row = [None] * len(fields), [None] * len(fields)
        text_row[1] = b'12:34:56'
        binary_row[1] = struct.pack('!q', (12 * 3600 + 34 * 60 + 56) * 1000000)
        phase('SELECT * FROM ' + table, fields, [[None] * len(fields), text_row],
              [[None] * len(fields), binary_row])
        print('TEMPORAL_COLUMN_PRECISION_COMPLETE', checks, 'FAILURES', len(failures), flush=True)
        assert checks == 104, checks
        assert not failures, failures
        print('[TEMPORAL COLUMN PRECISION] all 104 controls passed', flush=True)
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
