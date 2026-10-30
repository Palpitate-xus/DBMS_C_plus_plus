#!/usr/bin/env python3
"""Actual catalog OIDs keep their unsigned four-byte binary send identity."""
from compat.numeric_type_review import Review
import struct
import sys
import uuid


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    r = Review('OID_BINARY_RESULT_PROTOCOL')
    token = uuid.uuid4().hex[:8]
    names = ['oid228_' + token, 'Mixed228_' + token, 'dot.' + token,
             'space ' + token, 'q"' + token]
    owned = []

    def command(sql):
        messages = r.client.simple_query(r.sock, sql)
        result = r.runner.decode_wire_result(messages, include_types=True)
        valid = (result[1] is None and result[0] == [] and result[3] == [] and
                 result[5] == [] and messages[-1] == (b'Z', b'I') and
                 sum(kind == b'C' for kind, _ in messages) == 1)
        r.record(sql, valid, result, 'one successful command')

    def wire(sql, values, formats, parameter=None):
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', int(parameter is not None))
        if parameter is not None:
            parse += struct.pack('!I', 26)
        bind = b'\0\0'
        if parameter is None:
            bind += struct.pack('!HH', 0, 0)
        else:
            bind += struct.pack('!HHHiI', 1, 1, 1, 4, parameter)
        bind += struct.pack('!H', len(formats)) + b''.join(struct.pack('!H', code) for code in formats)
        r.sock.sendall(r.client.typed(b'P', parse) + r.client.typed(b'B', bind) +
                       r.client.typed(b'D', b'P\0') +
                       r.client.typed(b'E', b'\0' + struct.pack('!I', 0)) + r.client.typed(b'S'))
        messages = r.client.read_until_ready(r.sock)
        fields = (r.client.row_description_fields(messages)
                  if any(kind == b'T' for kind, _ in messages) else [])
        expected = []
        for row in values:
            raw = struct.pack('!H', 3)
            for value, code in zip(row, formats):
                cell = struct.pack('!I', int(value)) if code else value.encode()
                raw += struct.pack('!i', len(cell)) + cell
            expected.append(raw)
        valid = ([kind for kind, _ in messages] == [b'1', b'2', b'T'] + [b'D'] * len(values) + [b'C', b'Z'] and
                 [field[3] for field in fields] == [26, 26, 26] and
                 [field[4] for field in fields] == [4, 4, 4] and
                 [field[-1] for field in fields] == formats and
                 [body for kind, body in messages if kind == b'D'] == expected and
                 [body for kind, body in messages if kind == b'C'] == [('SELECT ' + str(len(values))).encode() + b'\0'] and
                 messages[-1] == (b'Z', b'I'))
        r.record(('WIRE OID', sql, parameter, formats), valid, (messages, fields), expected)

    try:
        for name in names:
            command('CREATE TABLE public.' + ident(name) + '(n integer)')
            owned.append(name)
        for name in names:
            suffix = ' WHERE relname=' + literal(name)
            projection = 'SELECT oid AS id,relnamespace AS ns,relowner AS owner FROM '
            capture = r.runner.decode_wire_result(r.client.simple_query(r.sock, projection + 'pg_class' + suffix), include_types=True)
            valid = (capture[1] is None and capture[5] == [26, 26, 26] and len(capture[0]) == 1 and
                     len(capture[0][0]) == 3 and all(value.isdigit() for value in capture[0][0]) and
                     int(capture[0][0][0]) > 0)
            r.record(('ACTUAL OID CELLS', name), valid, capture, 'three actual uint32 catalog values')
            values = capture[0] if valid else [['0', '0', '0']]
            for source in ['pg_class', 'pg_catalog.pg_class', '"pg_catalog"."pg_class"']:
                sql = projection + source + suffix
                r.query(sql, rows=values, oids=[26, 26, 26])
                r.describe(sql, oids=[26, 26, 26])
                wire(sql, values, [1, 1, 1])
                r.parameter(projection + source + ' WHERE oid::text=$1',
                            [values[0][0]], [25], values, [26, 26, 26])
                wire(projection + source + ' WHERE oid=$1', values, [1, 1, 1], int(values[0][0]))
            wire(projection + 'pg_catalog.pg_class WHERE FALSE', [], [1, 1, 1])
            wire(projection + 'pg_catalog.pg_class' + suffix, values, [0, 1, 0])
    finally:
        for name in reversed(owned):
            command('DROP TABLE public.' + ident(name))
        r.close()
    r.complete(100)


if __name__ == '__main__':
    main()
