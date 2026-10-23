#!/usr/bin/env python3
"""Exact interval/interval[] binary Bind input, errors and durable DML values."""
from compat.numeric_type_review import Review
from interval_binary_result_protocol_e2e_test import cases, interval, array
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows, identifier
import struct
import sys
import uuid


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('INTERVAL_BINARY_INPUT_PROTOCOL')
    client, sock = review.client, review.sock
    schema = 'interval_recv_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    owned = False

    def check(sql, inputs, values, outputs, expected=None, state=None, modifiers=None):
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', len(inputs))
        parse += b''.join(struct.pack('!I', oid) for oid in inputs)
        bind = b'\0\0' + struct.pack('!HHH', 1, 1, len(values))
        for value in values:
            bind += struct.pack('!i', -1 if value is None else len(value)) + (value or b'')
        bind += struct.pack('!HH', 1, 1)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) + client.typed(b'D', b'P\0') +
                     client.typed(b'E', b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        rows, valid = strict_data_rows(messages)
        errors = [client.diagnostic_fields(body).get(b'C', b'').decode() for kind, body in messages if kind == b'E']
        descriptions = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        tags = [body.rstrip(b'\0').decode() for kind, body in messages if kind == b'C']
        if state:
            good = errors == [state] and not rows and not tags and not descriptions
        else:
            modifiers = modifiers or [-1] * len(outputs)
            wanted = [(oid, 16 if oid == 1186 else -1, modifier, 1) for oid, modifier in zip(outputs, modifiers)]
            metadata = [field[3:] for field in descriptions]
            good = (valid and not errors and rows == expected and metadata == wanted and
                    all(field[1] == 0 and field[2] == 0 for field in descriptions) and
                    sum(kind == b'1' for kind, _ in messages) == 1 and sum(kind == b'2' for kind, _ in messages) == 1 and
                    len(tags) == 1 and messages[-1] == (b'Z', b'I'))
        review.record((sql, inputs, tuple(None if value is None else len(value) for value in values)), good,
                      (rows, errors, descriptions, tags), (expected, state, outputs))

    def scalar(value, state=None):
        check('SELECT $1 AS v', [1186], [value], [1186], [[value]], state)

    def array_value(value, state=None):
        check('SELECT $1 AS v', [1187], [value], [1187], [[value]], state)

    def recovery():
        scalar(interval(-1, 1, -1))

    try:
        assert len(cases()) == 35
        for _, micros, days, months in cases():
            datum = interval(micros, days, months)
            scalar(datum)
            array_value(array([datum, None], [(2, -3)]))
        scalar(None)
        array_value(None)
        array_value(array([]))
        array_value(array([interval(1), None, interval(-1), interval(0, -1, 1)], [(2, -1), (2, 3)]))
        array_value(array([interval(-2**63)], [(1, -1)] * 6))
        for length in tuple(range(16)) + tuple(range(17, 21)):
            scalar(bytes(length), '08P01' if length < 16 else '22P03')
            recovery()
        for length in (0, 1, 8, 15, 17, 20):
            array_value(array([bytes(length)]), '08P01' if length < 16 else '22P03')
            recovery()
        for raw, state in ((struct.pack('!iii', 0, 0, 23), '42804'),
                           (struct.pack('!iii', 0, 2, 1186), '22P03'),
                           (array([]) + b'x', '22P03')):
            array_value(raw, state)
            recovery()
        for oid in (1186, 1187):
            review.describe('SELECT $1 AS v', oids=[oid], parameter_oids=(oid,))
        previous = len(review.failures)
        review.query('CREATE SCHEMA ' + identifier(schema), rows=[], tag='CREATE SCHEMA')
        assert len(review.failures) == previous, 'private schema ownership not confirmed'
        owned = True
        review.query('CREATE TABLE ' + table + '(id integer,s interval,a interval[],p interval DAY TO SECOND(3))', rows=[], tag='CREATE TABLE')
        value = interval(3456789, 1)
        values = array([interval(0, 0, 1), None, interval(-1)], [(3, -2)])
        typmod = (7176 << 16) | 3
        check('INSERT INTO ' + table + ' VALUES(1,$1,$2,$1) RETURNING s+INTERVAL \'0\',CAST(CAST(a AS text) AS interval[]),p+INTERVAL \'0\'',
              [1186, 1187], [value, values], [1186, 1187, 1186], [[value, values, interval(3457000, 1)]])
        check('SELECT s+INTERVAL \'0\',CAST(CAST(a AS text) AS interval[]),p+INTERVAL \'0\' FROM ' + table + ' WHERE id=1',
              [], [], [1186, 1187, 1186], [[value, values, interval(3457000, 1)]])
        negative = interval(-1234567, -1)
        check('UPDATE ' + table + ' SET s=$1,p=$1 WHERE id=1 RETURNING s+INTERVAL \'0\',p+INTERVAL \'0\'',
              [1186], [negative], [1186, 1186], [[negative, interval(-1235000, -1)]])
        check('INSERT INTO ' + table + ' VALUES(2,$1,$2,$1) RETURNING s+INTERVAL \'0\',CAST(CAST(a AS text) AS interval[]),p+INTERVAL \'0\'',
              [1186, 1187], [None, None], [1186, 1187, 1186], [[None, None, None]])
        # A real typmod cast must still run after binary reception.
        check('SELECT $1::interval DAY TO SECOND(3) AS v', [1186], [value], [1186], [[interval(3457000, 1)]], modifiers=[typmod])
        review.query('DROP SCHEMA ' + identifier(schema) + ' CASCADE', rows=[], tag='DROP SCHEMA')
        owned = False
        review.complete(143)
    finally:
        if owned:
            client.simple_query(sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
