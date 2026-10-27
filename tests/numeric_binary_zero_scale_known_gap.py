#!/usr/bin/env python3
"""Finite binary NUMERIC zero-scale preservation within the existing domain."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, WIDTHS, strict_data_rows
import struct
import uuid

WIDTHS.update({1700: -1})
SCALES = (0, 1, 2, 6, 32, 999, 1000)
WEIGHTS = (0, -1, 1, -32768, 32767)


def numeric_zero(scale, weight=0, negative=False):
    return struct.pack('!HhHH', 0, weight, 0x4000 if negative else 0, scale)


def zero_text(scale):
    return '0' + ('.' + '0' * scale if scale else '')


class NumericWire(Wire):
    def parameter(self, sql, raw, oid, expected, binary=False, tag='SELECT 1'):
        packet = self.parse(b'', sql, (1700,))
        body = b'\0\0' + struct.pack('!HHH', 1, 1, 1)
        body += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
        body += struct.pack('!HH', 1, int(binary))
        packet += self.client.typed(b'B', body) + self.client.typed(b'D', b'P\0')
        packet += self.client.typed(b'E', b'\0' + struct.pack('!I', 0))

        def valid(messages):
            rows, well_formed = strict_data_rows(messages)
            return (well_formed and not self.errors(messages) and rows == [[expected]] and
                    [kind for kind, _ in messages] == [b'1', b't', b'T', b'2', b'T', b'D', b'C', b'Z'] and
                    [body for kind, body in messages if kind == b't'] == [self.parameters((1700,))] and
                    [body for kind, body in messages if kind == b'T'] == [self.description(oid), self.description(oid, binary)] and
                    [body for kind, body in messages if kind == b'C'] == [tag.encode() + b'\0'] and
                    all(body == b'' for kind, body in messages if kind in (b'1', b'2')) and
                    messages[-1:] == [(b'Z', b'I')])

        self.collect((sql, raw, binary), lambda: self.exchange(packet), valid, expected)


def main():
    review = Review('NUMERIC_BINARY_ZERO_SCALE')
    wire = NumericWire(review)
    try:
        for scale in SCALES:
            for weight in WEIGHTS:
                for negative in (False, True):
                    raw = numeric_zero(scale, weight, negative)
                    for binary in (False, True):
                        wire.parameter('SELECT $1 AS v', raw, 1700,
                                       numeric_zero(scale) if binary else zero_text(scale).encode(), binary)
                        wire.parameter('SELECT scale($1) AS v', raw, 23,
                                       struct.pack('!i', scale) if binary else str(scale).encode(), binary)
                        wire.parameter('SELECT $1::text AS v', raw, 25, zero_text(scale).encode(), binary)
        for binary in (False, True):
            wire.parameter('SELECT $1 AS v', None, 1700, None, binary)
            wire.parameter('SELECT $1 AS v', struct.pack('!HhHHH', 1, 0, 0, 2, 125), 1700,
                           struct.pack('!HhHHH', 1, 0, 0, 2, 125) if binary else b'125.00', binary)
        table = 'r295_numeric_' + uuid.uuid4().hex
        review.query('CREATE TABLE ' + table + ' (v numeric)')
        for scale in (0, 2, 999, 1000):
            raw = numeric_zero(scale, -1, True)
            wire.parameter('INSERT INTO ' + table + ' VALUES ($1) RETURNING v::text AS v', raw, 25,
                           zero_text(scale).encode(), tag='INSERT 0 1')
        review.query('SELECT v::text,scale(v) FROM ' + table + ' ORDER BY scale(v)',
                     [[zero_text(scale), str(scale)] for scale in (0, 2, 999, 1000)], [25, 23])
        review.query('SELECT 42 AS v', [['42']], [23])
        review.complete(431)
    finally:
        review.close()


if __name__ == '__main__':
    main()
