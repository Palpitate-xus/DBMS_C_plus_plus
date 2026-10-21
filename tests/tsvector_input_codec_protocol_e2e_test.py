#!/usr/bin/env python3
"""Finite text/binary TSVECTOR input validation and connection recovery."""
from compat.numeric_type_review import Review
from tsvector_concat_protocol_e2e_test import Wire, literal
import struct
import sys


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('TSVECTOR_INPUT_CODEC_PROTOCOL')
    wire = Wire(review)
    try:
        for raw, value in (
                ('', ''), ('a:2b,1d', "'a':1,2B"), ('a:1*', "'a':1A"),
                ('a:1D,1A', "'a':1A"), ('z aa', "'aa' 'z'"),
                ("'quote''d'", "'quote''d'"), ('a:20000', "'a':16383"),
                ('x' * 2046, "'" + 'x' * 2046 + "'")):
            review.query('SELECT (' + literal(raw) + ')::text AS v', [[value]], [25])
        review.query('SELECT ' + literal('x' * 2047) + ' AS v', state='54000')
        for raw in ('a:0', 'a:1Z', "'unclosed", 'a:1,', 'a:', "''"):
            review.query('SELECT ' + literal(raw) + ' AS v', state='42601')
        statement = b'tsvector_codec_boundary'
        wire.describe(statement, "SELECT $1 || ''::tsvector AS v", 3614, (3614,))
        word = lambda n: struct.pack('!I', n)
        short = lambda n: struct.pack('!H', n)
        errors = (
            (b'', '08P01'), (b'\0' * 3, '08P01'),
            (word(0xffffffff), 'XX000'), (word(1 << 20), 'XX000'),
            (word(1), '08P01'), (word(1) + b'a', '08P01'),
            (word(1) + b'a\0', '08P01'),
            (word(1) + b'\0' + short(0), 'XX000'),
            (word(1) + b'a\0' + short(257), 'XX000'),
            (word(1) + b'a\0' + short(2) + short(2) + short(1), 'XX000'),
            (word(1) + b'\xc0\xaf\0' + short(0), '22021'),
            (word(0) + b'x', '22P03'),
            (word(1) + b'a\0' + short(1) + b'\0', '08P01'),
        )
        for raw, state in errors:
            wire.collect(('BIND ERROR', raw, state),
                         lambda: wire.exchange(wire.bind(statement, (3614,), (raw,), True, True)),
                         lambda messages: [kind for kind, _ in messages] == [b'E', b'Z'] and wire.errors(messages) == [state] and messages[-1:] == [(b'Z', b'I')], state)
            wire.execute(statement, 3614, ({'a': [(1, 'A')]},), (3614,), ({'a': [(1, 'A')]},), True, True)
        wire.close(statement)
        review.complete(43)
    finally:
        review.close()


if __name__ == '__main__':
    main()
