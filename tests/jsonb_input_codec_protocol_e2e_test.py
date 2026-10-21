#!/usr/bin/env python3
"""JSONB scalar/array input boundaries, exact decimals, errors and recovery."""
from compat.numeric_type_review import Review
from jsonb_concat_protocol_e2e_test import Wire, literal
from array_binary_result_protocol_e2e_test import binary_array
import sys


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('JSONB_INPUT_CODEC_PROTOCOL')
    wire = Wire(review)
    try:
        for raw, value in (
                ('1e131071', '1' + '0' * 131071),
                ('1e-16383', '0.' + '0' * 16382 + '1'),
                ('9' * 131072, '9' * 131072),
                ('0e1000000', '0'), ('-0.000e+1', '0.00'),
                ('"\\uDBFF\\uDFFF"', '"' + chr(0x10ffff) + '"')):
            review.query('SELECT (' + literal(raw) + ')::text AS v', [[value]], [25])
        for raw, state in (
                ('1e131072', '22003'), ('1e-16384', '22003'), ('0e-16384', '22003'),
                ('1e1073741824', '22003'), ('1e1073741824x', '22P02'),
                ('[1e1073741824x]', '22P02'), ('\v{}', '22P02'), ('\f[]', '22P02')):
            review.query('SELECT ' + literal(raw) + ' AS v', state=state)
        statement = b'jsonb_boundary_scalar'
        wire.describe(statement, "SELECT $1 || '{}'::jsonb AS v", 3802, (3802,))
        for raw, state in ((b'', '08P01'), (b'\1', '22P02'), (b'\2{}', 'XX000'),
                           (b'\1"\xc0\xaf"', '22021'), (b'\1"\xe2\x82"', '22021'),
                           (b'\1"\xed\xa0\x80"', '22021'), (b'\1"\xf4\x90\x80\x80"', '22021')):
            wire.bind_error(statement, raw, True, state)
            wire.execute(statement, 3802, ('{}',), (3802,), ('{}',), True, True)
        wire.close(statement)
        statement = b'jsonb_boundary_array'
        wire.describe(statement, "SELECT $1 || ARRAY['{}'::jsonb] AS v", 3807, (3807,))
        for raw, state in ((b'', '08P01'), (b'\2{}', 'XX000'), (b'\1{', '22P02'), (b'\1"\\u0000"', '22P05')):
            value = binary_array(3802, [raw])
            wire.collect(('ARRAY BIND ERROR', state),
                         lambda: wire.exchange(wire.bind(statement, (3807,), (value,), True, True)),
                         lambda messages: [kind for kind, _ in messages] == [b'E', b'Z'] and wire.errors(messages) == [state] and messages[-1:] == [(b'Z', b'I')], state)
            wire.execute(statement, 3807, (('{}',),), (3807,), ((),), True, True)
        wire.close(statement)
        review.complete(40)
    finally:
        review.close()


if __name__ == '__main__':
    main()
