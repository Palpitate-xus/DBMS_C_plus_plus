#!/usr/bin/env python3
"""Sequence completion tags use parsed command identity, not SQL word two."""
import struct
import sys

PREFIXES = (
    'CREATE SEQUENCE',
    'CREATE TEMP SEQUENCE',
    'CREATE TEMPORARY SEQUENCE',
    'CrEaTe TeMpOrArY SeQuEnCe',
    '/* lead */ CREATE /* kind */ TEMP /* scope */ SEQUENCE',
    'CREATE\nTEMPORARY\tSEQUENCE',
    'CREATE -- kind\n TEMP -- scope\n SEQUENCE',
    'CREATE SEQUENCE IF NOT EXISTS',
    'CREATE TEMP SEQUENCE IF NOT EXISTS',
    'CREATE UNLOGGED SEQUENCE',
)
CONTROL_COUNT = len(PREFIXES) * 2 * 5


def main():
    if sys.argv[1:] == ['--list']:
        print('SEQUENCE_COMMAND_TAG_CONTROLS', CONTROL_COUNT)
        return
    from compat.numeric_type_review import Review
    review = Review('SEQUENCE_COMMAND_TAG')

    def check(mode, sql, tag=None, rows=(), state=None):
        if mode == 'simple':
            messages = review.client.simple_query(review.sock, sql)
            prefix = []
        else:
            parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', 0)
            bind = b'\0\0' + struct.pack('!HHH', 0, 0, 0)
            review.sock.sendall(review.client.typed(b'P', parse) +
                review.client.typed(b'B', bind) +
                review.client.typed(b'E', b'\0' + struct.pack('!I', 0)) +
                review.client.typed(b'S'))
            messages = review.client.read_until_ready(review.sock)
            prefix = [b'1', b'2']
        result = review.runner.decode_wire_result(messages)
        kinds = [kind for kind, _ in messages]
        expected_rows = [list(row) for row in rows]
        tags = [body for kind, body in messages if kind == b'C']
        expected_tags = [] if tag is None else [tag.encode() + b'\0']
        expected_kinds = prefix + ([b'E'] if state else
            (([b'T'] if mode == 'simple' else []) + [b'D'] * len(rows) + [b'C']
             if rows else [b'C'])) + [b'Z']
        good = (result[0] == expected_rows and result[1] == state and
                tags == expected_tags and kinds == expected_kinds and
                messages[-1] == (b'Z', b'I'))
        if mode != 'simple':
            good = good and all(not body for kind, body in messages if kind in (b'1', b'2'))
        review.record((mode, sql), good,
            dict(result=result, kinds=[kind.decode() for kind in kinds],
                 tags=[body.hex() for body in tags], ready=messages[-1][1].hex()),
            dict(rows=expected_rows, state=state, tag=tag,
                 kinds=[kind.decode() for kind in expected_kinds], ready='I'))

    try:
        for index, prefix in enumerate(PREFIXES):
            for mode in ('simple', 'extended'):
                name = 'r306_sequence_' + str(index) + '_' + mode
                check(mode, prefix + ' ' + name + ' START WITH 7', 'CREATE SEQUENCE')
                check('simple', "SELECT nextval('" + name + "') AS v", 'SELECT 1', [('7',)])
                # The object must really exist; a failed command must never
                # acquire a successful CREATE tag. Sync must permit recovery.
                duplicate_prefix = prefix.replace(' IF NOT EXISTS', '')
                check(mode, duplicate_prefix + ' ' + name, state='42P07')
                check(mode, 'SELECT 42 AS v', 'SELECT 1', [('42',)])
                check(mode, 'DROP SEQUENCE ' + name, 'DROP SEQUENCE')
        review.complete(CONTROL_COUNT)
    finally:
        review.close()


if __name__ == '__main__':
    main()
