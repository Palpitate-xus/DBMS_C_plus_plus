#!/usr/bin/env python3
"""Duplicate sequence DDL preserves SQLSTATE, objects and transaction recovery."""
import struct
import sys

CONTROL_COUNT = 260


def main():
    if sys.argv[1:] == ['--list']:
        print('SEQUENCE_DUPLICATE_SQLSTATE_CONTROLS', CONTROL_COUNT)
        return
    from compat.numeric_type_review import Review
    review = Review('SEQUENCE_DUPLICATE_SQLSTATE')

    def check(mode, sql, tag=None, rows=(), state=None, ready='I', notice=None):
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
        expected_rows = [list(row) for row in rows]
        kinds = [kind for kind, _ in messages]
        tags = [body for kind, body in messages if kind == b'C']
        expected_tags = [] if tag is None else [tag.encode() + b'\0']
        expected_kinds = prefix + ([b'N'] if notice else []) + ([b'E'] if state else
            (([b'T'] if mode == 'simple' else []) + [b'D'] * len(rows) + [b'C']
             if rows else [b'C'])) + [b'Z']
        notices = [review.client.diagnostic_fields(body) for kind, body in messages if kind == b'N']
        notice_fields = [(fields.get(b'S'), fields.get(b'C')) for fields in notices]
        expected_notices = [] if notice is None else [(b'NOTICE', notice.encode())]
        good = (result[0] == expected_rows and result[1] == state and tags == expected_tags and
                kinds == expected_kinds and messages[-1] == (b'Z', ready.encode()) and
                notice_fields == expected_notices and
                all(not body for kind, body in messages if kind in (b'1', b'2')))
        review.record((mode, sql), good,
            dict(result=result, kinds=[kind.decode() for kind in kinds],
                 tags=[body.hex() for body in tags], ready=messages[-1][1].decode(),
                 notices=[(severity.decode(), code.decode()) for severity, code in notice_fields]),
            dict(rows=expected_rows, state=state, tag=tag, ready=ready,
                 kinds=[kind.decode() for kind in expected_kinds], notice=notice))

    try:
        check('simple', 'CREATE SCHEMA r307_scope', 'CREATE SCHEMA')
        for scope in ('public', 'r307_scope', 'temp'):
            for kind in ('sequence', 'table', 'view'):
                for mode in ('simple', 'extended'):
                    name = 'r307_' + scope + '_' + kind + '_' + mode
                    target = name if scope == 'temp' else scope + '.' + name
                    prefix = 'CREATE TEMP ' if scope == 'temp' else 'CREATE '
                    if kind == 'sequence':
                        setup = prefix + 'SEQUENCE ' + target + ' START WITH 11'
                    elif kind == 'table':
                        setup = prefix + 'TABLE ' + target + '(id integer)'
                    else:
                        setup = prefix + 'VIEW ' + target + ' AS SELECT 9 AS id'
                    check('simple', setup, 'CREATE ' + kind.upper())
                    if kind == 'table':
                        check('simple', 'INSERT INTO ' + target + ' VALUES(9)', 'INSERT 0 1')
                    next_value = 11

                    def probe(ready='I'):
                        nonlocal next_value
                        if kind == 'sequence':
                            sql = "SELECT nextval('" + target + "') AS v"
                            value = str(next_value)
                            next_value += 1
                        else:
                            sql, value = 'SELECT id FROM ' + target, '9'
                        check('simple', sql, 'SELECT 1', [(value,)], ready=ready)

                    probe()
                    duplicate = prefix + 'SEQUENCE ' + target + ' START WITH 999'
                    check(mode, duplicate, state='42P07')
                    probe()
                    if_not_exists = prefix + 'SEQUENCE IF NOT EXISTS ' + target + ' START WITH 999'
                    check(mode, if_not_exists, 'CREATE SEQUENCE', notice='42P07')
                    probe()
                    check('simple', 'BEGIN', 'BEGIN', ready='T')
                    check('simple', 'SAVEPOINT r307_guard', 'SAVEPOINT', ready='T')
                    check(mode, duplicate, state='42P07', ready='E')
                    check('simple', 'SELECT 42 AS v', state='25P02', ready='E')
                    check('simple', 'ROLLBACK TO SAVEPOINT r307_guard', 'ROLLBACK', ready='T')
                    probe('T')
                    check('simple', 'ROLLBACK', 'ROLLBACK')
                    check('simple', 'DROP ' + kind.upper() + ' ' + target, 'DROP ' + kind.upper())
        check('simple', 'DROP SCHEMA r307_scope', 'DROP SCHEMA')
        review.complete(CONTROL_COUNT)
    finally:
        review.close()


if __name__ == '__main__':
    main()
