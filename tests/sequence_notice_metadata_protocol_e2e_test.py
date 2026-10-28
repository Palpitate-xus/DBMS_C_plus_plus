#!/usr/bin/env python3
"""Sequence skip notices preserve real SQLSTATE without parsing user text."""
import struct
import sys

CONTROLS = 120

def main():
    if sys.argv[1:] == ['--list']:
        print(CONTROLS)
        return
    from compat.numeric_type_review import Review
    from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows
    r = Review('SEQUENCE_NOTICE_METADATA')
    def check(mode, sql, tag, rows=(), ready='I', notices=()):
        if mode == 'simple':
            messages = r.client.simple_query(r.sock, sql)
            prefix = []
        else:
            r.sock.sendall(r.client.typed(b'P', b'\0' + sql.encode() + b'\0\0\0') +
                r.client.typed(b'B', b'\0\0' + struct.pack('!HHH', 0, 0, 0)) +
                r.client.typed(b'E', b'\0' + struct.pack('!I', 0)) + r.client.typed(b'S'))
            messages = r.client.read_until_ready(r.sock)
            prefix = [b'1', b'2']
        result = r.runner.decode_wire_result(messages)
        actual_rows, frames_valid = strict_data_rows(messages)
        expected_rows = [list(row) for row in rows]
        kinds = [kind for kind, _ in messages]
        expected_kinds = prefix + [b'N'] * len(notices) + (([b'T'] if mode == 'simple' else []) +
            [b'D'] * len(rows) if rows else []) + [b'C', b'Z']
        received = []
        for kind, body in messages:
            if kind == b'N':
                fields = r.client.diagnostic_fields(body)
                received.append(tuple(fields.get(key, b'').decode() for key in (b'S', b'C', b'M')))
        good = (result[0] == expected_rows and result[1] is None and actual_rows == [[None if value is None else value.encode() for value in row] for row in expected_rows] and
            frames_valid and kinds == expected_kinds and received == list(notices) and
            [body for kind, body in messages if kind == b'C'] == [tag.encode() + b'\0'] and
            messages[-1] == (b'Z', ready.encode()) and
            all(not body for kind, body in messages if kind in (b'1', b'2')))
        r.record((mode, sql), good,
            dict(result=result, rows=actual_rows, framesValid=frames_valid, kinds=[k.decode() for k in kinds],
                 notices=received, tags=[b.hex() for k, b in messages if k == b'C'], ready=messages[-1][1].decode()),
            dict(rows=expected_rows, kinds=[k.decode() for k in expected_kinds], notices=notices, tag=tag, ready=ready))
    try:
        check('simple', 'CREATE SCHEMA r308_scope', 'CREATE SCHEMA')
        for scope in ('public', 'r308_scope', 'temp'):
            for kind in ('sequence', 'table'):
                for mode in ('simple', 'extended'):
                    name = 'r308_' + scope + '_' + kind + '_' + mode
                    target = name if scope == 'temp' else scope + '.' + name
                    prefix = 'CREATE TEMP ' if scope == 'temp' else 'CREATE '
                    setup = prefix + ('SEQUENCE ' + target + ' START WITH 11' if kind == 'sequence' else
                        'TABLE ' + target + '(id integer)')
                    check('simple', setup, 'CREATE ' + kind.upper())
                    if kind == 'table':
                        check('simple', 'INSERT INTO ' + target + ' VALUES(9)', 'INSERT 0 1')
                    duplicate = prefix + 'SEQUENCE IF NOT EXISTS ' + target + ' START WITH 999'
                    notice = (('NOTICE', '42P07', 'relation "' + name + '" already exists, skipping'),)
                    probe = "SELECT nextval('" + target + "') AS v" if kind == 'sequence' else 'SELECT id FROM ' + target
                    check(mode, duplicate, 'CREATE SEQUENCE', notices=notice)
                    check(mode, probe, 'SELECT 1', [('11' if kind == 'sequence' else '9',)])
                    check('simple', 'BEGIN', 'BEGIN', ready='T')
                    check(mode, duplicate, 'CREATE SEQUENCE', ready='T', notices=notice)
                    check(mode, probe, 'SELECT 1', [('12' if kind == 'sequence' else '9',)], ready='T')
                    check('simple', 'ROLLBACK', 'ROLLBACK')
                    check('simple', 'DROP ' + kind.upper() + ' ' + target, 'DROP ' + kind.upper())
        check('simple', 'DROP SCHEMA r308_scope', 'DROP SCHEMA')
        for mode in ('simple', 'extended'):
            for severity, code, text in (
                ('NOTICE', '00000', 'user (SQLSTATE 42P07)'),
                ('NOTICE', '00000', 'relation already exists, skipping'),
                ('WARNING', '01000', 'user (SQLSTATE 42P07)'),
                ('NOTICE', '00000', 'normal user notice')):
                check(mode, "DO $$ BEGIN RAISE " + severity + " '" + text + "'; END $$", 'DO',
                    notices=((severity, code, text),))
                check(mode, 'SELECT 42 AS v', 'SELECT 1', [('42',)])
        r.complete(CONTROLS)
    finally:
        r.close()

if __name__ == '__main__':
    main()
