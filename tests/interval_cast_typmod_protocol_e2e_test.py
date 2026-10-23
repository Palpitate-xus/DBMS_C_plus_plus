#!/usr/bin/env python3
"""Interval cast declarations retain exact typmods without executing values."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows, identifier
import struct
import sys
import uuid

KEYWORDS = (
    ('interval', -1), ('interval(3)', (32767 << 16) | 3),
    ('interval YEAR', (4 << 16) | 65535), ('interval MONTH', (2 << 16) | 65535),
    ('interval DAY', (8 << 16) | 65535), ('interval HOUR', (1024 << 16) | 65535),
    ('interval MINUTE', (2048 << 16) | 65535), ('interval SECOND', (4096 << 16) | 65535),
    ('interval YEAR TO MONTH', (6 << 16) | 65535),
    ('interval DAY TO HOUR', (1032 << 16) | 65535),
    ('interval DAY TO MINUTE', (3080 << 16) | 65535),
    ('interval DAY TO SECOND(3)', (7176 << 16) | 3),
    ('interval HOUR TO MINUTE', (3072 << 16) | 65535),
    ('interval HOUR TO SECOND(6)', (7168 << 16) | 6),
    ('interval MINUTE TO SECOND(0)', 6144 << 16), ('interval SECOND(2)', (4096 << 16) | 2),
)
NAMED = (('pg_catalog.interval(7176,3)', (7176 << 16) | 3),
         ('"pg_catalog"."interval"(7176,3)', (7176 << 16) | 3))


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('INTERVAL_CAST_TYPMOD_PROTOCOL')
    client, sock = review.client, review.sock
    schema = 'interval_mod_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    sequence = "'" + identifier(schema) + '.effects_' + "'"
    owned = False

    def operation(mode, sql, oid, modifier, rows=(None,), parameter=False):
        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', int(parameter))
        if parameter:
            parse += struct.pack('!I', oid)
        bind = b'\0\0' + struct.pack('!HH', 0, int(parameter))
        if parameter:
            bind += struct.pack('!i', -1)
        binary = mode == 'execute'
        bind += struct.pack('!HH', 1, int(binary))
        if mode == 'simple':
            messages = client.simple_query(sock, sql)
        else:
            packet = client.typed(b'P', parse)
            if mode == 'describe':
                packet += client.typed(b'D', b'S\0')
            else:
                packet += client.typed(b'B', bind) + client.typed(b'D', b'P\0')
                if mode == 'execute':
                    packet += client.typed(b'E', b'\0' + struct.pack('!I', 0))
            sock.sendall(packet + client.typed(b'S'))
            messages = client.read_until_ready(sock)
        description = struct.pack('!H', 1) + b'v\0' + struct.pack('!IhIhih', 0, 0, oid, 16 if oid == 1186 else -1, modifier, int(binary))
        actual_rows, valid = strict_data_rows(messages)
        errors = [client.diagnostic_fields(body).get(b'C', b'').decode() for kind, body in messages if kind == b'E']
        wanted_rows = [[value] for value in rows]
        if binary:
            # Non-NULL execution here is only the single effect schedule's
            # exact zero datum: TEXT '1' means one second before the DAY
            # coercion truncates it. NULLs/empty rows need no text decoder.
            wanted_rows = [[None if value is None else struct.pack('!qii', 0, 0, 0)] for value in rows]
        good = (not errors and valid and [body for kind, body in messages if kind == b'T'] == [description] and messages[-1:] == [(b'Z', b'I')])
        if mode in ('describe', 'portal'):
            good = good and not actual_rows and not any(kind == b'C' for kind, _ in messages)
        else:
            good = good and actual_rows == wanted_rows and [body for kind, body in messages if kind == b'C'] == [('SELECT ' + str(len(rows))).encode() + b'\0']
        if mode != 'simple':
            good = good and sum(kind == b'1' for kind, _ in messages) == 1
            if mode != 'describe':
                good = good and sum(kind == b'2' for kind, _ in messages) == 1
        review.record((mode, sql), good, (actual_rows, errors, [body for kind, body in messages if kind == b'T']), (rows, oid, modifier, binary))

    try:
        assert len(KEYWORDS) == 16 and len(NAMED) == 2
        for declaration, modifier in KEYWORDS + NAMED:
            for array in (False, True):
                target = declaration + ('[]' if array else '')
                oid = 1187 if array else 1186
                for form in ('CAST({value} AS ' + target + ')', '({value})::' + target):
                    for mode in ('simple', 'describe', 'portal', 'execute'):
                        parameter = mode in ('portal', 'execute')
                        expression = form.format(value='$1' if parameter else 'NULL')
                        operation(mode, 'SELECT ' + expression + ' AS v', oid, modifier, parameter=parameter)
        for declaration, modifier in KEYWORDS:
            literal = declaration + " '0'" if declaration.startswith('interval(') else "INTERVAL '0'" + declaration[len('interval'):]
            for mode in ('simple', 'describe'):
                operation(mode, 'SELECT ' + literal + ' AS v', 1186, modifier, (b'00:00:00',))
        previous = len(review.failures)
        review.query('CREATE SCHEMA ' + identifier(schema), rows=[], tag='CREATE SCHEMA')
        assert len(review.failures) == previous, 'private schema ownership not confirmed'
        owned = True
        review.query('CREATE TABLE ' + table + '(v interval,a interval[])', rows=[], tag='CREATE TABLE')
        review.query('CREATE SEQUENCE ' + identifier(schema) + '.effects_', rows=[], tag='CREATE SEQUENCE')
        specs = (('interval DAY', (8 << 16) | 65535, 1186, 'v'),
                 ('interval SECOND(3)', (4096 << 16) | 3, 1186, 'v'),
                 ('interval DAY TO SECOND(3)[]', (7176 << 16) | 3, 1187, 'a'))
        for target, modifier, oid, column in specs:
            for expression in ('CAST(' + column + ' AS ' + target + ')', '(' + column + ')::' + target):
                for mode in ('simple', 'describe', 'execute'):
                    operation(mode, 'SELECT ' + expression + ' AS v FROM ' + table, oid, modifier, ())
            for mode in ('simple', 'execute'):
                operation(mode, 'SELECT CAST(' + column + ' AS ' + target + ') AS v FROM ' + table + ' LIMIT 0', oid, modifier, ())
        for target, _, _, _ in specs[:2] + (('interval DAY TO SECOND(3)', (7176 << 16) | 3, 1186, 'v'),):
            for mode in ('simple', 'execute'):
                operation(mode, "SELECT CAST(NULL AS " + target + ")+INTERVAL '0' AS v", 1186, -1)
        review.query('SELECT currval(' + sequence + ')', state='55000')
        effect = 'SELECT CAST(CAST(nextval(' + sequence + ') AS text) AS interval DAY) AS v'
        operation('describe', effect, 1186, (8 << 16) | 65535, (b'00:00:00',))
        operation('portal', effect, 1186, (8 << 16) | 65535, (b'00:00:00',))
        review.query('SELECT currval(' + sequence + ')', state='55000')
        operation('execute', effect, 1186, (8 << 16) | 65535, (b'00:00:00',))
        review.query('SELECT currval(' + sequence + ')', rows=[['1']], oids=[20])
        review.query('DROP SCHEMA ' + identifier(schema) + ' CASCADE', rows=[], tag='DROP SCHEMA')
        owned = False
        review.complete(360)
    finally:
        if owned:
            client.simple_query(sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
