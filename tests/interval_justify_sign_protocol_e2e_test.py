#!/usr/bin/env python3
"""All justify functions: exact fields, signs, overflow and wire ownership."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows, identifier
import itertools
import struct
import sys
import uuid

DAY = 86400000000
NAMES = ('justify_hours', 'justify_days', 'justify_interval')
SQL = 'SELECT justify_hours($1::interval) AS h,justify_days($1::interval) AS d,justify_interval($1::interval) AS i'
INFINITIES = ((2**31-1, 2**31-1, 2**63-1), (-2**31, -2**31, -2**63))


def quotient(value, denominator):
    return (-1 if value < 0 else 1) * (abs(value) // denominator)


def normal(fields, mode):
    if fields in INFINITIES:
        return fields
    months, days, micros = fields
    if mode == 0:
        total = days * DAY + micros
        days = quotient(total, DAY); micros = total - days * DAY
    elif mode == 1:
        total = months * 30 + days
        months = quotient(total, 30); days = total - months * 30
    else:
        total = (months * 30 + days) * DAY + micros
        months = quotient(total, 30 * DAY)
        remainder = total - months * 30 * DAY
        days = quotient(remainder, DAY); micros = remainder - days * DAY
    return months, days, micros


def text(fields):
    if fields in INFINITIES:
        return 'infinity' if fields == INFINITIES[0] else '-infinity'
    months, days, micros = fields
    parts, previous_negative = [], False
    for value, singular in ((quotient(months, 12), 'year'), (months - quotient(months, 12) * 12, 'mon'), (days, 'day')):
        if value:
            parts.append(('+' if previous_negative and value > 0 else '') + str(value) + ' ' + singular + ('' if value == 1 else 's'))
            previous_negative = value < 0
    if micros or not parts:
        magnitude = abs(micros)
        clock = ('-' if micros < 0 else '+' if previous_negative else '')
        clock += f'{magnitude // 3600000000:02d}:{magnitude // 60000000 % 60:02d}:{magnitude // 1000000 % 60:02d}'
        if magnitude % 1000000:
            clock += ('.' + str(magnitude % 1000000).zfill(6)).rstrip('0')
        parts.append(clock)
    return ' '.join(parts)


def source(fields):
    if fields in INFINITIES:
        return text(fields)
    return f'{fields[0]} months {fields[1]} days {fields[2]} microseconds'


def payload(fields):
    months, days, micros = fields
    return struct.pack('!qii', micros, days, months)


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('INTERVAL_JUSTIFY_SIGN_PROTOCOL')
    client, sock = review.client, review.sock
    schema = 'justify_' + uuid.uuid4().hex
    owned = False

    def exchange(fields, binary):
        # Each operation reuses the named statement; every supplied interval
        # has the genuine interval OID and a text or exact 16-byte input datum.
        raw = None if fields is None else payload(fields) if binary else source(fields).encode()
        bind = b'\0justify_sign\0' + struct.pack('!HHH', 1, int(binary), 1)
        bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
        bind += struct.pack('!HH', 1, int(binary))
        sock.sendall(client.typed(b'B', bind) + client.typed(b'D', b'P\0') + client.typed(b'E', b'\0' + struct.pack('!I', 0)) + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        rows, valid = strict_data_rows(messages)
        expected = [None] * 3 if fields is None else [payload(normal(fields, mode)) if binary else text(normal(fields, mode)).encode() for mode in range(3)]
        wanted = struct.pack('!H', 3) + b''.join(name + b'\0' + struct.pack('!IhIhih', 0, 0, 1186, 16, -1, int(binary)) for name in (b'h', b'd', b'i'))
        good = (valid and rows == [expected] and [body for kind, body in messages if kind == b'T'] == [wanted] and
                [body for kind, body in messages if kind == b'C'] == [b'SELECT 1\0'] and
                not any(kind == b'E' for kind, _ in messages) and sum(kind == b'2' for kind, _ in messages) == 1)
        review.record(('BOUND', fields, binary), good, (rows, [(kind, body) for kind, body in messages if kind in (b'E', b'T')]), expected)

    try:
        combinations = tuple(itertools.product((-13, -1, 0, 1, 13), (-61, -30, -1, 0, 1, 30, 61), (-DAY-1, -DAY, -3600000000, -1, 0, 1, 3600000000, DAY, DAY+1)))
        assert len(combinations) == 315
        for fields in combinations:
            arguments = "INTERVAL '" + source(fields) + "'"
            review.query('SELECT ' + ','.join(name + '(' + arguments + ')' for name in NAMES), rows=[[text(normal(fields, mode)) for mode in range(3)]], oids=[1186] * 3)
        parse = b'justify_sign\0' + SQL.encode() + b'\0' + struct.pack('!HI', 1, 1186)
        sock.sendall(client.typed(b'P', parse) + client.typed(b'D', b'Sjustify_sign\0') + client.typed(b'S'))
        messages = client.read_until_ready(sock)
        fields = client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        review.record('PREPARE/DESCRIBE', [field[3:] for field in fields] == [(1186, 16, -1, 0)] * 3 and
                      not any(kind in (b'D', b'C', b'E') for kind, _ in messages), fields, [(1186, 16, -1, 0)] * 3)
        for binary in (False, True):
            for fields in combinations:
                exchange(fields, binary)
            exchange(None, binary)
            for fields in INFINITIES + ((2**31-1,0,0), (-2**31,0,0), (0,2**31-1,0), (0,-2**31,0), (0,0,2**63-1), (0,0,-2**63)):
                exchange(fields, binary)
        for name in NAMES:
            review.query('SELECT ' + name + '(NULL::interval)', rows=[[None]], oids=[1186])
            for infinity in ('infinity', '-infinity'):
                review.query("SELECT " + name + "(INTERVAL '" + infinity + "')", rows=[[infinity]], oids=[1186])
        for name, value in (('justify_hours', '2147483647 days 24 hours'), ('justify_hours', '-2147483648 days -24 hours'),
                            ('justify_days', '2147483647 months 30 days'), ('justify_days', '-2147483648 months -30 days'),
                            ('justify_interval', '2147483647 months 30 days'), ('justify_interval', '-2147483648 months -30 days')):
            review.query("SELECT " + name + "(INTERVAL '" + value + "')", state='22008')
        for name, value, expected in (
            ('justify_interval', '2147483647 days 24 hours', '5965232 years 4 mons 8 days'),
            ('justify_interval', '-2147483648 days -24 hours', '-5965232 years -4 mons -9 days'),
            ('justify_interval', '2147483647 months 30 days -1 microsecond', None),
            ('justify_interval', '-2147483648 months -30 days 1 microsecond', None),
            ('justify_days', '-9223372036854775808 microseconds', '-2562047788:00:54.775808')):
            review.query("SELECT " + name + "(INTERVAL '" + value + "')", rows=[[expected]] if expected is not None else None,
                         oids=[1186] if expected is not None else None, state='22008' if expected is None else None)
        for name in NAMES:
            review.query("SELECT " + name + "(INTERVAL '9223372036854775807 days')", state='22015')
        previous = len(review.failures)
        review.query('CREATE SCHEMA ' + identifier(schema), rows=[], tag='CREATE SCHEMA')
        assert len(review.failures) == previous, 'private schema ownership not confirmed'
        owned = True
        table = identifier(schema) + '.rows_'
        review.query('CREATE TABLE ' + table + '(id integer,v interval)', rows=[], tag='CREATE TABLE')
        review.query('INSERT INTO ' + table + " VALUES(1,'1 mon -1 day'),(2,'-1 mon 1 day'),(3,'1 day -1 hour'),(4,NULL)", rows=[], tag='INSERT 0 4')
        for name, mode in zip(NAMES, range(3)):
            rows = [[text(normal(value, mode))] for value in ((1, -1, 0), (-1, 1, 0), (0, 1, -3600000000))] + [[None]]
            review.query('SELECT ' + name + '(v) FROM ' + table + ' ORDER BY id', rows=rows, oids=[1186])
            review.query('SELECT ' + name + '(v) FROM ' + table + ' WHERE false', rows=[], oids=[1186])
        review.query('DROP SCHEMA ' + identifier(schema) + ' CASCADE', rows=[], tag='DROP SCHEMA')
        owned = False
        review.complete(997)
    finally:
        if owned:
            client.simple_query(sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
