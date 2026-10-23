#!/usr/bin/env python3
"""SPLIT_PART field selection without materializing every other field.

Exact values/metadata are compared with PG18; native allocation observations
test the resource bound separately, without timing or RSS thresholds.
"""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, identifier
import sys
import uuid


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('SPLIT_PART_BOUNDED_MEMORY_PROTOCOL')
    wire = Wire(review)
    schema = 'split_part_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    owned = False
    samples = (
        ('', ',', ('',)), ('a,b,c', ',', ('a', 'b', 'c')),
        (',a,,', ',', ('', 'a', '', '')), ('aaa', 'aa', ('', 'a')),
        ('aaaaa', 'aa', ('', '', 'a')), ('a<>b<>', '<>', ('a', 'b', '')),
        ('é界😀界z', '界', ('é', '😀', 'z')), ('unchanged', '', ('unchanged',)),
    )
    try:
        for mode in ('simple', 'extended', 'binary'):
            for source, delimiter, fields in samples:
                for position in (-2147483648, -5, -3, -2, -1, 1, 2, 3, 5, 2147483647):
                    index = position - 1 if position > 0 else len(fields) + position
                    expected = fields[index] if 0 <= index < len(fields) else ''
                    sql = 'SELECT split_part(' + literal(source) + ',' + literal(delimiter) + ',' + str(position) + ') AS v;'
                    wire.query(mode, sql, (expected,), 25)
            for expression in ("NULL::text,',',0", "'a,b',NULL::text,0", "'a,b',',',NULL::integer"):
                wire.query(mode, 'SELECT split_part(' + expression + ') AS v;', (None,), 25)
            for position, expected in ((1, 'a'), (-1, 'b')):
                wire.query(mode, "SELECT split_part('a,b  '::char(5),',  '::char(3)," + str(position) + ') AS v;', (expected,), 25)
            for count in (65536, 262144):
                for position, expected in ((1, 'x'), (2, 'x'), (-1, 'last'), (-2, 'x'), (-2147483648, '')):
                    sql = "SELECT CASE WHEN split_part(repeat('x,'," + str(count) + ")||'last',','," + str(position)
                    sql += ')=' + literal(expected) + " THEN 'same' ELSE 'different' END AS v;"
                    wire.query(mode, sql, ('same',), 25)
        for source, delimiter, _ in samples:
            review.query('SELECT split_part(' + literal(source) + ',' + literal(delimiter) + ',0) AS v;', state='22023')

        statement = b'split_part_positions'
        inputs = (25, 25, 23)
        wire.describe(statement, 'SELECT split_part($1::text,$2::text,$3::integer) AS v;', 25, inputs)
        wire.bind_describe(statement, 25, inputs, ('aaa', 'aa', -1), False, False)
        parameters = (
            ('aaa', 'aa', -1, 'a'), ('a,b,c', ',', 2, 'b'), ('', ',', -1, ''),
            ('é界😀界z', '界', -2, '😀'), ('a,b', ',', -2147483648, ''), (None, ',', 0, None),
        )
        for binary in (False, True):
            for source, delimiter, position, expected in parameters:
                wire.execute(statement, 25, (expected,), inputs, (source, delimiter, position),
                             binary_input=binary, binary_output=binary)
        wire.close(statement)

        previous = len(review.failures)
        wire.command('CREATE SCHEMA ' + identifier(schema) + ';', 'CREATE SCHEMA')
        owned = len(review.failures) == previous
        assert owned, 'private schema ownership not confirmed'
        wire.command('CREATE TABLE ' + table + '(id integer,s text,d text,n integer);', 'CREATE TABLE')
        wire.command('INSERT INTO ' + table + " VALUES(1,'a,b,c',',',2),(2,'aaa','aa',-1),(3,'a<>b<>','<>',-1),(4,NULL,',',0);", 'INSERT 0 4')
        for mode in ('simple', 'extended', 'binary'):
            wire.query(mode, 'SELECT split_part(s,d,n) AS v FROM ' + table + ' ORDER BY id;', ('b', 'a', '', None), 25)
            wire.query(mode, 'SELECT split_part(s,d,n) AS v FROM ' + table + ' WHERE false;', (), 25)
            wire.query(mode, 'SELECT split_part(s,d,n) AS v FROM ' + table + ' LIMIT 0;', (), 25)
        wire.command('DROP SCHEMA ' + identifier(schema) + ' CASCADE;', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(321)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
