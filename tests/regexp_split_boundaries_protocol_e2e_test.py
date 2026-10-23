#!/usr/bin/env python3
"""Complete scalar split-boundary fixture; also runs against PostgreSQL 18."""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, identifier
import sys
import uuid


SAMPLES = (
    ('', ',', ('',)), ('', '', ('',)), ('abc', ',', ('abc',)),
    ('a,', ',', ('a', '')), (',a', ',', ('', 'a')), (',,', ',', ('', '', '')),
    ('a,,b,', ',', ('a', '', 'b', '')), ('abc', '', ('a', 'b', 'c')),
    ('abc', 'x*', ('a', 'b', 'c')), ('aaa', 'a*', ('', '')),
    ('aba', 'a*', ('', 'b', '')), ('a', 'a?', ('', '')), ('abc', 'b*', ('a', 'c')),
    ('abbc', 'b*', ('a', 'c')), ('abc', '^', ('abc',)), ('abc', '$', ('abc',)),
    ('abc', '^|$', ('abc',)), ('abc', '(^)|($)', ('abc',)), ('a\nb\n', '\n', ('a', 'b', '')),
    ('a,b,', '(,)', ('a', 'b', '')), ('aaXbbX', 'x', ('aa', 'bb', ''), 'i'),
    ('é界😀', '', ('é', '界', '😀')), ('é界😀', 'x*', ('é', '界', '😀')),
    ('é界😀', '界', ('é', '😀')), ('é界😀界', '界', ('é', '😀', '')),
    ('éé', '', ('e', '́', 'é')), ('NULL,"x",a\\b,', ',', ('NULL', '"x"', 'a\\b', '')),
    ('a<>b<>', '<>', ('a', 'b', '')), ('a  b  ', ' +', ('a', 'b', '')),
)


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def array(fields):
    def quote(value):
        if not value or value.lower() == 'null' or any(c.isspace() or c in ',{}"\\' for c in value):
            return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
        return value
    return '{' + ','.join(quote(value) for value in fields) + '}'


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('REGEXP_SPLIT_BOUNDARIES_PROTOCOL')
    wire = Wire(review)
    schema = 'regexp_split_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    owned = False
    try:
        for mode in ('simple', 'extended', 'binary'):
            for sample in SAMPLES:
                source, pattern, fields, *flags = sample
                arguments = ','.join(literal(v) for v in (source, pattern, *flags))
                wire.query(mode, 'SELECT CAST(regexp_split_to_array(' + arguments + ') AS text) AS v;', (array(fields),), 25)
            wire.query(mode, "SELECT CAST(regexp_split_to_array('a,b,  '::char(7),',  '::char(3)) AS text) AS v;", ('{a,b,""}',), 25)
            for arguments in ("NULL::text,'(','z'", "'abc',NULL::text,'z'", "'abc','(',NULL::text"):
                wire.query(mode, 'SELECT CAST(regexp_split_to_array(' + arguments + ') AS text) AS v;', (None,), 25)
        for arguments, state in (("'abc','('", '2201B'), ("'abc',',','g'", '22023'), ("'abc',',','z'", '22023')):
            review.query('SELECT regexp_split_to_array(' + arguments + ') AS v;', state=state)
        # The raw array result is checked too; CAST is not a substitute for the
        # function's own SQL result type and array identity.
        review.query("SELECT regexp_split_to_array('a,',',') AS v;", rows=[['{a,""}']], oids=[1009])
        review.describe("SELECT regexp_split_to_array($1::text,$2::text) AS v;", oids=[1009], parameter_oids=(25, 25))
        statement = b'regexp_split_boundaries'
        sql = 'SELECT CAST(regexp_split_to_array($1::text,$2::text) AS text) AS v;'
        wire.describe(statement, sql, 25, (25, 25))
        wire.bind_describe(statement, 25, (25, 25), ('a,', ','), False, False)
        for binary in (False, True):
            for source, pattern, expected in (('a,', ',', '{a,""}'), ('é界😀', '', '{é,界,😀}'), ('aaa', 'a*', '{"",""}'), (None, '(', None)):
                wire.execute(statement, 25, (expected,), (25, 25), (source, pattern), binary_input=binary, binary_output=binary)
        wire.close(statement)
        previous = len(review.failures)
        wire.command('CREATE SCHEMA ' + identifier(schema) + ';', 'CREATE SCHEMA')
        owned = len(review.failures) == previous
        assert owned, 'private schema ownership not confirmed'
        wire.command('CREATE TABLE ' + table + '(id integer,s text,p text);', 'CREATE TABLE')
        wire.command('INSERT INTO ' + table + " VALUES(1,'a,',','),(2,'é界😀',''),(3,'aaa','a*'),(4,NULL,'(');", 'INSERT 0 4')
        for mode in ('simple', 'extended', 'binary'):
            wire.query(mode, 'SELECT CAST(regexp_split_to_array(s,p) AS text) AS v FROM ' + table + ' ORDER BY id;', ('{a,""}', '{é,界,😀}', '{"",""}', None), 25)
            wire.query(mode, 'SELECT CAST(regexp_split_to_array(s,p) AS text) AS v FROM ' + table + ' WHERE false;', (), 25)
            wire.query(mode, 'SELECT CAST(regexp_split_to_array(s,p) AS text) AS v FROM ' + table + ' LIMIT 0;', (), 25)
        wire.command('DROP SCHEMA ' + identifier(schema) + ' CASCADE;', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(128)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
