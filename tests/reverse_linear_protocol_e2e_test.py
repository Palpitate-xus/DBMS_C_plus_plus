#!/usr/bin/env python3
"""Linear UTF-8 reversal: exact code points/metadata, rows and prepared effects.

Elapsed observations are diagnostic only. No timing threshold is an assertion.
BYTEA remains byte-oriented; combining sequences remain code-point-oriented.
"""

from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, identifier
import time
import uuid


LABEL = 'REVERSE_LINEAR_PROTOCOL'
CONTROLS = 96
MODES = ('simple', 'extended', 'binary')


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def main():
    review = Review(LABEL)
    wire = Wire(review)
    schema = 'reverse_linear_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    sequence = identifier(schema) + '.effects_'
    owned = False
    try:
        samples = ('', 'a', 'abc', 'aé界😀', 'é', '́a', '\U0010ffff', 'Привет', '👩‍💻', ' a  ')
        cases = [(literal(value), value[::-1]) for value in samples]
        cases += [("'ab  '::char(4)", 'ba'), ("'ab  '::varchar", '  ba')]
        for mode in MODES:
            for expression, value in cases:
                wire.query(mode, 'SELECT reverse(' + expression + ') AS v;', (value,), 25)
            for source, expected in (('', ''), ('00c3a9ff', 'ffa9c300'), ('000000', '000000'), ('0123456789abcdef', 'efcdab8967452301')):
                wire.query(mode, "SELECT encode(reverse('\\x" + source + "'::bytea),'hex') AS v;", (expected,), 25)
            for count in (2048, 4096, 8192):
                sql = "SELECT CASE WHEN reverse(repeat('aé界😀'," + str(count) + "))=repeat('😀界éa'," + str(count) + ") THEN 'same' ELSE 'different' END AS v;"
                started = time.monotonic_ns()
                wire.query(mode, sql, ('same',), 25)
                print('REVERSE_LINEAR_ELAPSED_NS', mode, count, time.monotonic_ns() - started, flush=True)

        statement = b'reverse_linear_parameters'
        wire.describe(statement, 'SELECT reverse($1::text) AS v;', 25, (25,))
        wire.bind_describe(statement, 25, (25,), ('aé界😀',), False, False)
        for binary in (False, True):
            for value in ('', 'abc', 'aé界😀', 'é', ' a  ', None):
                expected = None if value is None else value[::-1]
                wire.execute(statement, 25, (expected,), (25,), (value,), binary_input=binary, binary_output=binary)
        wire.close(statement)

        previous = len(review.failures)
        wire.command('CREATE SCHEMA ' + identifier(schema) + ';', 'CREATE SCHEMA')
        owned = len(review.failures) == previous
        assert owned, 'private schema ownership not confirmed'
        wire.command('CREATE TABLE ' + table + '(id integer,s text);', 'CREATE TABLE')
        wire.command('INSERT INTO ' + table + " VALUES(1,'aé界😀'),(2,NULL),(3,''),(4,' a  ');", 'INSERT 0 4')
        for mode in MODES:
            wire.query(mode, 'SELECT reverse(s) AS v FROM ' + table + ' ORDER BY id;', ('😀界éa', None, '', '  a '), 25)
            wire.query(mode, 'SELECT reverse(s) AS v FROM ' + table + ' WHERE false;', (), 25)
            wire.query(mode, 'SELECT reverse(s) AS v FROM ' + table + ' LIMIT 0;', (), 25)

        wire.command('CREATE SEQUENCE ' + sequence + ';', 'CREATE SEQUENCE')
        effect = "SELECT reverse(nextval('" + sequence + "')::text) AS v;"
        effect_statement = b'reverse_linear_effects'
        wire.describe(effect_statement, effect, 25)
        wire.bind_describe(effect_statement, 25, (), (), False, False)
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v;", (1,), 20)
        for mode, current, following in zip(MODES, ('2', '4', '6'), (3, 5, 7)):
            wire.query(mode, effect, (current,), 25)
            wire.query('simple', "SELECT nextval('" + sequence + "') AS v;", (following,), 20)
        wire.close(effect_statement)
        wire.command('DROP SCHEMA ' + identifier(schema) + ' CASCADE;', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(CONTROLS)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + identifier(schema) + ' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
