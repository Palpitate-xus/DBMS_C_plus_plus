#!/usr/bin/env python3
"""Empty repeat fast path: values/NULL, validation, prepared input and effects.

No wall-clock threshold is an assertion: complete protocol results must agree
with PostgreSQL, which also spends time in empty appends at large counts.
"""

from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire, identifier
import time
import uuid


LABEL = 'REPEAT_EMPTY_INPUT_PROTOCOL'
CONTROLS = 80
MODES = ('simple', 'extended', 'binary')


def main():
    review = Review(LABEL)
    wire = Wire(review)
    schema = 'repeat_empty_' + uuid.uuid4().hex
    table = identifier(schema) + '.rows_'
    sequence = identifier(schema) + '.effects_'
    owned = False
    try:
        cases = (
            ("repeat('', 1)", ''),
            ("repeat('', 2147483647)", ''),
            ("repeat('', -2147483648)", ''),
            ("repeat('', 0)", ''),
            ("repeat(''::varchar, 134217728)", ''),
            ("repeat('   '::char(3), 16777216)", ''),
            ("repeat(NULL::text, 2147483647)", None),
            ("repeat('', NULL::integer)", None),
            ("repeat('é界', 3)", 'é界é界é界'),
            ("repeat('ab', 3)", 'ababab'),
            ("repeat('NULL', 2)", 'NULLNULL'),
            ("repeat('ab', 0)", ''),
            ("repeat('ab', -1)", ''),
        )
        for mode in MODES:
            for expression, value in cases:
                start = time.monotonic_ns()
                wire.query(mode, 'SELECT ' + expression + ' AS v;', (value,), 25)
                print('REPEAT_ELAPSED_NS', mode, expression,
                      time.monotonic_ns() - start, flush=True)

        # Empty input must not bypass input syntax/range validation.
        review.query("SELECT repeat('', '2x') AS v;", state='22P02')
        review.query("SELECT repeat('', '2147483648') AS v;", state='22003')

        statement = b'repeat_empty_parameters'
        wire.describe(statement, 'SELECT repeat($1, $2) AS v;', 25, (25, 23))
        wire.bind_describe(statement, 25, (25, 23), ('', 1), False, False)
        for binary in (False, True):
            for text, count, value in (
                ('', 2147483647, ''),
                ('   ', 2, '      '),
                (None, 2147483647, None),
                ('NULL', 2, 'NULLNULL'),
                ('é界', 3, 'é界é界é界'),
                ('', None, None),
            ):
                wire.execute(statement, 25, (value,), (25, 23), (text, count),
                             binary_input=binary, binary_output=binary)
        wire.close(statement)

        previous = len(review.failures)
        wire.command('CREATE SCHEMA ' + identifier(schema) + ';', 'CREATE SCHEMA')
        owned = len(review.failures) == previous
        assert owned, 'private schema ownership not confirmed'
        wire.command('CREATE TABLE ' + table + '(id integer, s text, n integer);', 'CREATE TABLE')
        wire.command('INSERT INTO ' + table + " VALUES(1,'',16777216),"
                     "(2,NULL,2147483647),(3,'é界',3),(4,'NULL',2);", 'INSERT 0 4')
        for mode in MODES:
            wire.query(mode, 'SELECT repeat(s,n) AS v FROM ' + table + ' ORDER BY id;',
                       ('', None, 'é界é界é界', 'NULLNULL'), 25)
            wire.query(mode, 'SELECT repeat(s,n) AS v FROM ' + table + ' WHERE false;', (), 25)
            wire.query(mode, 'SELECT repeat(s,n) AS v FROM ' + table + ' LIMIT 0;', (), 25)

        wire.command('CREATE SEQUENCE ' + sequence + ';', 'CREATE SEQUENCE')
        effect_sql = "SELECT repeat('',nextval('" + sequence + "')::integer) AS v;"
        effect_statement = b'repeat_empty_effects'
        wire.describe(effect_statement, effect_sql, 25)
        wire.bind_describe(effect_statement, 25, (), (), False, False)
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v;", (1,), 20)
        for mode, expected in zip(MODES, (3, 5, 7)):
            wire.query(mode, effect_sql, ('',), 25)
            wire.query('simple', "SELECT nextval('" + sequence + "') AS v;", (expected,), 20)
        wire.close(effect_statement)

        wire.command('DROP SCHEMA ' + identifier(schema) + ' CASCADE;', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(CONTROLS)
    finally:
        if owned:
            # Only the UUID namespace whose creation was confirmed is in scope.
            review.client.simple_query(review.sock,
                'DROP SCHEMA ' + identifier(schema) + ' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
