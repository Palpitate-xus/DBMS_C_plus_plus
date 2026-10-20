#!/usr/bin/env python3
"""Counter-based execution demand for a shadowing current_schema routine."""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire, MODES, WIDTHS, identifier
import sys
import uuid

WIDTHS[19] = 64


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('CURRENT_SCHEMA_DEMAND_PROTOCOL')
    wire = Wire(review)
    schema = 'schema_demand_' + uuid.uuid4().hex
    owner = identifier(schema)
    seq = owner + '.effects'
    literal = "'" + schema + ".effects'"
    owned = False
    try:
        before = len(review.failures)
        wire.command('CREATE SCHEMA ' + owner, 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned
        wire.command('CREATE SEQUENCE ' + seq + ' START WITH 1000', 'CREATE SEQUENCE')
        wire.command('CREATE FUNCTION ' + owner + '.current_schema() RETURNS name '
                     'LANGUAGE SQL VOLATILE AS $$SELECT nextval(' + literal + ')::text::name$$',
                     'CREATE FUNCTION')
        wire.command('CREATE TABLE ' + owner + '.empty_(id integer)', 'CREATE TABLE')
        wire.command('CREATE TABLE ' + owner + '.rows_(id integer)', 'CREATE TABLE')
        wire.command('INSERT INTO ' + owner + '.rows_ VALUES(1),(2)', 'INSERT 0 2')
        wire.command('SET search_path=' + schema + ',pg_catalog,public', 'SET',
                     parameter_status=schema + ', pg_catalog, public')
        for mode in MODES:
            for suffix in (' WHERE false', ' LIMIT 0', ' FROM ' + owner + '.empty_'):
                wire.query(mode, 'SELECT current_schema() AS v' + suffix, (), 19)
        statement = b'schema_demand'
        wire.describe(statement, 'SELECT current_schema() AS v', 19)
        for binary in (False, True):
            wire.bind_describe(statement, 19, (), (), False, binary)
        wire.close(statement)
        wire.query('simple', 'SELECT nextval(' + literal + ') AS v', (1000,), 20)
        expected = 1001
        for mode in MODES:
            wire.query(mode, 'SELECT current_schema() AS v', (str(expected),), 19)
            expected += 1
        for mode in MODES:
            wire.query(mode, 'SELECT current_schema() AS v FROM ' + owner + '.rows_ ORDER BY id',
                       (str(expected), str(expected + 1)), 19)
            expected += 2
        assert expected == 1010
        wire.query('simple', 'SELECT nextval(' + literal + ') AS v', (expected,), 20)
        wire.command('DROP SCHEMA ' + owner + ' CASCADE', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(29)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + owner + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
