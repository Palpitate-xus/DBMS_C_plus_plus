#!/usr/bin/env python3
"""Quoted pg_catalog scalar spelling must not turn numeric descriptors into text.

Run the entire fixture against a privately owned PostgreSQL 18 instance before
acceptance. Explicit paths leave user-deferred search_path defaults out of scope.
"""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire, MODES, WIDTHS, identifier
import sys
import uuid

WIDTHS[21] = 2
CALLEES = ('abs', '"abs"', 'pg_catalog.abs', '"pg_catalog".abs',
           'pg_catalog."abs"', '"pg_catalog"."abs"', 'PG_CATALOG.ABS', '"pg_catalog".ABS')
TYPES = (('integer', 23), ('bigint', 20), ('double precision', 701))
EXPECTED_CONTROLS = 576


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('QUOTED_CATALOG_SCALAR_DESCRIPTOR_PROTOCOL')
    wire = Wire(review)
    schema = 'r216_' + uuid.uuid4().hex
    owner = identifier(schema)
    owned = False
    try:
        before = len(review.failures)
        wire.command('CREATE SCHEMA ' + owner, 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned, 'private namespace creation not confirmed'
        wire.command('SET search_path=pg_catalog,public', 'SET',
                     parameter_status='pg_catalog, public')
        tables = []
        for index, (type_name, oid) in enumerate(TYPES):
            table, empty = owner + '.rows_' + str(index), owner + '.empty_' + str(index)
            wire.command('CREATE TABLE ' + table + '(id integer,n ' + type_name + ')', 'CREATE TABLE')
            wire.command('CREATE TABLE ' + empty + '(id integer,n ' + type_name + ')', 'CREATE TABLE')
            wire.command('INSERT INTO ' + table + ' VALUES(1,-2),(2,0),(3,NULL),(4,5)', 'INSERT 0 4')
            tables.append((type_name, oid, table, empty))
        for callee in CALLEES:
            for type_name, oid, table, empty in tables:
                scalar = callee + '((-2)::' + type_name + ')'
                rows = callee + '(n)'
                for mode in MODES:
                    for expression, tail, values in (
                            (scalar, '', (2,)),
                            (callee + '(NULL::' + type_name + ')', '', (None,)),
                            (rows, ' FROM ' + table + ' ORDER BY id', (2, 0, None, 5)),
                            (rows, ' FROM ' + empty, ()),
                            (rows, ' FROM ' + table + ' WHERE false', ()),
                            (rows, ' FROM ' + table + ' LIMIT 0', ())):
                        wire.query(mode, 'SELECT ' + expression + ' AS v' + tail, values, oid)
                statement = ('r216_' + str(review.controls)).encode()
                wire.describe(statement, 'SELECT ' + rows + ' AS v FROM ' + table, oid)
                wire.bind_describe(statement, oid, (), (), False, False)
                wire.bind_describe(statement, oid, (), (), False, True)
                wire.close(statement)
        # Stored declarations must still precede builtin-name fallbacks.
        wire.command('CREATE FUNCTION ' + owner + '.abs(p integer) RETURNS text '
                     'LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT \'shadow\'::text$$', 'CREATE FUNCTION')
        wire.command('SET search_path=' + schema + ',pg_catalog', 'SET',
                     parameter_status=schema + ', pg_catalog')
        table = tables[0][2]
        for callee in ('abs', '"abs"', owner + '."abs"'):
            for mode in MODES:
                for tail, values in ((' ORDER BY id', ('shadow', 'shadow', None, 'shadow')),
                                     (' WHERE false', ()), (' LIMIT 0', ())):
                    wire.query(mode, 'SELECT ' + callee + '(n) AS v FROM ' + table + tail, values, 25)
        wire.command('SET search_path=pg_catalog,public', 'SET',
                     parameter_status='pg_catalog, public')
        for mode in MODES:
            for tail in ('', ' FROM ' + tables[0][3]):
                wire.query(mode, 'SELECT "pg_catalog"."Abs"(-2) AS v' + tail, (), 23, '42883')
        wire.command('DROP SCHEMA ' + owner + ' CASCADE', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(EXPECTED_CONTROLS)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + owner + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
