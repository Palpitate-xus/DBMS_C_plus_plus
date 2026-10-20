#!/usr/bin/env python3
"""Default function result labels must use the SQL identifier, not its spelling.

Full fixture is also run on a privately owned PostgreSQL 18 instance. Explicit
paths avoid making R210 defaults or R211 current_schema values part of this fix.
This does not change the already-decoded native SelectExpr callback contract.
"""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire, MODES, WIDTHS, identifier
import sys
import uuid

WIDTHS[19] = 64
NAMES = ('r213_lower', 'MiXeD', 'Name.With.Dot', 'Say"Hi', 'two words',
         '模式 R213', 'NULL', 'case_when')
EXPECTED_CONTROLS = 1262


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('QUOTED_FUNCTION_RESULT_LABEL_PROTOCOL')
    wire = Wire(review)
    schema = 'r213_' + uuid.uuid4().hex
    owner = identifier(schema)
    table, empty = owner + '.rows_', owner + '.empty_'
    owned = False
    try:
        before = len(review.failures)
        wire.command('CREATE SCHEMA ' + owner, 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned, 'private namespace creation not confirmed'
        wire.command('CREATE TABLE ' + table + '(id integer,n integer)', 'CREATE TABLE')
        wire.command('CREATE TABLE ' + empty + '(id integer,n integer)', 'CREATE TABLE')
        wire.command('INSERT INTO ' + table + ' VALUES(1,-2),(2,0),(3,NULL),(4,5)', 'INSERT 0 4')
        wire.command('SET search_path=public,' + schema + ',pg_catalog', 'SET',
                     parameter_status='public, ' + schema + ', pg_catalog')
        calls = []
        for name in NAMES:
            spelling = identifier(name)
            wire.command('CREATE FUNCTION ' + owner + '.' + spelling +
                         '(p integer) RETURNS integer LANGUAGE SQL IMMUTABLE STRICT '
                         'AS $$SELECT p+1$$', 'CREATE FUNCTION')
            for callee in (spelling, owner + '.' + spelling):
                calls.append((callee, name, '-2', 'n', -1, (-1, 1, None, 6), 23))
        for callee in ('R213_LOWER', schema + '.R213_LOWER'):
            calls.append((callee, 'r213_lower', '-2', 'n', -1, (-1, 1, None, 6), 23))
        for callee in ('abs', '"abs"', 'pg_catalog.abs', '"pg_catalog"."abs"'):
            calls.append((callee, 'abs', '-2', 'n', 2, (2, 0, None, 5), 23))
        for callee in ('current_schema', '"current_schema"', 'pg_catalog.current_schema',
                       '"pg_catalog"."current_schema"'):
            calls.append((callee, 'current_schema', '', '', 'public', ('public',) * 4, 19))
        assert len(calls) == 26
        for callee, label, literal, column, scalar, rows, oid in calls:
            scalar_call = callee + '(' + literal + ')'
            row_call = callee + '(' + column + ')'
            tail = ' FROM ' + table
            text = lambda v: None if v is None else str(v)
            cases = (
                (scalar_call, '', (scalar,), oid, label),
                ('CAST(' + scalar_call + ' AS text)', '', (text(scalar),), 25, label),
                ('(' + scalar_call + " || '')", '', (text(scalar),), 25, '?column?'),
                (row_call, tail + ' ORDER BY id', rows, oid, label),
                ('CAST(' + row_call + ' AS text)', tail + ' ORDER BY id',
                 tuple(map(text, rows)), 25, label),
                (row_call, ' FROM ' + empty, (), oid, label),
                (row_call, tail + ' WHERE false', (), oid, label),
                (row_call, tail + ' LIMIT 0', (), oid, label))
            assert len(cases) == 8
            for mode in MODES:
                for expression, suffix, values, result_oid, default_label in cases:
                    for alias in (False, True):
                        wire.name = 'Out Name' if alias else default_label
                        wire.query(mode, 'SELECT ' + expression +
                                   (' AS "Out Name"' if alias else '') + suffix,
                                   values, result_oid)
        wire.name = 'v'
        wire.command('DROP SCHEMA ' + owner + ' CASCADE', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(EXPECTED_CONTROLS)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + owner + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
