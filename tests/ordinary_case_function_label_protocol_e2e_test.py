#!/usr/bin/env python3
"""Ordinary case_when callees keep function labels; CASE remains grammar.

All original 28 diagnostic scenarios are retained in the fuller matrix.
Each arity has an owned schema: same-schema overload DDL is separately tracked
in ordinary_case_function_overload_protocol_draft.py, unchanged and still strict.
Explicit paths keep user-deferred default search_path/role work out of scope.
"""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire, MODES, identifier
import sys
import uuid


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('ORDINARY_CASE_FUNCTION_LABEL_PROTOCOL')
    wire = Wire(review)
    schema = 'case_function_' + uuid.uuid4().hex
    owner = identifier(schema)
    table, empty, sequence = owner + '.rows_', owner + '.empty_', owner + '.effects_'
    function_schemas = {arity: (schema + suffix, identifier(schema + suffix)) for arity, suffix in ((1, ''), (2, '_two'), (3, '_three'), (0, '_zero'))}
    owned = False
    try:
        before = len(review.failures)
        wire.command('CREATE SCHEMA ' + owner, 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned, 'private namespace not owned'
        for arity in (2, 3, 0):
            wire.command('CREATE SCHEMA ' + function_schemas[arity][1], 'CREATE SCHEMA')
        wire.command('CREATE TABLE ' + table + '(id integer,n integer)', 'CREATE TABLE')
        wire.command('CREATE TABLE ' + empty + '(id integer,n integer)', 'CREATE TABLE')
        wire.command('INSERT INTO ' + table + ' VALUES(1,-2),(2,0),(3,NULL),(4,5)', 'INSERT 0 4')
        for arity, (arguments, body) in enumerate((('p integer', 'p+1'), ('p integer,q integer', 'p+q'),
                                ('p integer,q integer,r integer', 'p+q+r')), 1):
            wire.command('CREATE FUNCTION ' + function_schemas[arity][1] + '.case_when(' + arguments + ') RETURNS integer '
                         'LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT ' + body + '$$', 'CREATE FUNCTION')
        wire.command('CREATE SEQUENCE ' + sequence, 'CREATE SEQUENCE')
        wire.command('CREATE FUNCTION ' + function_schemas[0][1] + ".case_when() RETURNS integer LANGUAGE SQL VOLATILE AS $$SELECT nextval('" + sequence + "')::integer$$", 'CREATE FUNCTION')
        wire.command('SET search_path=public,' + schema + ',pg_catalog', 'SET',
                     parameter_status='public, ' + schema + ', pg_catalog')
        callees = ('case_when', '"case_when"', '{owner}.case_when', '{owner}."case_when"',
                   'CASE_WHEN', '{schema}.CASE_WHEN')
        shapes = (
            ('-2', 'n', -1, (-1, 1, None, 6), ((-2,), (0,), (None,)), (-1, 1, None)),
            ('-2,4', 'n,1', 2, (-1, 1, None, 6), ((-2, 4), (0, 1), (None, 4)), (2, 1, None)),
            ('-2,1,4', 'n,1,n', 3, (-3, 1, None, 11), ((-2, 1, 4), (0, 1, 2), (None, 1, 2)), (3, 3, None)),
        )
        for callee_index, spelling in enumerate(callees):
            for arity, (literal, column, scalar, rows, parameters, results) in enumerate(shapes, 1):
                role_schema, role_owner = function_schemas[arity]
                callee = spelling.format(owner=role_owner, schema=role_schema)
                wire.command('SET search_path=public,' + role_schema + ',pg_catalog', 'SET',
                             parameter_status='public, ' + role_schema + ', pg_catalog')
                call, row_call = callee + '(' + literal + ')', callee + '(' + column + ')'
                tail = ' FROM ' + table
                cases = (
                    (call, '', (scalar,), 23, 'case_when'),
                    ('CAST(' + call + ' AS bigint)', '', (scalar,), 20, 'case_when'),
                    (call + '::integer', '', (scalar,), 23, 'case_when'),
                    ('(' + call + ')', '', (scalar,), 23, 'case_when'),
                    (call + '+0', '', (scalar,), 23, '?column?'),
                    ('CASE WHEN true THEN -1 ELSE ' + call + ' END', '', (-1,), 23, 'case_when'),
                    ('CASE WHEN true THEN ' + call + ' ELSE 0 END', '', (scalar,), 23, 'case'),
                    ('CAST(CASE WHEN false THEN 0 ELSE ' + call + ' END AS bigint)', '', (scalar,), 20, 'case_when'),
                    (row_call, tail + ' ORDER BY id', rows, 23, 'case_when'),
                    ('CAST(' + row_call + ' AS bigint)', tail + ' ORDER BY id', rows, 20, 'case_when'),
                    (row_call, ' FROM ' + empty, (), 23, 'case_when'),
                    (row_call, tail + ' WHERE false', (), 23, 'case_when'),
                    (row_call, tail + ' LIMIT 0', (), 23, 'case_when'),
                )
                assert len(cases) == 13
                for mode in MODES:
                    for expression, suffix, values, oid, label in cases:
                        for alias in (False, True):
                            wire.name = 'Explicit Name' if alias else label
                            wire.query(mode, 'SELECT ' + expression + (' AS "Explicit Name"' if alias else '') + suffix, values, oid)
                wire.name = 'case_when'
                statement = ('case_function_' + str(callee_index) + '_' + str(arity)).encode()
                sql = 'SELECT ' + callee + '(' + ','.join('$' + str(i) for i in range(1, arity + 1)) + ')'
                inputs = (23,) * arity
                wire.describe(statement, sql, 23, inputs)
                wire.bind_describe(statement, 23, inputs, parameters[0], False, False)
                for binary_input in (False, True):
                    for binary_output in (False, True):
                        for values, result in zip(parameters, results):
                            wire.execute(statement, 23, (result,), inputs, values, binary_input, binary_output)
                wire.close(statement)
        # A function's actual name becomes a usable CTE/derived-table field.
        for arity, (literal, _, scalar, _, _, _) in enumerate(shapes, 1):
            role_schema, _ = function_schemas[arity]
            wire.command('SET search_path=public,' + role_schema + ',pg_catalog', 'SET',
                         parameter_status='public, ' + role_schema + ', pg_catalog')
            call = 'case_when(' + literal + ')'
            for prefix, tail in (('WITH named AS (SELECT ' + call + ') SELECT ', ' FROM named'),
                                 ('SELECT ', ' FROM (SELECT ' + call + ') AS named')):
                for mode in MODES:
                    for alias in (False, True):
                        wire.name = 'Explicit Name' if alias else 'case_when'
                        wire.query(mode, prefix + '"case_when"' + (' AS "Explicit Name"' if alias else '') + tail, (scalar,), 23)
        # Genuine CASE and its strong/weak ELSE-name rules remain intact.
        wire.command('SET search_path=public,' + schema + ',pg_catalog', 'SET',
                     parameter_status='public, ' + schema + ', pg_catalog')
        real_cases = (
            ('CASE WHEN true THEN 1 ELSE 2 END', '', (1,), 23, 'case'),
            ('CAST(CASE WHEN true THEN 1 ELSE 2 END AS bigint)', '', (1,), 20, 'int8'),
            ('CASE WHEN false THEN 1 ELSE abs(-2) END', '', (2,), 23, 'abs'),
            ('CASE WHEN true THEN 1 ELSE case_when(-2) END', '', (1,), 23, 'case_when'),
            ('CASE WHEN false THEN n ELSE case_when(n) END', ' FROM ' + table + ' ORDER BY id', (-1, 1, None, 6), 23, 'case_when'),
            ('CASE WHEN false THEN case_when(n) ELSE n END', ' FROM ' + table + ' ORDER BY id', (-2, 0, None, 5), 23, 'n'),
            ('CASE WHEN false THEN case_when(n) ELSE n::bigint END', ' FROM ' + table + ' ORDER BY id', (-2, 0, None, 5), 20, 'n'),
            ('CASE WHEN true THEN 1 ELSE (CASE WHEN false THEN 2 ELSE case_when(-2) END) END', '', (1,), 23, 'case_when'),
            ('CAST(CASE WHEN true THEN 1 ELSE case_when(-2) END AS bigint)', '', (1,), 20, 'case_when'),
            ('CASE WHEN true THEN 1 ELSE NULL END', '', (1,), 23, 'case'),
        )
        assert len(real_cases) == 10
        for mode in MODES:
            for expression, suffix, values, oid, label in real_cases:
                for alias in (False, True):
                    wire.name = 'Explicit Name' if alias else label
                    wire.query(mode, 'SELECT ' + expression + (' AS "Explicit Name"' if alias else '') + suffix, values, oid)
        # Preparing/describing/zero-row execution must not execute a volatile
        # same-named routine or an undemanded CASE arm to obtain a name.
        wire.name = 'case_when'
        role_schema, _ = function_schemas[0]
        wire.command('SET search_path=public,' + role_schema + ',pg_catalog', 'SET',
                     parameter_status='public, ' + role_schema + ', pg_catalog')
        statement = b'case_function_effects'
        wire.describe(statement, 'SELECT case_when()', 23)
        wire.bind_describe(statement, 23, (), (), False, True)
        for mode in MODES:
            for suffix in (' FROM ' + empty, ' FROM ' + table + ' WHERE false', ' FROM ' + table + ' LIMIT 0'):
                wire.query(mode, 'SELECT case_when()' + suffix, (), 23)
        wire.name = 'v'
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v", (1,), 20)
        for binary_output, value, next_value in ((False, 2, 3), (True, 4, 5)):
            wire.name = 'case_when'
            wire.execute(statement, 23, (value,), binary_output=binary_output)
            wire.name = 'v'
            wire.query('simple', "SELECT nextval('" + sequence + "') AS v", (next_value,), 20)
        wire.name = 'case'
        for mode in MODES:
            wire.query(mode, 'SELECT CASE WHEN false THEN case_when() ELSE 9 END', (9,), 23)
        wire.name = 'v'
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v", (6,), 20)
        wire.name = 'case_when'
        for mode in MODES:
            wire.query(mode, 'SELECT CASE WHEN true THEN 9 ELSE case_when() END', (9,), 23)
        wire.name = 'v'
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v", (7,), 20)
        wire.name = 'case_when'
        wire.query('binary', 'SELECT case_when() FROM ' + table + ' ORDER BY id', (8, 9, 10, 11), 23)
        wire.name = 'v'
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v", (12,), 20)
        wire.close(statement)
        for arity in (0, 3, 2, 1):
            wire.command('DROP SCHEMA ' + function_schemas[arity][1] + ' CASCADE', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(1837)
    finally:
        if owned:
            for arity in (0, 3, 2, 1):
                review.client.simple_query(review.sock, 'DROP SCHEMA ' + function_schemas[arity][1] + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
