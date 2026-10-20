#!/usr/bin/env python3
"""Resolve current_schema() through its real scalar/namespace context.

The built-in search_path default is a separate open issue (R210). Every path
below is set explicitly, not inferred from the candidate's SHOW output.
"""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire, MODES, WIDTHS, identifier
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows
import sys

WIDTHS[19] = 64
FORMS = ('current_schema', 'current_schema()', 'pg_catalog.current_schema()',
         '"current_schema"()', 'pg_catalog."current_schema"()', 'current_schema ( )')


def canonical(entries):
    return ', '.join(value if value and value != '$user' and
                     all('a' <= c <= 'z' or '0' <= c <= '9' or c == '_' for c in value)
                     else identifier(value) for value in entries)


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('CURRENT_SCHEMA_PROJECTION_PROTOCOL')
    wire = Wire(review)
    user = review.runner._reference_connection_settings()[2] if sys.argv[1:] else 'alice'
    schemas = (user, 'r211_a', 'R211.B', 'R211.Dot', 'R211.Quote"', '模式 R211', 'NULL', 'two words')
    owned = []
    table = 'public.r211_schema_rows'
    created_table = False

    def path(entries):
        sql = "SET search_path TO '';" if entries == ('',) else 'SET search_path TO '+', '.join(identifier(v) for v in entries)+';'
        wire.command(sql, 'SET', parameter_status=canonical(entries))

    def value(mode, expression, wanted, alias=True, suffix='', oid=19):
        wire.name = 'v' if alias else 'current_schema'
        wire.query(mode, 'SELECT '+expression+(' AS v' if alias else '')+suffix+';', wanted, oid)
        wire.name = 'v'

    def error(sql, state):
        # Error class only; this task does not change Parse/Bind error phases.
        def valid(messages):
            rows, well_formed = strict_data_rows(messages)
            return (well_formed and not rows and wire.errors(messages) == [state] and
                    not any(kind == b'C' for kind, _ in messages) and messages[-1:] == [(b'Z', b'I')])
        wire.collect(('SQLSTATE_ONLY', sql), lambda: wire.client.simple_query(wire.sock, sql), valid, state)

    try:
        for schema in schemas:
            before = len(review.failures)
            wire.command('CREATE SCHEMA '+identifier(schema)+';', 'CREATE SCHEMA')
            if len(review.failures) == before:
                owned.append(schema)
        assert len(owned) == len(schemas), 'private namespace setup not confirmed'
        before = len(review.failures)
        wire.command('CREATE TABLE '+table+'(id integer);', 'CREATE TABLE')
        created_table = len(review.failures) == before
        assert created_table, 'owned row source setup not confirmed'
        wire.command('INSERT INTO '+table+' VALUES(1),(2);', 'INSERT 0 2')
        scopes = (
            (('public',), 'public'), (('r211_absent',), None), (('',), None),
            (('pg_catalog',), 'pg_catalog'), (('r211_a', 'public'), 'r211_a'),
            (('r211_absent', 'R211.B', 'public'), 'R211.B'), (('$user', 'public'), user),
            (('R211.Dot', 'public'), 'R211.Dot'), (('R211.Quote"', 'public'), 'R211.Quote"'),
            (('模式 R211', 'public'), '模式 R211'), (('NULL', 'public'), 'NULL'),
            (('two words', 'public'), 'two words'))
        assert len(scopes) == 12 and len(FORMS) == 6
        for entries, wanted in scopes:
            path(entries)
            for mode in MODES:
                for expression in FORMS:
                    for alias in (False, True):
                        value(mode, expression, (wanted,), alias)
                value(mode, 'current_schema()', (wanted, wanted), suffix=' FROM '+table+' ORDER BY id')
                value(mode, 'current_schema()', (), suffix=' FROM '+table+' WHERE false')
                value(mode, 'CAST(current_schema() AS text)', (wanted,), oid=25)
        path(('r211_a', 'pg_catalog', 'public'))
        for wanted, body in (('shadow', "'shadow'"), (None, 'NULL'), ('', "''"), ('NULL', "'NULL'")):
            wire.command('CREATE OR REPLACE FUNCTION r211_a.current_schema() RETURNS name LANGUAGE SQL VOLATILE AS $$SELECT '+body+'::name$$;', 'CREATE FUNCTION')
            for mode in MODES:
                for expression in FORMS:
                    actual = 'r211_a' if expression == 'current_schema' or expression.startswith('pg_catalog.') else wanted
                    for alias in (False, True):
                        value(mode, expression, (actual,), alias)
        for expression in ('current_schema(1)', 'pg_catalog.current_schema(1)', '"CURRENT_SCHEMA"()'):
            error('SELECT '+expression+' AS v WHERE false;', '42883')
        wire.command('DROP FUNCTION r211_a.current_schema();', 'DROP FUNCTION')
        for mode in MODES:
            for expression in FORMS:
                for alias in (False, True):
                    value(mode, expression, ('r211_a',), alias)
        path(('public',))
        for schema in reversed(owned):
            wire.command('DROP SCHEMA '+identifier(schema)+' CASCADE;', 'DROP SCHEMA', notices=True)
        owned.clear()
        review.complete(760)
    finally:
        for schema in reversed(owned):
            review.client.simple_query(review.sock, 'DROP SCHEMA '+identifier(schema)+' CASCADE;')
        # The table is privately owned and unique to this owned server. It is
        # intentionally not reused as evidence from another connection/run.
        if created_table:
            review.client.simple_query(review.sock, 'DROP TABLE '+table+';')
        review.close()


if __name__ == '__main__':
    main()
