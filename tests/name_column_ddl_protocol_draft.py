#!/usr/bin/env python3
"""Real builtin NAME column declarations, attributes, rows and type ownership."""
from compat.numeric_type_review import Review
from name_binary_result_protocol_e2e_test import Wire, literal
from trim_keyword_identity_protocol_e2e_test import MODES, identifier
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows
from array_binary_result_protocol_e2e_test import binary_array
import sys
import uuid

SPELLINGS = ('name', 'NAME', '"name"', 'pg_catalog.name', '"pg_catalog"."name"', 'pg_catalog."name"', 'pg_catalog.NAME')
MODIFIERS = ('1', '0', '-1', "'x'", 'a', '1,2')
BAD = (('"NAME"', '42704'), ('pg_catalog."NAME"', '42704'),
       ('"PG_CATALOG".name', '3F000'), ('r214_absent.name', '3F000'), ('"Name"', '42704'))


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('NAME_COLUMN_DDL_PROTOCOL')
    wire = Wire(review)
    schema = 'name_ddl_'+uuid.uuid4().hex
    q = identifier(schema)
    shadow = schema+'_s'
    qs = identifier(shadow)
    owned = []

    def error(sql, state):
        # Exact error class and no successful result; unrelated error phases
        # are not silently turned into a broader compatibility assertion.
        def valid(messages):
            rows, well_formed = strict_data_rows(messages)
            return (well_formed and not rows and wire.errors(messages) == [state] and
                    not any(kind == b'C' for kind, _ in messages) and messages[-1:] == [(b'Z', b'I')])
        wire.collect(('SQLSTATE_ONLY', sql), lambda: wire.client.simple_query(wire.sock, sql), valid, state)

    def scalar(table, value, condition):
        for mode in MODES:
            wire.query(mode, 'SELECT CAST(CAST(n AS text) AS name) AS v FROM '+table+' WHERE '+condition+';', (value,), 19)

    try:
        wire.command('SET search_path=public;', 'SET', parameter_status='public')
        before = len(review.failures)
        wire.command('CREATE SCHEMA '+q+';', 'CREATE SCHEMA')
        if len(review.failures) == before:
            owned.append(schema)
        assert owned, 'private namespace setup not confirmed'
        for index, type_name in enumerate(SPELLINGS):
            table = q+'.t_'+str(index)
            wire.command('CREATE TABLE '+table+'(i integer PRIMARY KEY,n '+type_name+
                         " NOT NULL DEFAULT 'seed'::name CHECK(n <> 'bad'::name),a "+type_name+'[]);', 'CREATE TABLE')
            wire.command('INSERT INTO '+table+'(i) VALUES(1);', 'INSERT 0 1')
            scalar(table, 'seed', 'i=1')
            wire.query('binary', 'SELECT CAST(CAST(a AS text) AS name[]) AS v FROM '+table+' WHERE i=1;', (None,), 1003)
            wire.command('INSERT INTO '+table+" VALUES(2,'NULL','{\"\",\"NULL\",NULL,\"界\"}');", 'INSERT 0 1')
            scalar(table, 'NULL', 'i=2')
            wire.query('binary', 'SELECT CAST(CAST(a AS text) AS name[]) AS v FROM '+table+' WHERE i=2;',
                       (binary_array(19, [b'', b'NULL', None, '界'.encode()]),), 1003)
            wire.command('UPDATE '+table+" SET n='' WHERE i=2;", 'UPDATE 1')
            scalar(table, '', 'i=2')
            wire.command('UPDATE '+table+' SET n=DEFAULT WHERE i=2;', 'UPDATE 1')
            scalar(table, 'seed', 'i=2')
            error('INSERT INTO '+table+'(i,n) VALUES(3,NULL);', '23502')
            error('INSERT INTO '+table+"(i,n) VALUES(4,'bad');", '23514')
            wire.query('simple', 'SELECT CAST(count(*) AS text) AS v FROM '+table+' WHERE i IN(3,4);', ('0',), 25)
            wire.command('DROP TABLE '+table+';', 'DROP TABLE')
        for type_name in SPELLINGS:
            for modifiers in MODIFIERS:
                for array in ('', '[]'):
                    error('CREATE TABLE '+q+'.bad(n '+type_name+'('+modifiers+')'+array+');', '42601')
        for type_name, state in BAD:
            error('CREATE TABLE '+q+'.bad_identity(n '+type_name+');', state)
        before = len(review.failures)
        wire.command('CREATE SCHEMA '+qs+';', 'CREATE SCHEMA')
        if len(review.failures) == before:
            owned.append(shadow)
        assert shadow in owned, 'private shadow namespace setup not confirmed'
        wire.command('CREATE DOMAIN '+qs+'.name AS integer CHECK(VALUE > 0);', 'CREATE DOMAIN')
        wire.command('SET search_path='+qs+',pg_catalog,public;', 'SET', parameter_status=shadow+', pg_catalog, public')
        wire.command('CREATE TABLE '+q+'.domain_choice(v name);', 'CREATE TABLE')
        wire.command('INSERT INTO '+q+'.domain_choice VALUES(7);', 'INSERT 0 1')
        wire.query('simple', 'SELECT CAST(v AS text) AS v FROM '+q+'.domain_choice;', ('7',), 25)
        error('INSERT INTO '+q+'.domain_choice VALUES(-1);', '23514')
        wire.query('simple', 'SELECT CAST(count(*) AS text) AS v FROM '+q+'.domain_choice;', ('1',), 25)
        wire.command('SET search_path=pg_catalog,'+qs+',public;', 'SET', parameter_status='pg_catalog, '+shadow+', public')
        wire.command('CREATE TABLE '+q+'.builtin_choice(v name);', 'CREATE TABLE')
        wire.command('INSERT INTO '+q+".builtin_choice VALUES('word');", 'INSERT 0 1')
        for mode in MODES:
            wire.query(mode, 'SELECT CAST(CAST(v AS text) AS name) AS v FROM '+q+'.builtin_choice;', ('word',), 19)
        wire.command('SET search_path='+qs+',pg_catalog,public;', 'SET', parameter_status=shadow+', pg_catalog, public')
        wire.command('CREATE TABLE '+q+'.qualified_choice(v pg_catalog.name);', 'CREATE TABLE')
        wire.command('INSERT INTO '+q+".qualified_choice VALUES('word');", 'INSERT 0 1')
        for mode in MODES:
            wire.query(mode, 'SELECT CAST(CAST(v AS text) AS pg_catalog.name) AS v FROM '+q+'.qualified_choice;', ('word',), 19)
        for table in ('domain_choice', 'builtin_choice', 'qualified_choice'):
            wire.command('DROP TABLE '+q+'.'+table+';', 'DROP TABLE')
        wire.command('DROP DOMAIN '+qs+'.name;', 'DROP DOMAIN')
        wire.command('SET search_path=public;', 'SET', parameter_status='public')
        wire.command('DROP SCHEMA '+qs+';', 'DROP SCHEMA')
        owned.remove(shadow)
        wire.command('DROP SCHEMA '+q+';', 'DROP SCHEMA')
        owned.remove(schema)
        review.complete(279)
    finally:
        for name in reversed(owned):
            review.client.simple_query(review.sock, 'DROP SCHEMA '+identifier(name)+' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
