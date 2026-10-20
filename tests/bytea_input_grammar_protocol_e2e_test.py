#!/usr/bin/env python3
"""BYTEA input grammar, values, SQLSTATE and successful wire metadata.

Invalid-input checks intentionally own error class, not the independently
documented Parse/Bind/Execute timing difference (see R208).
"""
from bytea_hex_sqlstate_protocol_e2e_test import CASES as BASE_CASES, Wire, literal, source
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import MODES, identifier
import sys
import uuid


def cases():
    result = list(BASE_CASES)
    result += [(value, None, '22P02') for value in ('\\X', '\\X00', '\\XAF', '\\X00ff', '\\X \t')]
    for space in ('\v', '\f'):
        result += [(value, None, '22023') for value in
                   ('\\x'+space+'00', '\\x00'+space+'ff', '\\x00'+space, '\\x0'+space+'0')]
    for space in (' ', '\t', '\n', '\r'):
        result += [('\\x'+space+'00', '\\x00', None),
                   ('\\x00'+space+'ff', '\\x00ff', None),
                   ('\\x00'+space, '\\x00', None)]
    result += [('\v', '\\x0b', None), ('\f', '\\x0c', None), ('\\x0b0c', '\\x0b0c', None)]
    assert len(result) == 51
    assert sum(state is None for _, _, state in result) == 25
    return result


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('BYTEA_INPUT_GRAMMAR_PROTOCOL')
    wire = Wire(review)
    inputs = cases()
    schema = 'bytea_grammar_'+uuid.uuid4().hex
    qualified = identifier(schema)
    table = qualified+'.rows_'
    owned = False
    try:
        for type_name in ('unknown', 'text', 'varchar', 'name'):
            for value, wanted, state in inputs:
                sql = 'SELECT CAST('+source(literal(value), type_name)+' AS bytea) AS v;'
                wire.query('simple', sql, () if state else (wanted,), 17, state)
                if not state:
                    for mode in ('extended', 'binary'):
                        wire.query(mode, sql, (wanted,), 17)
        for input_oid in (25, 17):
            statement = ('bytea_grammar_parameters_'+str(input_oid)).encode()
            sql = 'SELECT '+('CAST($1 AS bytea)' if input_oid == 25 else '$1')+' AS v;'
            wire.describe(statement, sql, 17, (input_oid,))
            wire.bind_describe(statement, 17, (input_oid,), ('\\x00',), False, False)
            for binary_output in (False, True):
                for value, wanted, state in inputs:
                    if not state:
                        wire.execute(statement, 17, (wanted,), (input_oid,), (value,),
                                     binary_input=False, binary_output=binary_output)
            wire.close(statement)
        before = len(review.failures)
        wire.command('CREATE SCHEMA '+qualified+';', 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned, 'private schema ownership not confirmed'
        wire.command('CREATE TABLE '+table+'(id integer,t text);', 'CREATE TABLE')
        wire.command('INSERT INTO '+table+' VALUES'+','.join(
            '('+str(index)+','+literal(value)+')'
            for index, (value, _, _) in enumerate(inputs, 1))+';', 'INSERT 0 51')
        for type_name in ('unknown', 'text', 'varchar', 'name'):
            for index, (_, wanted, state) in enumerate(inputs, 1):
                sql = 'SELECT CAST('+source('t', type_name)+' AS bytea) AS v FROM '+table+' WHERE id='+str(index)+';'
                wire.query('simple', sql, () if state else (wanted,), 17, state)
                if not state:
                    for mode in ('extended', 'binary'):
                        wire.query(mode, sql, (wanted,), 17)
        for mode in MODES:
            for suffix in (' WHERE false', ' LIMIT 0'):
                wire.query(mode, 'SELECT CAST(t AS bytea) AS v FROM '+table+suffix+';', (), 17)
        for type_name, width in (('smallint', 2), ('integer', 4), ('bigint', 8)):
            for value in (0, -1, 1):
                wanted = '\\x'+value.to_bytes(width, 'big', signed=True).hex()
                for mode in MODES:
                    wire.query(mode, 'SELECT CAST(CAST('+str(value)+' AS '+type_name+') AS bytea) AS v;', (wanted,), 17)
        wire.command('DROP SCHEMA '+qualified+' CASCADE;', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(951)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA '+qualified+' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
