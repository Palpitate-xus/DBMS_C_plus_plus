#!/usr/bin/env python3
"""NAME/NAME[] binary sends: exact bytes, NULL, metadata and array envelopes.

Retained inputs use TEXT (OID25), not a claim about NAME binary receive.
R210 defaults and R211 unqualified current_schema()/quoted labels remain open.
"""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire as BaseWire, MODES, WIDTHS, identifier
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows
from array_binary_result_protocol_e2e_test import binary_array
import struct
import sys
import uuid

WIDTHS[19] = 64
WIDTHS[1003] = -1
VALUES = (None, '', 'plain', 'two words', 'NULL', 'a,b', 'quote"', 'slash\\', 'é', '中文', '😀',
          'n'*63, 'n'*62+'x', '界'*21, '😀'*15+'abc', '\tline\nbreak\r')


def literal(value):
    return 'NULL' if value is None else "'"+value.replace("'", "''")+"'"


def array_quote(value):
    return '"'+value.replace('\\', '\\\\').replace('"', '\\"')+'"'


def arrays():
    result = [(None, None)]
    for value in VALUES[1:]:
        result.append(('{'+array_quote(value)+',NULL,"NULL"}',
                       binary_array(19, [value.encode(), None, b'NULL'])))
    assert len(result) == 16
    return result


class Wire(BaseWire):
    def result_ok(self, messages, values, oid, state=None, binary=False,
                  parsed=False, bound=False, simple=False, inputs=()):
        if oid != 1003:
            return super().result_ok(messages, values, oid, state, binary, parsed, bound, simple, inputs)
        assert binary and state is None
        rows, well_formed = strict_data_rows(messages)
        prefix = [b'1', b't', b'T'] if parsed else []
        descriptions = [self.description(oid)] if parsed else []
        if bound:
            prefix += [b'2', b'T']
            descriptions.append(self.description(oid, True))
        return (well_formed and not self.errors(messages) and rows == [[value] for value in values] and
                [kind for kind, _ in messages] == prefix+[b'D']*len(values)+[b'C', b'Z'] and
                [body for kind, body in messages if kind == b'T'] == descriptions and
                [body for kind, body in messages if kind == b't'] == ([self.parameters(inputs)] if parsed else []) and
                [body for kind, body in messages if kind == b'C'] == [('SELECT '+str(len(values))).encode()+b'\0'] and
                all(body == b'' for kind, body in messages if kind in (b'1', b'2')) and messages[-1:] == [(b'Z', b'I')])


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('NAME_BINARY_RESULT_PROTOCOL')
    wire = Wire(review)
    user = review.runner._reference_connection_settings()[2] if sys.argv[1:] else 'alice'
    schema = 'name_send_'+uuid.uuid4().hex
    qualified = identifier(schema)
    table = qualified+'.rows_'
    owned = False
    try:
        assert len(VALUES) == 16
        wire.command('SET search_path=public;', 'SET', parameter_status='public')
        for value in VALUES:
            for mode in MODES:
                wire.query(mode, 'SELECT CAST('+literal(value)+' AS name) AS v;', (value,), 19)
        for text, encoded in arrays():
            wire.query('binary', 'SELECT CAST('+literal(text)+' AS name[]) AS v;', (encoded,), 1003)
        for expression, wanted in (('current_user', user), ('session_user', user), ('current_role', user),
                                   ('current_schema', 'public'), ('pg_catalog.current_schema()', 'public')):
            for mode in MODES:
                wire.query(mode, 'SELECT '+expression+' AS v;', (wanted,), 19)
        statement = b'name_send_text_input'
        wire.describe(statement, 'SELECT CAST($1 AS name) AS v;', 19, (25,))
        for binary_input in (False, True):
            for binary_output in (False, True):
                for value in VALUES:
                    wire.execute(statement, 19, (value,), (25,), (value,), binary_input, binary_output)
        wire.close(statement)
        statement = b'name_array_send_text_input'
        wire.describe(statement, 'SELECT CAST($1 AS name[]) AS v;', 1003, (25,))
        for binary_input in (False, True):
            for text, encoded in arrays():
                wire.execute(statement, 1003, (encoded,), (25,), (text,), binary_input, True)
        wire.close(statement)
        before = len(review.failures)
        wire.command('CREATE SCHEMA '+qualified+';', 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned, 'private namespace ownership not confirmed'
        wire.command('CREATE TABLE '+table+'(id integer,n name,a name[]);', 'CREATE TABLE')
        rows = ','.join('('+str(index)+',CAST('+literal(value)+' AS name),CAST('+literal(array_text)+' AS name[]))'
                        for index, (value, (array_text, _)) in enumerate(zip(VALUES, arrays()), 1))
        wire.command('INSERT INTO '+table+' VALUES'+rows+';', 'INSERT 0 16')
        for index, (value, (_, encoded)) in enumerate(zip(VALUES, arrays()), 1):
            suffix = ' FROM '+table+' WHERE id='+str(index)+';'
            for mode in MODES:
                wire.query(mode, 'SELECT CAST(CAST(n AS text) AS name) AS v'+suffix, (value,), 19)
            wire.query('binary', 'SELECT CAST(CAST(a AS text) AS name[]) AS v'+suffix, (encoded,), 1003)
        for mode in MODES:
            wire.query(mode, 'SELECT CAST(CAST(n AS text) AS name) AS v FROM '+table+' WHERE false;', (), 19)
        wire.query('binary', 'SELECT CAST(CAST(a AS text) AS name[]) AS v FROM '+table+' WHERE false;', (), 1003)
        shapes = (
            ('{}', binary_array(19, [])),
            ('{NULL,NULL}', binary_array(19, [None, None])),
            ('[-2:-1]={left,NULL}', binary_array(19, [b'left', None], [(2, -2)])),
            ('[0:1][-2:-1]={{left,NULL},{"NULL",""}}', binary_array(19, [b'left', None, b'NULL', b''], [(2, 0), (2, -2)])),
            ('{{{{{{left}}}}}}', binary_array(19, [b'left'], [(1, 1)]*6)))
        for text, encoded in shapes:
            wire.query('binary', 'SELECT CAST('+literal(text)+' AS name[]) AS v;', (encoded,), 1003)
        wire.command('DROP SCHEMA '+qualified+' CASCADE;', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(257)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA '+qualified+' CASCADE;')
        review.close()


if __name__ == '__main__':
    main()
