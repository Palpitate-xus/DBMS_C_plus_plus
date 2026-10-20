#!/usr/bin/env python3
"""Actual || result declarations, specialization, NULLs and demand boundaries.

Compare this complete fixture with private PostgreSQL 18. This is not an
assertion that every PostgreSQL operator/signature is implemented.
"""
from compat.numeric_type_review import Review
from trim_keyword_identity_protocol_e2e_test import Wire as BaseWire, MODES, identifier
from quoted_unqualified_scalar_projection_protocol_e2e_test import strict_data_rows
from array_binary_result_protocol_e2e_test import binary_array
import re
import struct
import sys
import uuid

WIDTHS = {17: -1, 20: 8, 23: 4, 25: -1, 1043: -1,
          1560: -1, 1562: -1, 1007: -1, 1016: -1, 1009: -1}
ELEMENTS = {1007: 23, 1016: 20, 1009: 25}
EXPECTED_CONTROLS = 709


def array_text(values):
    cells = []
    for value in values:
        if value is None:
            cells.append('NULL')
            continue
        value = str(value)
        if not value or value.upper() == 'NULL' or re.search(r'[\s,{}"\\]', value):
            value = '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
        cells.append(value)
    return '{' + ','.join(cells) + '}'


def datum(value, oid, binary):
    if value is None:
        return None
    if oid in ELEMENTS:
        return (binary_array(ELEMENTS[oid], [datum(v, ELEMENTS[oid], True) for v in value])
                if binary else array_text(value).encode())
    if oid == 17:
        return value if binary else ('\\x' + value.hex()).encode()
    if oid in (1560, 1562):
        if not binary:
            return value.encode()
        width = (len(value) + 7) // 8
        return struct.pack('!i', len(value)) + (
            int(value.ljust(width * 8, '0'), 2).to_bytes(width, 'big') if width else b'')
    if binary and oid in (20, 23):
        return struct.pack('!q' if oid == 20 else '!i', value)
    return str(value).encode()


class Wire(BaseWire):
    def description(self, oid, binary=False):
        return struct.pack('!H', 1) + b'v\0' + struct.pack('!IhIhih', 0, 0, oid, WIDTHS[oid], -1, int(binary))

    def query(self, mode, sql, values=(), oid=25, state=None):
        assert state is None
        binary = mode == 'binary'
        producer = (lambda: self.client.simple_query(self.sock, sql)) if mode == 'simple' else (
            lambda: self.exchange(self.parse(b'', sql) + self.bind(b'', binary_output=binary)))
        self.collect((mode, sql), producer,
                     lambda messages: self.result_ok(messages, values, oid, binary=binary,
                         parsed=mode != 'simple', bound=mode != 'simple', simple=mode == 'simple'),
                     (values, oid, WIDTHS[oid], binary))

    def execute(self, statement, oid, values, inputs=(), parameters=(), binary_input=False,
                binary_output=False):
        self.collect(('BIND/EXECUTE', statement, parameters, binary_input, binary_output),
                     lambda: self.exchange(self.bind(statement, inputs, parameters, binary_input, binary_output)),
                     lambda messages: self.result_ok(messages, values, oid, binary=binary_output, bound=True),
                     (values, oid, WIDTHS[oid], binary_output))

    def describe_inferred(self, statement, sql, oid, parameter_oid):
        self.collect(('INFERRED PARSE/DESCRIBE', statement, sql),
                     lambda: self.exchange(self.parse(statement, sql, (0,))),
                     lambda messages: messages == [(b'1', b''), (b't', self.parameters((parameter_oid,))),
                                                    (b'T', self.description(oid)), (b'Z', b'I')],
                     (oid, parameter_oid, 'actual inferred parameter, no execution'))

    def bind(self, statement, inputs=(), values=(), binary_input=False,
             binary_output=False, execute=True):
        assert len(inputs) == len(values)
        body = b'\0' + statement + b'\0' + struct.pack('!H', len(inputs))
        body += b''.join(struct.pack('!H', int(binary_input)) for _ in inputs)
        body += struct.pack('!H', len(values))
        for oid, value in zip(inputs, values):
            raw = datum(value, oid, binary_input)
            body += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
        body += struct.pack('!HH', 1, int(binary_output))
        packet = self.client.typed(b'B', body) + self.client.typed(b'D', b'P\0')
        if execute:
            packet += self.client.typed(b'E', b'\0' + struct.pack('!I', 0))
        return packet

    def result_ok(self, messages, values, oid, state=None, binary=False,
                  parsed=False, bound=False, simple=False, inputs=()):
        assert state is None
        rows, valid_rows = strict_data_rows(messages)
        prefix = [b'T'] if simple else [b'1', b't', b'T'] if parsed else []
        descriptions = [self.description(oid)] if parsed else []
        if simple or bound:
            descriptions.append(self.description(oid, binary))
        if bound:
            prefix += [b'2', b'T']
        return (valid_rows and not self.errors(messages) and
                [kind for kind, _ in messages] == prefix + [b'D'] * len(values) + [b'C', b'Z'] and
                [body for kind, body in messages if kind == b'T'] == descriptions and
                [body for kind, body in messages if kind == b't'] == ([self.parameters(inputs)] if parsed else []) and
                [body for kind, body in messages if kind == b'C'] == [('SELECT ' + str(len(values))).encode() + b'\0'] and
                rows == [[datum(value, oid, binary)] for value in values] and
                messages[-1:] == [(b'Z', b'I')] and
                all(body == b'' for kind, body in messages if kind in (b'1', b'2')))


def constants(owner):
    return (
        ("1 || ''", '1', 25), ("'' || 1", '1', 25),
        ("9007199254740993::bigint || ''", '9007199254740993', 25),
        ("true || ''", 'true', 25), ("'' || false", 'false', 25),
        ("NULL::boolean || ''", None, 25), ("NULL::integer || ''", None, 25),
        ("1 || NULL::text", None, 25), ("NULL || NULL", None, 25),
        ("'x' || NULL", None, 25), ("NULL || 'x'", None, 25),
        ("'NULL'::text || ''", 'NULL', 25), ("'NULL'::name || ''", 'NULL', 25),
        ("'模式'::name || '界'", '模式界', 25),
        ("'a'::varchar(8) || 'b'::varchar(3)", 'ab', 25),
        ("' a '::char(6) || 'z'", ' az', 25),
        ("DATE '2000-01-01' || 'x'", '2000-01-01x', 25),
        ("2.50::numeric(8,2) || ''", '2.50', 25),
        ("('x' || 1) || 'y'", 'x1y', 25),
        ("length(1 || 'x')", 2, 23),
        ("'\\x00ff'::bytea || '\\x80'::bytea", bytes.fromhex('00ff80'), 17),
        ("NULL::bytea || '\\x'::bytea", None, 17),
        ("'\\x'::bytea || '\\x'::bytea", b'', 17),
        ("'\\x00ff'::bytea || '\\x80'", bytes.fromhex('00ff80'), 17),
        ("'\\x80' || '\\x00ff'::bytea", bytes.fromhex('8000ff'), 17),
        ("NULL || '\\x00'::bytea", None, 17),
        ("B'101' || B'01'", '10101', 1562),
        ("B'10'::varbit || B'1'::varbit", '101', 1562),
        ("B''::varbit || B''::varbit", '', 1562),
        ("NULL::varbit || B'1'::varbit", None, 1562),
        ("B'10' || '1'", '101', 1562), ("'1' || B'10'", '110', 1562),
        ("NULL || B'10'", None, 1562),
        ("B'10' || '110'", '10110', 1562), ("'110' || B'10'", '11010', 1562),
        ("ARRAY[1,NULL] || ARRAY[2]", (1, None, 2), 1007),
        ("ARRAY[1] || ARRAY[2147483648::bigint]", (1, 2147483648), 1016),
        ("NULL::integer[] || ARRAY[1]", (1,), 1007),
        ("ARRAY[]::integer[] || ARRAY[]::integer[]", (), 1007),
        ("ARRAY['NULL',NULL] || ARRAY['']", ('NULL', None, ''), 1009),
        ("ARRAY[1] || 2", (1, 2), 1007), ("0 || ARRAY[1]", (0, 1), 1007),
        (owner + ".fn_int(-2) || ''", '-1', 25),
        (owner + ".fn_big(9007199254740993) || ''", '9007199254740994', 25),
        (owner + ".fn_bool(false) || ''", 'false', 25),
        ("pg_catalog.current_schema() || ''", 'pg_catalog', 25),
        ("(B'10' || B'1') & B'111'", '101', 1560),
        ("(B'10' || B'1') | B'010'", '111', 1560),
        ("(B'10' || B'1') # B'111'", '010', 1560),
        ("(B'10' || B'1') << 1", '010', 1560),
        ("(B'10' || B'1') >> 1", '010', 1560),
        ("~(B'10' || B'1')", '010', 1560),
        ("B'10'::varbit & B'11'::varbit", '10', 1560),
        ("~(B'10'::varbit)", '01', 1560),
        ("B'10'::varbit || 'x'::text", '10x', 25),
        ("'x'::text || B'10'::varbit", 'x10', 25),
        ("'\\x00'::bytea || 'x'::text", '\\x00x', 25),
        ("'x'::text || '\\x00'::bytea", 'x\\x00', 25),
        ("pg_typeof(B'10' || B'1')::text", 'bit varying', 25))


def physical():
    return (
        ("n || t", ('-2X', None, '0'), 25),
        ("t || n", ('X-2', None, '0'), 25),
        ("b || ''", ('9007199254740993', None, '0'), 25),
        ("bo || t", ('trueX', None, 'false'), 25),
        ("v || t", ('VX', None, ''), 25),
        ("c || t", ('CX', None, ''), 25),
        ("ba || ba", (bytes.fromhex('00ff00ff'), None, b''), 17),
        ("bt || bt", ('101101', None, '000000'), 1562),
        ("bv || bv", ('1010', None, ''), 1562),
        ("bt || bv", ('10110', None, '000'), 1562),
        ("a || a", ((1, None, 1, None), None, ()), 1007),
        ("a || w", ((1, None, 2147483648), None, ()), 1016),
        ("a || n", ((1, None, -2), (None,), (0,)), 1007),
        ("n || a", ((-2, 1, None), (None,), (0,)), 1007),
        ("ta || t", (('a', 'NULL', 'X'), (None,), ('',)), 1009),
        ("(n || t) || 'z'", ('-2Xz', None, '0z'), 25),
        ("length(n || t)", (3, None, 1), 23),
        ("'Q' || (n || t)", ('Q-2X', None, 'Q0'), 25),
        ("CAST(n || t AS varchar)", ('-2X', None, '0'), 1043),
        ("pg_typeof(n || t)::text", ('text',) * 3, 25),
        ("~bv", ('01', None, ''), 1560),
        ("~(bv || bt)", ('01010', None, '111'), 1560),
        ("pg_typeof(bv || bt)::text", ('bit varying',) * 3, 25))


def parameters():
    return (
        ('$1 || $2', (23, 25), (-2, 'X'), '-2X', 25),
        ('$1 || $2', (25, 23), ('X', -2), 'X-2', 25),
        ('$1 || $2', (17, 17), (bytes.fromhex('00ff'), b'\x80'), bytes.fromhex('00ff80'), 17),
        ('$1 || $2', (1562, 1562), ('10', '1'), '101', 1562),
        ('$1 || $2', (1007, 1007), ((1, None), (2,)), (1, None, 2), 1007),
        ('$1 || $2', (1007, 1016), ((1,), (2147483648,)), (1, 2147483648), 1016))


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('CONCAT_RESULT_DECLARATION_PROTOCOL')
    wire = Wire(review)
    schema = 'r215_' + uuid.uuid4().hex
    owner = identifier(schema)
    table, empty, sequence = owner + '.rows_', owner + '.empty_', owner + '.effects'
    owned = False
    try:
        before = len(review.failures)
        wire.command('CREATE SCHEMA ' + owner, 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned, 'private schema ownership not confirmed'
        wire.command('SET search_path=pg_catalog,public', 'SET', parameter_status='pg_catalog, public')
        definition = ('(id integer,n integer,b bigint,bo boolean,t text,v varchar(8),c char(4),'
                      'ba bytea,bt bit(3),bv varbit,a integer[],w bigint[],ta text[])')
        for relation in (table, empty):
            wire.command('CREATE TABLE ' + relation + definition, 'CREATE TABLE')
        wire.command('INSERT INTO ' + table + " VALUES(1,-2,9007199254740993,true,'X','V','C',"
                     "'\\x00ff',B'101',B'10',ARRAY[1,NULL],ARRAY[2147483648::bigint],ARRAY['a','NULL']),"
                     '(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),'
                     "(3,0,0,false,'','','','\\x',B'000',B'',ARRAY[]::integer[],ARRAY[]::bigint[],ARRAY[]::text[])", 'INSERT 0 3')
        wire.command('CREATE SEQUENCE ' + sequence, 'CREATE SEQUENCE')
        for name, type_name, body in (('fn_int', 'integer', 'p+1'), ('fn_big', 'bigint', 'p+1'),
                                      ('fn_bool', 'boolean', 'p')):
            wire.command('CREATE FUNCTION ' + owner + '.' + name + '(p ' + type_name + ') RETURNS ' +
                         type_name + ' LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT ' + body + '$$', 'CREATE FUNCTION')
        assert len(constants(owner)) == 59 and len(physical()) == 23 and len(parameters()) == 6
        for expression, value, oid in constants(owner):
            for mode in MODES:
                wire.query(mode, 'SELECT ' + expression + ' AS v', (value,), oid)
        for index, (expression, values, oid) in enumerate(physical()):
            prefix = 'SELECT ' + expression + ' AS v FROM '
            for mode in MODES:
                for relation, suffix, rows in ((table, ' ORDER BY id', values), (empty, '', ()),
                                               (table, ' WHERE false', ()), (table, ' LIMIT 0', ())):
                    wire.query(mode, prefix + relation + suffix, rows, oid)
            statement = ('r215_physical_' + str(index)).encode()
            wire.describe(statement, prefix + table, oid)
            wire.bind_describe(statement, oid, (), (), False, False)
            wire.bind_describe(statement, oid, (), (), False, True)
            wire.close(statement)
        for index, (expression, inputs, values, wanted, oid) in enumerate(parameters()):
            statement = ('r215_params_' + str(index)).encode()
            wire.describe(statement, 'SELECT ' + expression + ' AS v', oid, inputs)
            for binary_output in (False, True):
                wire.bind_describe(statement, oid, inputs, values, False, binary_output)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    for actual, expected in ((values, wanted), ((None, values[1]), None),
                                             ((values[0], None), None)):
                        # array_cat treats a NULL array as empty, unlike the strict scalar operators.
                        if oid in ELEMENTS:
                            expected = actual[1] if actual[0] is None else actual[0] if actual[1] is None else wanted
                        wire.execute(statement, oid, (expected,), inputs, actual, binary_input, binary_output)
            wire.close(statement)
        inferred = (
            ("$1 || B'10'", 1562, '110', '11010'),
            ("B'10' || $1", 1562, '110', '10110'),
            ("$1 || '\\x00'::bytea", 17, b'\x80', bytes.fromhex('8000')),
            ("'\\x00'::bytea || $1", 17, b'\x80', bytes.fromhex('0080')))
        assert len(inferred) == 4
        for index, (expression, oid, value, expected) in enumerate(inferred):
            statement = ('r215_inferred_' + str(index)).encode()
            wire.describe_inferred(statement, 'SELECT ' + expression + ' AS v', oid, oid)
            for binary_output in (False, True):
                wire.bind_describe(statement, oid, (oid,), (value,), False, binary_output)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.execute(statement, oid, (expected,), (oid,), (value,), binary_input, binary_output)
            wire.execute(statement, oid, (None,), (oid,), (None,), False, True)
            wire.close(statement)
        effect = "nextval('" + sequence + "')"
        wire.query('simple', 'SELECT ' + effect + ' AS v', (1,), 20)
        statement = b'r215_demand'
        wire.describe(statement, 'SELECT ' + effect + " || '' AS v", 25)
        wire.bind_describe(statement, 25, (), (), False, False)
        wire.bind_describe(statement, 25, (), (), False, True)
        wire.query('simple', 'SELECT ' + effect + ' AS v', (2,), 20)
        for binary_output, value in ((False, '3'), (True, '4')):
            wire.execute(statement, 25, (value,), binary_output=binary_output)
        for mode in MODES:
            for relation, suffix in ((empty, ''), (table, ' WHERE false'), (table, ' LIMIT 0')):
                wire.query(mode, 'SELECT ' + effect + " || '' AS v FROM " + relation + suffix, (), 25)
        wire.query('simple', 'SELECT ' + effect + ' AS v', (5,), 20)
        for mode, start in zip(MODES, (6, 9, 12)):
            wire.query(mode, 'SELECT ' + effect + " || '' AS v FROM " + table + ' ORDER BY id',
                       tuple(str(v) for v in range(start, start + 3)), 25)
        wire.query('simple', 'SELECT ' + effect + ' AS v', (15,), 20)
        wire.close(statement)
        wire.command('DROP SCHEMA ' + owner + ' CASCADE', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(EXPECTED_CONTROLS)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + owner + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
