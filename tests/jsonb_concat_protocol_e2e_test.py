#!/usr/bin/env python3
"""JSONB || values, declarations, input selection and real version-1 wire I/O.

The independent Python oracle uses exact decimals and decoded object keys.
This fixture does not claim completion of SQL/JSON, jsonpath, GIN or storage.
"""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire as BaseWire, MODES, WIDTHS, identifier, strict_data_rows
from array_binary_result_protocol_e2e_test import binary_array
from concat_result_declaration_protocol_e2e_test import array_text
from decimal import Decimal
import json
import struct
import sys
import uuid

WIDTHS.update({3802: -1, 3807: -1})
KINDS = ('{}', '{"a":1}', '[]', '[1,{"b":2}]', '"x"', '2', 'true', 'null')
EXTRA_PAIRS = (
    ('{"b":2,"a":1}', '{"a":3,"c":4}'),
    ('{"a":{"x":1},"b":[1]}', '{"a":{"y":2},"b":[2]}'),
    ('{"a":1,"a":2}', '{"b":3,"b":4}'),
    ('{"\\u0061":1,"a":2}', '{"\\u0061":3}'),
    ('{"aa":1,"z":2,"b":3,"é":4}', '{"界":5,"é":6}'),
    ('{"":1,"a:b,c":2}', '{"":3,"a\\\"b":4}'),
    ('[1,1,null,[2]]', '[null,1,[3,4]]'),
    ('[[1,2]]', '[[3,4]]'),
    ('["a,b","[x]","{x}","\\\\"]', '["\\\"","\\n"]'),
    ('"null"', 'null'), ('null', '"NULL"'),
    ('false', 'true'), ('-0.00', '1.2300e-2'),
    ('9007199254740993', '9007199254740994'),
    ('123456789012345678901234567890.1200', '-2.500e2'),
    ('1e1000', '2'), ('1e-1000', '3'),
    ('"\\ud834\\udd1e"', '"𝄞"'), ('"a\\/b"', '"a/b"'),
    ('"\\b\\f\\n\\r\\t\\u001f"', '"界😀"'),
    (None, '{}'), ('[]', None), (None, None),
    ('null', None), (None, 'null'),
)


def render(value):
    if value is None:
        return 'null'
    if value is True or value is False:
        return 'true' if value else 'false'
    if isinstance(value, Decimal):
        return format(value.copy_abs() if value == 0 else value, 'f')
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return '[' + ', '.join(render(v) for v in value) + ']'
    assert isinstance(value, dict)
    keys = sorted(value, key=lambda key: (len(key.encode()), key.encode()))
    return '{' + ', '.join(render(key) + ': ' + render(value[key]) for key in keys) + '}'


def merged(left, right):
    if left is None or right is None:
        return None
    left = json.loads(left, parse_int=Decimal, parse_float=Decimal)
    right = json.loads(right, parse_int=Decimal, parse_float=Decimal)
    value = dict(left, **right) if isinstance(left, dict) and isinstance(right, dict) else (
        (left if isinstance(left, list) else [left]) + (right if isinstance(right, list) else [right]))
    return render(value)


def literal(value, typed=True):
    return ('NULL' if value is None else "'" + value.replace("'", "''") + "'") + ('::jsonb' if typed else '')


def pairs():
    result = [(left, right) for left in KINDS for right in KINDS] + list(EXTRA_PAIRS)
    assert len(result) == 89
    return result


def constant_cases(owner):
    result = [(literal(left) + ' || ' + literal(right), merged(left, right), 3802)
              for left, right in pairs()]
    for left, right in EXTRA_PAIRS[:10] + EXTRA_PAIRS[-5:]:
        result += [(literal(left, False) + ' || ' + literal(right), merged(left, right), 3802),
                   (literal(left) + ' || ' + literal(right, False), merged(left, right), 3802)]
    result += [
        ("('{}'::jsonb || '{\"b\":2,\"a\":1}'::jsonb)::text", '{"a": 1, "b": 2}', 25),
        ("'x'::text || ('{}'::jsonb || '{\"a\":1}'::jsonb)", 'x{"a": 1}', 25),
        ("('{}'::jsonb || '{\"a\":1}'::jsonb) || 'x'::text", '{"a": 1}x', 25),
        ("'{}'::jsonb || 'x'::text", '{}x', 25),
        ("'x'::text || '{}'::jsonb", 'x{}', 25),
        ("'null'::jsonb || NULL::text", None, 25),
        ("pg_typeof('{}'::jsonb || '{}'::jsonb)::text", 'jsonb', 25),
        ("pg_typeof('[]'::jsonb || NULL)::text", 'jsonb', 25),
        ("jsonb_typeof('1'::jsonb || '2'::jsonb)", 'array', 25),
        ("jsonb_array_length('[1]'::jsonb || '[2,3]'::jsonb)", 3, 23),
        ("('[]'::jsonb || '[1,2]'::jsonb) ->> 1", '2', 25),
        (owner + '.echo_jsonb(' + literal('{"b":2}') + ') || ' + literal('{"a":1}'), '{"a": 1, "b": 2}', 3802),
        ('ARRAY[' + literal('{}') + ' || ' + literal('{"a":1}') + ',' + literal('null') + ' || ' + literal('1') + ']', ('{"a": 1}', '[null, 1]'), 3807),
        (literal('{}') + ' || (' + literal('{}') + ' || ' + literal('{"a":1}') + ')', '{"a": 1}', 3802),
    ]
    assert len(result) == 133
    return result


def datum(value, oid, binary):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if oid == 3807:
        return binary_array(3802, [datum(v, 3802, True) for v in value]) if binary else array_text(value).encode()
    if binary and oid in (20, 23):
        return struct.pack('!q' if oid == 20 else '!i', value)
    raw = str(value).encode()
    return b'\1' + raw if binary and oid == 3802 else raw


class Wire(BaseWire):
    def bind(self, statement, inputs=(), values=(), binary_input=False, binary_output=False, execute=True):
        assert len(inputs) == len(values)
        body = b'\0' + statement + b'\0' + struct.pack('!H', len(inputs))
        body += b''.join(struct.pack('!H', int(binary_input)) for _ in inputs)
        body += struct.pack('!H', len(values))
        for oid, value in zip(inputs, values):
            raw = datum(value, oid, binary_input)
            body += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
        body += struct.pack('!HH', 1, int(binary_output))
        packet = self.client.typed(b'B', body) + self.client.typed(b'D', b'P\0')
        return packet + (self.client.typed(b'E', b'\0' + struct.pack('!I', 0)) if execute else b'')

    def result_ok(self, messages, values, oid, state=None, binary=False, parsed=False, bound=False, simple=False, inputs=()):
        if state:
            return super().result_ok(messages, values, oid, state, binary, parsed, bound, simple, inputs)
        rows, valid = strict_data_rows(messages)
        prefix = [b'T'] if simple else [b'1', b't', b'T'] if parsed else []
        descriptions = [self.description(oid)] if parsed else []
        if simple or bound:
            descriptions.append(self.description(oid, binary))
        if bound:
            prefix += [b'2', b'T']
        return (valid and not self.errors(messages) and rows == [[datum(value, oid, binary)] for value in values] and
                [kind for kind, _ in messages] == prefix + [b'D'] * len(values) + [b'C', b'Z'] and
                [body for kind, body in messages if kind == b'T'] == descriptions and
                [body for kind, body in messages if kind == b't'] == ([self.parameters(inputs)] if parsed else []) and
                [body for kind, body in messages if kind == b'C'] == [('SELECT ' + str(len(values))).encode() + b'\0'] and
                all(body == b'' for kind, body in messages if kind in (b'1', b'2')) and messages[-1:] == [(b'Z', b'I')])

    def inferred(self, statement, sql, inputs):
        self.collect(('INFERRED DESCRIBE', statement, sql, inputs), lambda: self.exchange(self.parse(statement, sql, (0,))),
                     lambda messages: messages == [(b'1', b''), (b't', self.parameters(inputs)), (b'T', self.description(3802)), (b'Z', b'I')],
                     (3802, inputs, 'no execution'))

    def bind_error(self, statement, value, binary, state):
        self.collect(('BIND ERROR', statement, value, binary),
                     lambda: self.exchange(self.bind(statement, (3802,), (value,), binary, True)),
                     lambda messages: [kind for kind, _ in messages] == [b'E', b'Z'] and self.errors(messages) == [state] and messages[-1:] == [(b'Z', b'I')], state)


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    review = Review('JSONB_CONCAT_PROTOCOL')
    wire = Wire(review)
    owner = identifier('jsonb_concat_' + uuid.uuid4().hex)
    table, empty, sequence = owner + '.rows_', owner + '.empty_', owner + '.effects_'
    owned = False
    try:
        before = len(review.failures)
        wire.command('CREATE SCHEMA ' + owner, 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned
        wire.command('CREATE FUNCTION ' + owner + '.echo_jsonb(p jsonb) RETURNS jsonb LANGUAGE SQL IMMUTABLE STRICT AS $$SELECT p$$', 'CREATE FUNCTION')
        wire.command('CREATE SEQUENCE ' + sequence, 'CREATE SEQUENCE')
        wire.command('CREATE FUNCTION ' + owner + ".effect_jsonb() RETURNS jsonb LANGUAGE SQL VOLATILE AS $$SELECT jsonb_build_object('n',nextval('" + sequence + "'))$$", 'CREATE FUNCTION')
        for mode in MODES:
            for expression, value, oid in constant_cases(owner):
                wire.query(mode, 'SELECT ' + expression + ' AS v;', (value,), oid)
        wire.command('CREATE TABLE ' + table + '(id integer,j jsonb,k jsonb)', 'CREATE TABLE')
        wire.command('CREATE TABLE ' + empty + '(id integer,j jsonb,k jsonb)', 'CREATE TABLE')
        rows = pairs()[0:8] + list(EXTRA_PAIRS[-5:])
        assert len(rows) == 13
        wire.command('INSERT INTO ' + table + ' VALUES ' + ','.join('(' + str(i) + ',' + literal(j) + ',' + literal(k) + ')' for i, (j, k) in enumerate(rows, 1)), 'INSERT 0 13')
        physical = (
            ('j || k', tuple(merged(j, k) for j, k in rows), 3802),
            ('k || j', tuple(merged(k, j) for j, k in rows), 3802),
            ("j || '{}'", tuple(merged(j, '{}') for j, _ in rows), 3802),
            ("'{}' || j", tuple(merged('{}', j) for j, _ in rows), 3802),
            (owner + '.echo_jsonb(j) || k', tuple(merged(j, k) for j, k in rows), 3802),
            ('(j || k)::text', tuple(merged(j, k) for j, k in rows), 25),
            ('jsonb_typeof(j || k)', tuple(None if j is None or k is None else 'object' if isinstance(json.loads(merged(j, k)), dict) else 'array' for j, k in rows), 25),
        )
        for mode in MODES:
            for expression, values, oid in physical:
                query = 'SELECT ' + expression + ' AS v FROM '
                wire.query(mode, query + table + ' ORDER BY id', values, oid)
                wire.query(mode, query + table + ' WHERE false', (), oid)
                wire.query(mode, query + table + ' LIMIT 0', (), oid)
                wire.query(mode, query + empty, (), oid)
        for index, sql in enumerate(('$1 || $2', '$2 || $1', owner + '.echo_jsonb($1) || $2')):
            statement = ('jsonb_pair_' + str(index)).encode()
            wire.describe(statement, 'SELECT ' + sql + ' AS v', 3802, (3802, 3802))
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, 3802, (3802, 3802), ('{}', '{}'), binary_input, binary_output)
                    for left, right in EXTRA_PAIRS[0:5] + EXTRA_PAIRS[-5:]:
                        expected = merged(right, left) if index == 1 else merged(left, right)
                        wire.execute(statement, 3802, (expected,), (3802, 3802), (left, right), binary_input, binary_output)
            wire.close(statement)
        # JSONB results are also elements of the already-supported array
        # operators. Their version-1 binary element codec must round-trip.
        array_cases = ((), ('{}',), ('null',), ('{"a": 1}', '[null, 1]'), (None, '{}'), None)
        for index, sql in enumerate(("$1 || ARRAY['{}'::jsonb]", "ARRAY['{}'::jsonb] || $1")):
            statement = ('jsonb_arrays_' + str(index)).encode()
            wire.describe(statement, 'SELECT ' + sql + ' AS v', 3807, (3807,))
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.bind_describe(statement, 3807, (3807,), ((),), binary_input, binary_output)
                    for values in array_cases:
                        expected = tuple(values or ()) + ('{}',) if index == 0 else ('{}',) + tuple(values or ())
                        wire.execute(statement, 3807, (expected,), (3807,), (values,), binary_input, binary_output)
            wire.close(statement)
        for index, sql in enumerate(("$1 || '{}'::jsonb", "'{}'::jsonb || $1")):
            statement = ('jsonb_infer_' + str(index)).encode()
            wire.inferred(statement, 'SELECT ' + sql + ' AS v', (3802,))
            for binary_input in (False, True):
                for binary_output in (False, True):
                    wire.execute(statement, 3802, (merged('[1]', '{}') if index == 0 else merged('{}', '[1]'),), (3802,), ('[1]',), binary_input, binary_output)
            wire.close(statement)
        errors = (('1', '42883'), ('true', '42883'), ("'{}'::json", '42883'),
                  ("'{'", '22P02'), ("'[1,]'", '22P02'), ("'01'", '22P02'),
                  ("'\"\\u0000\"'", '22P05'), ("'\"\\ud800\"'", '22P02'))
        for mode in MODES:
            for bad, state in errors:
                for suffix in ('', ' FROM ' + empty, ' FROM ' + table + ' WHERE false', ' FROM ' + table + ' LIMIT 0'):
                    wire.query(mode, "SELECT '{}'::jsonb || " + bad + ' AS v' + suffix, (), 3802, state)
        statement = b'jsonb_bad_inputs'
        wire.describe(statement, "SELECT $1 || '{}'::jsonb AS v", 3802, (3802,))
        bad_inputs = (('{', False, '22P02'), ('"\\u0000"', False, '22P05'),
                      (b'\2{}', True, 'XX000'), (b'\1{', True, '22P02'), (b'\1"\\u0000"', True, '22P05'))
        for raw, binary, state in bad_inputs:
            wire.bind_error(statement, raw, binary, state)
            wire.execute(statement, 3802, ('{}',), (3802,), ('{}',), False, True)
        wire.close(statement)
        effect = owner + ".effect_jsonb() || '{}'::jsonb"
        statement = b'jsonb_effects'
        wire.describe(statement, 'SELECT ' + effect + ' AS v', 3802)
        wire.bind_describe(statement, 3802, (), (), False, True)
        for mode in MODES:
            for suffix in (' FROM ' + table + ' WHERE false', ' FROM ' + table + ' LIMIT 0', ' FROM ' + empty):
                wire.query(mode, 'SELECT ' + effect + ' AS v' + suffix, (), 3802)
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v", (1,), 20)
        for mode, number in zip(MODES, (2, 3, 4)):
            wire.query(mode, 'SELECT ' + effect + ' AS v', ('{"n": ' + str(number) + '}',), 3802)
        wire.query('simple', "SELECT nextval('" + sequence + "') AS v", (5,), 20)
        wire.close(statement)
        wire.command('DROP SCHEMA ' + owner + ' CASCADE', 'DROP SCHEMA', notices=True)
        owned = False
        review.complete(826)
    finally:
        if owned:
            review.client.simple_query(review.sock, 'DROP SCHEMA ' + owner + ' CASCADE')
        review.close()


if __name__ == '__main__':
    main()
