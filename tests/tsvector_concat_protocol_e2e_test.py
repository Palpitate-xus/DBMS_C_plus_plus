#!/usr/bin/env python3
"""Structured TSVECTOR concatenation, descriptors and actual wire datums.

Expectations are built from lexemes/positions, not from the product parser.
Run the entire fixture on PostgreSQL 18 as well as each product projection.
"""
from compat.numeric_type_review import Review
from quoted_unqualified_scalar_projection_protocol_e2e_test import Wire as BaseWire, MODES, WIDTHS, strict_data_rows, identifier
import struct
import sys
import uuid

WIDTHS.update({3614: -1})
# (input spelling, decoded lexeme -> sorted (position, weight) occurrences).
VECTORS = (
    ('', {}),
    ('a', {'a': []}),
    ('a:1', {'a': [(1, 'D')]}),
    ('b:2B a:5A', {'a': [(5, 'A')], 'b': [(2, 'B')]}),
    ('a:3B,1A a:2C', {'a': [(1, 'A'), (2, 'C'), (3, 'B')]}),
    ("'with space':2C x:4", {'with space': [(2, 'C')], 'x': [(4, 'D')]}),
    ("'quote''d':3A 'back\\\\slash':2", {"quote'd": [(3, 'A')], 'back\\slash': [(2, 'D')]}),
    ('end:16383A', {'end': [(16383, 'A')]}),
)
EXTRAS = (
    ('z:2 aa:1', 'bb:1 a:2', {'aa': [(1, 'D')], 'z': [(2, 'D')], 'a': [(4, 'D')], 'bb': [(3, 'D')]}),
    ('a:2A', 'a:1B', {'a': [(2, 'A'), (3, 'B')]}),
    ('a', 'a:1B', {'a': [(1, 'B')]}),
    ('a:1B', 'a', {'a': [(1, 'B')]}),
    ('z:16382B', 'a:1A,2B,3C', {'a': [(16383, 'A')], 'z': [(16382, 'B')]}),
    ('a:16383A', 'a:1B,2C', {'a': [(16383, 'A')]}),
    ('a:1A,1B,1C,1D', 'a:1', {'a': [(1, 'A'), (2, 'D')]}),
    ('a:2b,1d', 'b:2a', {'a': [(1, 'D'), (2, 'B')], 'b': [(4, 'A')]}),
    ('界:1 é:2', '😀:1', {'界': [(1, 'D')], 'é': [(2, 'D')], '😀': [(3, 'D')]}),
    ('a:20000', 'b:30000', {'a': [(16383, 'D')], 'b': [(16383, 'D')]}),
    ('a:' + ','.join(str(i) for i in range(1, 257)), 'a:1A', {'a': [(i, 'D') for i in range(1, 257)]}),
    ('a:' + ','.join(str(i) for i in range(1, 256)), 'a:1B,2A', {'a': [(i, 'D') for i in range(1, 256)] + [(256, 'B')]}),
    (None, 'a:1', None), ('a:1', None, None), (None, None, None),
)


def render(entries):
    if entries is None:
        return None
    result = []
    for lexeme in sorted(entries, key=lambda key: key.encode()):
        value = "'" + lexeme.replace('\\', '\\\\').replace("'", "''") + "'"
        if entries[lexeme]:
            value += ':' + ','.join(str(p) + (w if w != 'D' else '') for p, w in entries[lexeme])
        result.append(value)
    return ' '.join(result)


def merge(left, right):
    result = {key: list(value) for key, value in left.items()}
    offset = max((p for value in left.values() for p, _ in value), default=0)
    for key, positions in right.items():
        target = result.setdefault(key, [])
        for position, weight in positions:
            if len(target) == 256 or (target and target[-1][0] == 16383):
                break
            target.append((min(position + offset, 16383), weight))
    return result


def cases():
    result = [(left, right, merge(a, b)) for left, a in VECTORS for right, b in VECTORS] + list(EXTRAS)
    assert len(result) == 79
    return result


def literal(value, typed=True):
    return ('NULL' if value is None else "'" + value.replace("'", "''") + "'") + ('::tsvector' if typed else '')


def binary_vector(entries):
    result = struct.pack('!i', len(entries))
    for lexeme in sorted(entries, key=lambda key: key.encode()):
        result += lexeme.encode() + b'\0' + struct.pack('!H', len(entries[lexeme]))
        result += b''.join(struct.pack('!H', position | ('DCBA'.index(weight) << 14)) for position, weight in entries[lexeme])
    return result


def datum(value, oid, binary):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if oid == 3614:
        return binary_vector(value) if binary else render(value).encode()
    if binary and oid in (20, 23):
        return struct.pack('!q' if oid == 20 else '!i', value)
    if oid == 16:
        return bytes([int(value)]) if binary else (b't' if value else b'f')
    return str(value).encode()


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
        return (valid and not self.errors(messages) and rows == [[datum(v, oid, binary)] for v in values] and
                [kind for kind, _ in messages] == prefix + [b'D'] * len(values) + [b'C', b'Z'] and
                [body for kind, body in messages if kind == b'T'] == descriptions and
                [body for kind, body in messages if kind == b't'] == ([self.parameters(inputs)] if parsed else []) and
                [body for kind, body in messages if kind == b'C'] == [('SELECT ' + str(len(values))).encode() + b'\0'] and
                all(body == b'' for kind, body in messages if kind in (b'1', b'2')) and messages[-1:] == [(b'Z', b'I')])

    def inferred(self, statement, sql):
        self.collect(('INFERRED', sql), lambda: self.exchange(self.parse(statement, sql, (0,))),
                     lambda messages: messages == [(b'1', b''), (b't', self.parameters((3614,))), (b'T', self.description(3614)), (b'Z', b'I')],
                     (3614, 'no execution'))


def main():
    if sys.argv[1:] == ['--reference']:
        sys.argv[1:] = ['--reference18']
    WIDTHS.update({16: 1})
    review = Review('TSVECTOR_CONCAT_PROTOCOL')
    wire = Wire(review)
    owner = identifier('tsvector_concat_' + uuid.uuid4().hex)
    table, empty = owner + '.rows_', owner + '.empty_'
    owned = False
    try:
        before = len(review.failures)
        wire.command('CREATE SCHEMA ' + owner, 'CREATE SCHEMA')
        owned = len(review.failures) == before
        assert owned
        for mode in MODES:
            for left, right, expected in cases():
                wire.query(mode, 'SELECT ' + literal(left) + ' || ' + literal(right) + ' AS v', (expected,), 3614)
            for expression, value in (("to_tsvector('simple','alpha beta') || to_tsvector('simple','gamma')", {'alpha': [(1, 'D')], 'beta': [(2, 'D')], 'gamma': [(3, 'D')]}), ("setweight('a:1'::tsvector,'A') || 'b:1B'::tsvector", {'a': [(1, 'A')], 'b': [(2, 'B')]}), ("'a:1'::tsvector || setweight('b:1'::tsvector,'B')", {'a': [(1, 'D')], 'b': [(2, 'B')]}), ("to_tsvector(NULL::text) || 'a:1'::tsvector", None), ("NULL::tsvector || to_tsvector('simple','a')", None)):
                wire.query(mode, 'SELECT ' + expression + ' AS v', (value,), 3614)
            for left, right in (('a:1', 'b:1'), ('a', 'b:1'), (None, 'a:1')):
                expected = None if left is None else merge(next(v for s, v in VECTORS if s == left), {'b': [(1, 'D')]})
                for expression in (literal(left, False) + ' || ' + literal(right), literal(left) + ' || ' + literal(right, False)):
                    wire.query(mode, 'SELECT ' + expression + ' AS v', (expected,), 3614)
            wire.query(mode, "SELECT ('a:1'::tsvector || 'b:1'::tsvector) @@ 'a <-> b'::tsquery AS v", (True,), 16)
            wire.query(mode, "SELECT ('a:1'::tsvector || 'b:1'::tsvector) @@ 'b <-> a'::tsquery AS v", (False,), 16)
            wire.query(mode, "SELECT ('a:1A'::tsvector || 'b:1B'::tsvector)::text AS v", ("'a':1A 'b':2B",), 25)
            wire.query(mode, "SELECT 'a:1'::tsvector || 'x'::text AS v", ("'a':1x",), 25)
            wire.query(mode, "SELECT 'x'::text || 'a:1'::tsvector AS v", ("x'a':1",), 25)
        wire.command('CREATE TABLE ' + table + '(id integer,a tsvector,b tsvector)', 'CREATE TABLE')
        wire.command('CREATE TABLE ' + empty + '(id integer,a tsvector,b tsvector)', 'CREATE TABLE')
        rows = list(EXTRAS[:9]) + list(EXTRAS[-3:])
        wire.command('INSERT INTO ' + table + ' VALUES ' + ','.join('(' + str(i) + ',' + literal(a) + ',' + literal(b) + ')' for i, (a, b, _) in enumerate(rows, 1)), 'INSERT 0 12')
        for mode in MODES:
            wire.query(mode, 'SELECT a || b AS v FROM ' + table + ' ORDER BY id', tuple(value for _, _, value in rows), 3614)
            for suffix in (' WHERE false', ' LIMIT 0'):
                wire.query(mode, 'SELECT a || b AS v FROM ' + table + suffix, (), 3614)
            wire.query(mode, 'SELECT a || b AS v FROM ' + empty, (), 3614)
            for suffix in ('', ' WHERE false', ' LIMIT 0'):
                for bad in ('1', 'NULL::integer', "'{}'::jsonb"):
                    wire.query(mode, 'SELECT a || ' + bad + ' AS v FROM ' + table + suffix, state='42883')
        for index, sql in enumerate(("SELECT $1 || 'b:1'::tsvector AS v", "SELECT 'b:1'::tsvector || $1 AS v")):
            statement = ('tsvector_inferred_' + str(index)).encode()
            wire.inferred(statement, sql)
            for binary_input in (False, True):
                for binary_output in (False, True):
                    for value in ({'a': [(2, 'A')]}, {}, None):
                        expected = None if value is None else merge(value, {'b': [(1, 'D')]}) if index == 0 else merge({'b': [(1, 'D')]}, value)
                        wire.execute(statement, 3614, (expected,), (3614,), (value,), binary_input, binary_output)
            wire.close(statement)
        # Full fixture inventory: do not weaken failure collection on old code.
        review.complete(356)
    finally:
        if owned:
            wire.command('DROP SCHEMA ' + owner + ' CASCADE', 'DROP SCHEMA', notices=True)
        review.close()


if __name__ == '__main__':
    main()
