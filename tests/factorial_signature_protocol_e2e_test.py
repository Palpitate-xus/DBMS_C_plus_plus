#!/usr/bin/env python3
"""The real factorial(bigint)->numeric declaration and wire phases.

All original 69 observations are retained, including the held R271 large
result gaps. Expected integer factorials use Python's independent integers.
Run the complete fixture on PostgreSQL 18 before using it as a product gate.
"""
from compat.numeric_type_review import Review
from gcd_lcm_signature_protocol_e2e_test import Wire as NumericWire, MODES, TYPES
from quoted_unqualified_scalar_projection_protocol_e2e_test import identifier, strict_data_rows
import math
import struct
import sys
import uuid

PLANNED = 704
if hasattr(sys, 'set_int_max_str_digits'):
    sys.set_int_max_str_digits(max(sys.get_int_max_str_digits(), 20000))


def factorial_text(value):
    return str(math.factorial(value))


def numeric_bytes(value):
    if value is None:
        return None
    text = str(value)
    assert text.isascii() and text.isdigit(), text
    text = text.lstrip('0')
    if not text:
        return struct.pack('!HhHH', 0, 0, 0, 0)
    digits = [int(text[i:i+4]) for i in range(len(text) % 4 or 4, len(text), 4)]
    first = len(text) % 4 or 4
    digits = [int(text[:first])] + digits
    weight = len(digits) - 1
    while digits and digits[-1] == 0:
        digits.pop()
    return struct.pack('!HhHH', len(digits), weight, 0, 0) + b''.join(struct.pack('!H', x) for x in digits)


ORIGINAL_CASES = (
    ("SELECT factorial() AS v",None,1700,'42883'),
    ("SELECT factorial(5,6) AS v",None,1700,'42883'),
    ("SELECT factorial(5::smallint) AS v","120",1700,None),
    ("SELECT factorial(5::integer) AS v","120",1700,None),
    ("SELECT factorial(5::bigint) AS v","120",1700,None),
    ("SELECT factorial(5::numeric) AS v",None,1700,'42883'),
    ("SELECT factorial(5.1::numeric) AS v",None,1700,'42883'),
    ("SELECT factorial(5::real) AS v",None,1700,'42883'),
    ("SELECT factorial(5::double precision) AS v",None,1700,'42883'),
    ("SELECT factorial('5') AS v","120",1700,None),
    ("SELECT factorial('5'::text) AS v",None,1700,'42883'),
    ("SELECT factorial(true) AS v",None,1700,'42883'),
    ("SELECT factorial('2000-01-01'::date) AS v",None,1700,'42883'),
    ("SELECT factorial(ARRAY[5]) AS v",None,1700,'42883'),
    ("SELECT factorial(NULL) AS v",None,1700,None),
    ("SELECT factorial(NULL::bigint) AS v",None,1700,None),
    ("SELECT factorial(NULL::numeric) AS v",None,1700,'42883'),
    ("SELECT factorial(NULL::text) AS v",None,1700,'42883'),
    ("SELECT factorial('bad') AS v",None,1700,'22P02'),
    ("SELECT factorial('5.0') AS v",None,1700,'22P02'),
    ("SELECT factorial('NaN') AS v",None,1700,'22P02'),
    ("SELECT factorial('Infinity') AS v",None,1700,'22P02'),
    ("SELECT factorial(-1) AS v",None,1700,'22003'),
    ("SELECT factorial(0) AS v","1",1700,None),
    ("SELECT factorial(1) AS v","1",1700,None),
    ("SELECT factorial(20) AS v","2432902008176640000",1700,None),
    ("SELECT factorial(100) AS v","93326215443944152681699238856266700490715968264381621468592963895217599993229915608941463976156518286253697920827223758251185210916864000000000000000000000000",1700,None),
    ("SELECT factorial(1661) AS v",factorial_text(1661),1700,None),
    ("SELECT factorial(1662) AS v",factorial_text(1662),1700,None),
    ("SELECT factorial(32178) AS v",None,1700,'22003'),
    ("SELECT factorial('-9223372036854775808'::bigint) AS v",None,1700,'22003'),
    ("SELECT factorial('9223372036854775807'::bigint) AS v",None,1700,'22003'),
    ("SELECT factorial(x=>5) AS v",None,1700,'42883'),
    ("SELECT factorial(num=>5) AS v",None,1700,'42883'),
    ("SELECT factorial() AS v WHERE false",None,1700,'42883'),
    ("SELECT factorial() AS v LIMIT 0",None,1700,'42883'),
    ("SELECT factorial(1,2) AS v WHERE false",None,1700,'42883'),
    ("SELECT factorial(1,2) AS v LIMIT 0",None,1700,'42883'),
    ("SELECT factorial('bad') AS v WHERE false",None,1700,'22P02'),
    ("SELECT factorial('bad') AS v LIMIT 0",None,1700,'22P02'),
    ("SELECT factorial(1::text) AS v WHERE false",None,1700,'42883'),
    ("SELECT factorial(1::text) AS v LIMIT 0",None,1700,'42883'),
    ("SELECT factorial(-1) AS v WHERE false",None,1700,'22003'),
    ("SELECT factorial(-1) AS v LIMIT 0",None,1700,'22003'),
    ("SELECT factorial(NULL::text) AS v WHERE false",None,1700,'42883'),
    ("SELECT factorial(NULL::text) AS v LIMIT 0",None,1700,'42883'),
    ("SELECT factorial(5) OVER () AS v",None,1700,'42809'),
    ("SELECT factorial(DISTINCT 5) AS v",None,1700,'42809'),
    ("SELECT factorial(5) FILTER (WHERE true) AS v",None,1700,'42809'),
    ("SELECT factorial(5 ORDER BY 1) AS v",None,1700,'42809'),
    ("SELECT factorial('bad') OVER () AS v",None,1700,'42809'),
    ("SELECT factorial(DISTINCT 'bad') AS v",None,1700,'42809'),
    ("SELECT factorial('bad') FILTER (WHERE true) AS v",None,1700,'42809'),
    ("SELECT factorial('bad' ORDER BY 1) AS v",None,1700,'42809'),
    ("SELECT factorial(1::text) OVER () AS v",None,1700,'42883'),
    ("SELECT factorial(DISTINCT 1::text) AS v",None,1700,'42883'),
    ("SELECT factorial(1::text) FILTER (WHERE true) AS v",None,1700,'42883'),
    ("SELECT factorial(1::text ORDER BY 1) AS v",None,1700,'42883'),
    ("SELECT factorial(1) FILTER (WHERE missing) AS v",None,1700,'42703'),
    ("SELECT factorial(1) FILTER (WHERE 1) AS v",None,1700,'42804'),
    ("SELECT factorial(1) FILTER (WHERE NULL) AS v",None,1700,'42809'),
    ("SELECT COALESCE(1::numeric,factorial(-1)) AS v","1",1700,None),
    ("SELECT CASE WHEN false THEN factorial(-1) ELSE 1::numeric END AS v","1",1700,None),
    ("SELECT COALESCE(1::numeric,factorial('bad')) AS v",None,1700,'22P02'),
    ("SELECT CASE WHEN false THEN factorial('bad') ELSE 1::numeric END AS v",None,1700,'22P02'),
    ("SELECT \"FACTORIAL\"(5) AS v",None,1700,'42883'),
    ("SELECT pg_catalog.factorial(5) AS v","120",1700,None),
    ("SELECT factorial(5)+factorial(4) AS v","144",1700,None),
    ("SELECT 42 AS v",42,23,None),
)


def scalar_cases():
    # The preparation script inserts all 69 primary-observed SQL requests here.
    return ORIGINAL_CASES


class Wire(NumericWire):
    def __init__(self, review):
        super().__init__(review)
        self.ready = b'I'

    def command(self, sql, tag, notices=False):
        ready = b'T' if sql == 'BEGIN' or sql.startswith('SAVEPOINT ') or sql.startswith('ROLLBACK TO ') else b'I'
        def valid(messages):
            filtered = [(k, v) for k, v in messages if k != b'N'] if notices else messages
            return filtered == [(b'C', tag.encode()+b'\0'), (b'Z', ready)]
        self.collect(sql, lambda: self.client.simple_query(self.sock, sql), valid, (tag, ready))
        self.ready = ready

    def generic_bad(self, statement, inputs):
        def valid(messages):
            rows, valid_rows = strict_data_rows(messages)
            return valid_rows and not rows and [k for k, _ in messages] == [b'2', b'T', b'E', b'Z'] and self.errors(messages) == ['22003'] and [v for k, v in messages if k == b'T'] == [self.description(1700)] and messages[-1:] == [(b'Z', b'E')]
        self.collect(('GENERIC_EXECUTE_ERROR_PHASE', statement, inputs),
            lambda: self.exchange(self.bind(statement, inputs, (-1,), False, False, True)), valid,
            ('BindComplete, RowDescription, then Execute 22003/E'))
        self.ready = b'E'

    def result_ok(self, messages, values, oid, state=None, binary=False, parsed=False, bound=False, simple=False, inputs=()):
        if messages[-1:] != [(b'Z', self.ready)]:
            return False
        messages = messages[:-1]+[(b'Z', b'I')]
        if oid == 1700 and binary and not state:
            values = tuple(numeric_bytes(value) for value in values)
        # NumericWire's small GCD oracle is intentionally not used for results.
        return super(NumericWire, self).result_ok(messages, values, oid, state, binary, parsed, bound, simple, inputs) if state != '22003' else super().result_ok(messages, values, oid, state, binary, parsed, bound, simple, inputs)

    def bad_bind(self, statement, inputs=(), values=(), binary_input=False, in_transaction=False):
        self.collect(('BIND_ERROR_PHASE', statement, inputs, values, binary_input, in_transaction),
            lambda: self.exchange(self.bind(statement, inputs, values, binary_input, False, False)),
            lambda messages: [k for k, _ in messages] == [b'E', b'Z'] and self.errors(messages) == ['22003']
                and messages[-1:] == [(b'Z', b'E' if in_transaction else b'I')],
            ('22003', 'no BindComplete, RowDescription or execution', 'E' if in_transaction else 'I'))
        self.ready = b'E' if in_transaction else b'I'


def plan(schema):
    operations = []
    def add(method, *args, **kwargs):
        operations.append((method, args, kwargs))
    q = identifier(schema)
    table, empty, seq = q+'.rows_', q+'.empty_', q+'.effects_'
    add('command', 'CREATE SCHEMA '+q, 'CREATE SCHEMA')
    columns = '(id integer,s smallint,i integer,b bigint,n numeric,t text)'
    add('command', 'CREATE TABLE '+table+columns, 'CREATE TABLE')
    add('command', 'CREATE TABLE '+empty+columns, 'CREATE TABLE')
    add('command', 'INSERT INTO '+table+" VALUES(1,5,5,5,5,'5'),(2,NULL,NULL,NULL,NULL,NULL)", 'INSERT 0 2')
    for sql, value, oid, state in scalar_cases():
        for mode in MODES:
            add('query', mode, sql, () if ' WHERE false' in sql or ' LIMIT 0' in sql else (value,), oid, state=state)
    for sql, value, oid, state in scalar_cases()[:34]:
        for suffix in (' FROM '+empty+' WHERE false', ' FROM '+empty+' LIMIT 0'):
            for mode in MODES:
                add('query', mode, sql+suffix, (), oid, state=state)
    for n in (448, 449, 450, 451):
        for mode in MODES:
            add('query', mode, 'SELECT factorial('+str(n)+') AS v', (factorial_text(n),), 1700)
    for column, valid in (('s', True), ('i', True), ('b', True), ('n', False), ('t', False)):
        for suffix, values in ((' FROM '+table+' ORDER BY id', (factorial_text(5), None)),
            (' FROM '+table+' WHERE false', ()), (' FROM '+table+' LIMIT 0', ()), (' FROM '+empty, ())):
            for mode in MODES:
                add('query', mode, 'SELECT factorial('+column+') AS v'+suffix, values, 1700, state=None if valid else '42883')
    for type_, kind, oid in (*TYPES, ('unknown', 'U', 0)):
        statement = ('factorial_arg_'+str(oid)).encode()
        valid = oid in (0, 21, 23, 20)
        add('inferred', statement, 'SELECT factorial($1) AS v', (oid,), (20 if oid == 0 else oid,), 1700, None if valid else '42883')
        if not valid:
            continue
        resolved = (20 if oid == 0 else oid,)
        for binary_input in (False, True):
            for binary_output in (False, True):
                for n in (0, 5, 20, 100, None):
                    add('execute', statement, 1700, (None if n is None else factorial_text(n),), resolved, (n,), binary_input=binary_input, binary_output=binary_output)
        # Twenty successful binds exercise automatic generic-plan selection.
        # Its parameter error belongs to Execute, not to Bind. Preserve that
        # control as well as fresh custom-plan Bind errors below.
        add('command', 'BEGIN', 'BEGIN')
        add('generic_bad', statement, resolved)
        add('command', 'ROLLBACK', 'ROLLBACK')
        for binary_input in (False, True):
            fresh = statement+b'_fresh_'+str(int(binary_input)).encode()
            add('inferred', fresh, 'SELECT factorial($1) AS v', resolved, resolved, 1700)
            add('command', 'BEGIN', 'BEGIN')
            add('command', 'SAVEPOINT factorial_recovery', 'SAVEPOINT')
            add('bad_bind', fresh, resolved, (-1,), binary_input, True)
            add('command', 'ROLLBACK TO SAVEPOINT factorial_recovery', 'ROLLBACK')
            add('execute', fresh, 1700, (factorial_text(5),), resolved, (5,), binary_input=binary_input)
            add('command', 'ROLLBACK', 'ROLLBACK')
            add('close', fresh)
        add('close', statement)
    for i, tail in enumerate(('', ' WHERE false', ' LIMIT 0', ' FROM '+empty)):
        statement = ('factorial_literal_bad_'+str(i)).encode()
        add('describe', statement, 'SELECT factorial(-1) AS v'+tail, 1700)
        add('bad_bind', statement)
        add('close', statement)
    for spelling, state in (('factorial', None), ('FACTORIAL', None), ('"factorial"', None), ('"FACTORIAL"', '42883'), ('pg_catalog.factorial', None)):
        for mode in MODES:
            add('query', mode, 'SELECT '+spelling+'(5::bigint) AS v', (factorial_text(5),), 1700, state=state)
    add('command', 'CREATE SEQUENCE '+seq, 'CREATE SEQUENCE')
    statement = b'factorial_effects'
    add('describe', statement, "SELECT factorial(nextval('"+seq+"')) AS v", 1700)
    add('bind_describe', statement, 1700, (), (), False, False)
    add('query', 'simple', "SELECT nextval('"+seq+"') AS v", (1,), 20)
    add('execute', statement, 1700, (factorial_text(2),))
    add('query', 'simple', "SELECT nextval('"+seq+"') AS v", (3,), 20)
    add('execute', statement, 1700, (factorial_text(4),), binary_output=True)
    add('query', 'simple', "SELECT nextval('"+seq+"') AS v", (5,), 20)
    add('close', statement)
    add('command', 'CREATE FUNCTION '+q+".factorial(text) RETURNS text LANGUAGE SQL AS 'SELECT $1'", 'CREATE FUNCTION')
    for mode in MODES:
        add('query', mode, 'SELECT '+q+".factorial('actual stored callee'::text) AS v", ('actual stored callee',), 25)
        add('query', mode, 'SELECT '+q+'.factorial(5::bigint) AS v', (), 1700, state='42883')
        add('query', mode, "SELECT pg_catalog.factorial('5'::text) AS v", (), 1700, state='42883')
    add('query', 'simple', 'SELECT 42 AS v', (42,), 23)
    add('command', 'DROP SCHEMA '+q+' CASCADE', 'DROP SCHEMA', notices=True)
    if PLANNED:
        assert len(operations) == PLANNED, len(operations)
    return operations


def main():
    if sys.argv[1:] == ['--list']:
        print(len(scalar_cases()), len(plan('factorial_signature_'+'0'*32)))
        return
    assert PLANNED
    operations = plan('factorial_signature_'+uuid.uuid4().hex)
    review = Review('FACTORIAL_SIGNATURE')
    wire = Wire(review)
    owned = False
    try:
        for i, (method, args, kwargs) in enumerate(operations):
            before = len(review.failures)
            getattr(wire, method)(*args, **kwargs)
            if i == 0:
                owned = len(review.failures) == before
                assert owned, 'private schema ownership not confirmed'
        owned = False
        review.complete(PLANNED)
    finally:
        if owned:
            review.client.simple_query(review.sock, operations[-1][1][0])
        review.close()


if __name__ == '__main__':
    main()
