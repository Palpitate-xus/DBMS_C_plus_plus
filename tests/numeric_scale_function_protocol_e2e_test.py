#!/usr/bin/env python3
"""SCALE's numeric declaration, int4 result, error phase and parameter datums."""
from compat.numeric_type_review import Review
from tsvector_concat_protocol_e2e_test import Wire
from quoted_unqualified_scalar_projection_protocol_e2e_test import identifier
import json
import struct
import sys
import uuid

MODES = ('simple', 'extended', 'binary')
PLANNED = 416
EXTRA_PHASE_CASES = (
    ("SELECT scale('bad') OVER () AS v", (), '42809'),
    ("SELECT scale(DISTINCT 'bad') AS v", (), '42809'),
    ("SELECT scale('bad') FILTER (WHERE true) AS v", (), '42809'),
    ("SELECT scale('bad' ORDER BY 1) AS v", (), '42809'),
    ('SELECT scale(1::text) OVER () AS v', (), '42883'),
    ('SELECT scale(DISTINCT 1::text) AS v', (), '42883'),
    ('SELECT scale(1::text) FILTER (WHERE true) AS v', (), '42883'),
    ('SELECT scale(1::text ORDER BY 1) AS v', (), '42883'),
    ('SELECT scale(1 ORDER BY missing) AS v', (), '42809'),
    ('SELECT scale(1) FILTER (WHERE missing) AS v', (), '42703'),
    ('SELECT scale(missing) FILTER (WHERE true) AS v', (), '42703'),
    ('SELECT scale(1) FILTER (WHERE 1) AS v', (), '42804'),
    ('SELECT scale(NULL) FILTER (WHERE false) AS v', (), '42809'),
    ('SELECT scale(1 ORDER BY 1/0) AS v', (), '42809'),
    ("SELECT scale('bad' ORDER BY 1/0) AS v", (), '42809'),
    ("SELECT scale('1.00'::numeric ORDER BY 1) AS v", (), '42809'),
    ('SELECT scale(1,2 ORDER BY 1) AS v', (), '42883'),
    ('SELECT scale(NULL::text ORDER BY 1) AS v', (), '42883'),
    ('SELECT scale(1) FILTER (WHERE NULL) AS v', (), '42809'),
    ('SELECT 42 AS v', (42,), None),
)


def number(scale, weight=0, sign=0, groups=()):
    return struct.pack('!HhHH', len(groups), weight, sign, scale) + b''.join(struct.pack('!H', n) for n in groups)


def plan(schema):
    operations = []
    def add(method, *args, **kwargs):
        operations.append(dict(method=method, args=args, kwargs=kwargs))
    q = identifier(schema)
    empty, rows, seq = q+'.empty_', q+'.rows_', q+'.effects_'
    add('command', 'CREATE SCHEMA '+q, 'CREATE SCHEMA')
    add('command', 'CREATE TABLE '+empty+'(n numeric,t text)', 'CREATE TABLE')
    add('command', 'CREATE TABLE '+rows+'(id integer,n numeric)', 'CREATE TABLE')
    add('command', 'INSERT INTO '+rows+" VALUES(1,1.2300),(2,0.00),(3,NULL),(4,'NaN'),(5,'Infinity'),(6,-0.000)", 'INSERT 0 6')
    positive = (
        ('1.2300::numeric', 4), ('0.00::numeric', 2), ('-0.000::numeric', 3),
        ('6e-4', 4), ('6e4', 0), ('1::smallint', 0), ('1::integer', 0), ('1::bigint', 0),
        ("'1.2300'", 4), ('NULL', None), ('NULL::numeric', None),
        ("'NaN'::numeric", None), ("'Infinity'::numeric", None), ("'-Infinity'::numeric", None),
        ('1.2300::decimal', 4), ('0.00::numeric(6,4)', 4), ('1.2300::numeric+0.00000::numeric', 5),
        ("'0."+'0'*1000+"'::numeric", 1000),
    )
    suffixes = ('', ' FROM '+empty+' WHERE false', ' FROM '+empty+' LIMIT 0')
    for argument, expected in positive:
        for suffix in suffixes:
            for mode in MODES:
                add('query', mode, 'SELECT scale('+argument+') AS v'+suffix,
                    () if suffix else (expected,), 23)
    invalid = ('', '1,2', 'x=>1', "'1.00'::text", 'NULL::text', '1::real',
               '1::double precision', 'true', "'2000-01-01'::date", 'ARRAY[1]',
               "'\\x31'::bytea", "NULL::uuid")
    for argument in invalid:
        for suffix in suffixes:
            for mode in MODES:
                add('query', mode, 'SELECT scale('+argument+') AS v'+suffix, (), 23, state='42883')
    for modifier in (' OVER ()', ' FILTER (WHERE true)'):
        for mode in MODES:
            add('query', mode, 'SELECT scale(1)'+modifier+' AS v', (), 23, state='42809')
    for argument in ('DISTINCT 1', '1 ORDER BY 1'):
        for mode in MODES:
            add('query', mode, 'SELECT scale('+argument+') AS v', (), 23, state='42809')
    for expression in ("COALESCE(1,scale('bad'))", "CASE WHEN false THEN scale('bad') ELSE 0 END"):
        for mode in MODES:
            add('query', mode, 'SELECT '+expression+' AS v', (), 23, state='22P02')
    for mode in MODES:
        add('query', mode, 'SELECT pg_catalog.scale(0.0000::numeric) AS v', (4,), 23)
        add('query', mode, 'SELECT "scale"(0.0000::numeric) AS v', (4,), 23)
        add('query', mode, 'SELECT "SCALE"(0.0000::numeric) AS v', (), 23, state='42883')
        add('query', mode, 'SELECT pg_typeof(scale(NULL))::text AS v', ('integer',), 25)
        add('query', mode, 'SELECT scale(missing) AS v FROM '+empty+' LIMIT 0', (), 23, state='42703')
        add('query', mode, 'SELECT scale(n) AS v FROM '+rows+' ORDER BY id', (4,2,None,None,None,3), 23)
        add('query', mode, 'SELECT scale(n) AS v FROM '+rows+' WHERE scale(n)>2 ORDER BY id', (4,3), 23)
    inferred = 'scale_inferred'
    add('inferred', inferred, 'SELECT scale($1) AS v')
    parameter_values = (
        ('1.2300', number(4, groups=(1,2300)), 4),
        ('0.00', number(2), 2), ('-0.000', number(3, -1, 0x4000), 3),
        ('0.'+'0'*1000, number(1000), 1000),
        ('NaN', number(0, sign=0xc000), None),
        ('Infinity', number(0, sign=0xd000), None), (None, None, None),
    )
    for binary in (False, True):
        for text, raw, expected in parameter_values:
            add('execute', inferred, 23, (expected,), (1700,), (raw if binary else text,),
                binary_input=binary, binary_output=binary)
    add('close', inferred)
    for oid, raw in ((21, struct.pack('!h', -2)), (23, struct.pack('!i', -2)), (20, struct.pack('!q', -2))):
        name = 'scale_typed_'+str(oid)
        add('describe', name, 'SELECT scale($1) AS v', 23, (oid,))
        for binary in (False, True):
            add('execute', name, 23, (0,), (oid,), (raw if binary else '-2',), binary_input=binary, binary_output=binary)
        add('close', name)
    for oid in (25, 700, 701, 16, 1007):
        add('describe', 'scale_bad_'+str(oid), 'SELECT scale($1) AS v', 23, (oid,), state='42883')
    add('command', 'CREATE SEQUENCE '+seq, 'CREATE SEQUENCE')
    retained = 'scale_retained_effect'
    add('describe', retained, 'SELECT scale(nextval(\''+seq+'\')) AS v', 23)
    add('bind_describe', retained, 23, (), (), False, False)
    add('query', 'simple', "SELECT nextval('"+seq+"') AS v", (1,), 20)
    for binary, expected in ((False,3), (True,5)):
        add('execute', retained, 23, (0,), binary_output=binary)
        add('query', 'simple', "SELECT nextval('"+seq+"') AS v", (expected,), 20)
    add('close', retained)
    for sql, values, state in EXTRA_PHASE_CASES:
        for mode in MODES:
            add('query', mode, sql, values, 23, state=state)
    add('command', 'DROP SCHEMA '+q+' CASCADE', 'DROP SCHEMA', notices=True)
    if PLANNED: assert len(operations) == PLANNED, len(operations)
    return operations


def main():
    if sys.argv[1:] == ['--list']:
        print(len(plan('scale_'+'0'*32)))
        return
    assert PLANNED > 0
    operations = plan('scale_'+uuid.uuid4().hex)
    review = Review('NUMERIC_SCALE_FUNCTION')
    wire = Wire(review)
    owned = False
    try:
        for index, operation in enumerate(operations):
            method, args = operation['method'], list(operation['args'])
            if method in ('inferred', 'describe', 'bind_describe', 'execute', 'close'):
                args[0] = args[0].encode('ascii')
            if method == 'inferred':
                name, sql = args
                wire.collect(('INFERRED', sql), lambda: wire.exchange(wire.parse(name, sql, (0,))),
                             lambda messages: messages == [(b'1',b''),(b't',wire.parameters((1700,))),
                                 (b'T',wire.description(23)),(b'Z',b'I')], 'numeric parameter/int4 output; no execution')
            else:
                before = len(review.failures)
                getattr(wire, method)(*args, **operation['kwargs'])
                if index == 0:
                    owned = len(review.failures) == before
                    assert owned, 'private schema ownership not confirmed'
        owned = False
        review.complete(PLANNED)
    finally:
        if owned: review.client.simple_query(review.sock, operations[-1]['args'][0])
        review.close()


if __name__ == '__main__':
    main()
