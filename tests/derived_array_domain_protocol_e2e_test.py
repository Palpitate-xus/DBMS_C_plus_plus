#!/usr/bin/env python3
"""Compare ordinary derived-array queries and writes with PostgreSQL 18.6."""

import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import uuid

from array_domain_protocol_e2e_test import (
    Checks, ProjectServer, interrupted, reference_connection, runner,
)


def check_workload(checks, names, reconnect):
    source, plain, ints, refs, sequence, domain, other, reference, checked, width = (
        names[name] for name in ('source', 'plain', 'ints', 'refs', 'sequence',
                                'domain', 'other', 'reference', 'checked', 'width')
    )
    array = '[0:1]={2,NULL}'
    references = '[0:2]={integer,NULL,-}'
    q = checks.query

    def setup(label, sql, tag):
        q(label, sql, tag=tag, require_success=True)

    def select(label, sql, rows, headers=('v',), oids=(1007,), *, ready='I', wire_layout=None):
        q(label, sql, tag=f'SELECT {len(rows)}', rows=rows, names=list(headers),
          oids=list(oids), ready=ready, wire_layout=wire_layout)

    def reset(label, value):
        select(label, f"SELECT setval('{sequence}',{value},true) AS calls",
               [[str(value)]], ('calls',), (20,))

    def counter(label, value):
        select(label, f"SELECT currval('{sequence}') AS calls",
               [[str(value)]], ('calls',), (20,))

    def row(label, table, identifier, value, oid=1007, *, ready='I'):
        select(label, f'SELECT id,v FROM {table} WHERE id={identifier}',
               [[str(identifier), value]], ('id', 'v'), (23, oid), ready=ready)

    def copy(label, table, identifier, incoming, value, oid=1007, *, expected_calls=None):
        q(label, f'INSERT INTO {table} SELECT {identifier},q.v FROM {incoming}', tag='INSERT 0 1')
        if expected_calls is not None:
            counter(f'{label}-checks', expected_calls)
        row(f'{label}-stored', table, identifier, value, oid)

    def cast_error(label, sql, state, *, described):
        descriptor = {'oids': [1007], 'names': ['v'], 'wire_layout': [[-1, -1]]} if described else {}
        q(label, sql, tag=None, state=state, **descriptor)

    setup('sequence', f'CREATE SEQUENCE {sequence}', 'CREATE SEQUENCE')
    for name, base_type in ((domain, 'integer[]'), (other, 'integer[]'), (reference, 'regtype[]')):
        setup(f'create-{name.rsplit("_", 1)[-1]}',
              f"CREATE DOMAIN {name} AS {base_type} CHECK(nextval('{sequence}')>0)", 'CREATE DOMAIN')
    setup('create-checked', f'CREATE DOMAIN {checked} AS integer[] CHECK(cardinality(VALUE)<=2)',
          'CREATE DOMAIN')
    setup('create-width', f'CREATE DOMAIN {width} AS varchar(3)[]', 'CREATE DOMAIN')
    setup('source', f'CREATE TABLE {source}(id integer PRIMARY KEY,a integer[],r regtype[],raw text,w {width})',
          'CREATE TABLE')
    setup('seed', f"INSERT INTO {source} VALUES(1,'{array}','{references}','bad-array','{{ab,NULL}}'),"
          "(2,'{1,2,3}',NULL,'{8}','{cd}')", 'INSERT 0 2')
    for label, name, declaration in (
        ('plain', plain, 'integer[]'), ('ints', ints, domain), ('refs', refs, reference),
    ):
        setup(f'table-{label}', f'CREATE TABLE {name}(id integer PRIMARY KEY,v {declaration})', 'CREATE TABLE')
    domain_oid = q('domain-oid', f"SELECT oid FROM pg_type WHERE typname='{domain}'",
                   tag='SELECT 1', names=['oid'], oids=[26], capture_oid=True)
    width_oid = q('width-oid', f"SELECT oid FROM pg_type WHERE typname='{width}'",
                  tag='SELECT 1', names=['oid'], oids=[26], capture_oid=True)

    bare = f"(SELECT '{array}'::integer[] AS v) AS q"
    literal = f"(SELECT '{array}'::{domain} AS v) AS q"
    physical = f'(SELECT a::{domain} AS v FROM {source} WHERE id=1) AS q'
    frozen = f'(SELECT a::{domain} AS v FROM {source} WHERE id=1 OFFSET 0) AS q'
    nested = f"(SELECT inner_q.v FROM (SELECT '{array}'::{domain} AS v) AS inner_q) AS q"
    literal_ref = f"(SELECT '{references}'::{reference} AS v) AS q"
    physical_ref = f'(SELECT r::{reference} AS v FROM {source} WHERE id=1) AS q'
    join = f' JOIN {source} AS peer ON peer.id=1'

    select('plain-derived', f'SELECT q.v FROM {bare}', [[array]], wire_layout=[[-1, -1]])
    reset('read-reset', 100)
    select('literal-derived', f'SELECT q.v FROM {literal}', [[array]], wire_layout=[[-1, -1]])
    counter('literal-derived-check', 101)
    select('nested-derived', f'SELECT q.v FROM {nested}', [[array]], wire_layout=[[-1, -1]])
    counter('nested-derived-check', 102)
    select('physical-derived', f'SELECT q.v FROM {physical}', [[array]])
    counter('physical-derived-check', 103)
    select('physical-derived-reference', f'SELECT q.v FROM {physical_ref}', [[references]],
           oids=(2211,))
    counter('physical-derived-reference-check', 104)
    select('offset-zero-two-references', f'SELECT q.v,pg_typeof(q.v)::oid AS typ FROM {frozen}',
           [[array, domain_oid]], ('v', 'typ'), (1007, 26), wire_layout=[[-1, -1], [4, -1]])
    counter('offset-zero-keeps-one-check', 105)
    select('stored-width-and-domain', f'SELECT q.v,pg_typeof(q.v)::oid AS typ FROM '
           f'(SELECT w FROM {source} WHERE id=1) AS q(v)',
           [['{ab,NULL}', width_oid]], ('v', 'typ'), (1015, 26), wire_layout=[[-1, 7], [4, -1]])

    for label, sql in (
        ('limit-zero', f'SELECT q.v FROM {literal} LIMIT 0'),
        ('false-filter', f'SELECT q.v FROM {literal} WHERE false'),
        ('empty-source', f'SELECT q.v FROM (SELECT a::{domain} AS v FROM {source} WHERE id=99) AS q'),
    ):
        select(label, sql, [], wire_layout=[[-1, -1]])
        counter(f'{label}-no-check', 105)
    select('whole-null', f'SELECT q.v FROM (SELECT CAST(NULL AS {domain}) AS v) AS q LIMIT 1',
           [[None]])
    counter('whole-null-still-checks-its-cast', 106)
    cast_error('malformed-at-bind', f"SELECT q.v FROM (SELECT CAST('bad-array' AS {domain}) AS v) AS q",
               '22P02', described=False)
    cast_error('malformed-at-bind-with-limit-zero',
               f"SELECT q.v FROM (SELECT 'bad-array'::{domain} AS v) AS q LIMIT 0",
               '22P02', described=False)
    cast_error('malformed-at-runtime',
               f'SELECT q.v FROM (SELECT CAST(raw AS {domain}) AS v FROM {source} WHERE id=1) AS q',
               '22P02', described=True)
    counter('malformed-input-never-reaches-check', 106)
    cast_error('valid-input-failed-check',
               f"SELECT q.v FROM (SELECT '{{1,2,3}}'::{checked} AS v) AS q",
               '23514', described=True)
    select('limit-zero-skips-failed-check',
           f'SELECT q.v FROM (SELECT a::{checked} AS v FROM {source} WHERE id=2) AS q LIMIT 0',
           [], wire_layout=[[-1, -1]])

    copy('native-copy', plain, 1, bare, array)
    copy('native-copy-join', plain, 2, bare + join, array)
    reset('copy-reset', 200)
    copy('int-copy', ints, 1, physical, array, expected_calls=201)
    copy('int-copy-join', ints, 2, literal + join, array, expected_calls=202)
    copy('ref-copy', refs, 1, physical_ref, references, 2211, expected_calls=203)
    copy('ref-copy-join', refs, 2, literal_ref + join, references, 2211, expected_calls=204)
    copy('int-copy-null', ints, 3, f'(SELECT NULL::{domain} AS v) AS q', None, expected_calls=205)

    first, second = "'{1}'", "'{2}'"
    branches = (
        ('same-union', f'SELECT 1,{first}::{domain} UNION ALL SELECT 2,{second}::{domain}',
         domain_oid, 2, '{2}'),
        ('mixed-union', f'SELECT 1,{first}::{domain} UNION ALL SELECT 2,{second}::integer[]',
         '1007', 3, '{2}'),
        ('different-domain-union', f'SELECT 1,{first}::{domain} UNION ALL SELECT 2,{second}::{other}',
         '1007', 4, '{2}'),
        ('null-union', f'SELECT 1,{first}::{domain} UNION ALL SELECT 2,NULL',
         '1007', 3, None),
        ('same-values', f'VALUES(1,{first}::{domain}),(2,{second}::{domain})',
         domain_oid, 2, '{2}'),
        ('mixed-values', f'VALUES(1,{first}::{domain}),(2,{second}::integer[])',
         '1007', 3, '{2}'),
    )
    for n, (label, sql, output_oid, executions, second_value) in enumerate(branches):
        incoming = f'({sql}) AS q(id,v)'
        select(f'{label}-type', f'SELECT pg_typeof(q.v)::oid AS typ FROM {incoming} ORDER BY q.id',
               [[output_oid], [output_oid]], ('typ',), (26,))
        start = 300 + n * 10
        reset(f'{label}-reset', start)
        first_id = 10 + n * 2
        q(f'{label}-copy', f'INSERT INTO {ints} SELECT {first_id}-1+q.id,q.v FROM {incoming}',
          tag='INSERT 0 2')
        counter(f'{label}-checks', start + executions)
        select(f'{label}-stored', f'SELECT id,v FROM {ints} WHERE id BETWEEN {first_id} '
               f'AND {first_id+1} ORDER BY id', [[str(first_id), '{1}'], [str(first_id+1), second_value]],
               ('id', 'v'), (23, 1007))

    q('begin-rolled-copy', 'BEGIN', tag='BEGIN', ready='T')
    q('write-in-transaction', f'INSERT INTO {ints} SELECT 99,q.v FROM {physical}',
      tag='INSERT 0 1', ready='T')
    row('visible-before-rollback', ints, 99, array, ready='T')
    q('rollback-copy', 'ROLLBACK', tag='ROLLBACK')
    select('absent-after-rollback', f'SELECT id,v FROM {ints} WHERE id=99', [], ('id','v'), (23, 1007))

    reconnect()
    row('cold-native-copy', plain, 1, array)
    row('cold-int-copy', ints, 1, array)
    row('cold-ref-copy', refs, 2, references, 2211)
    row('cold-sql-null', ints, 3, None)
    select('cold-rolled-back-row-absent', f'SELECT id,v FROM {ints} WHERE id=99', [],
           ('id', 'v'), (23, 1007))
    select('cold-domain-identity', f'SELECT q.v,pg_typeof(q.v)::oid AS typ FROM '
           f'(SELECT v FROM {ints} WHERE id=1 OFFSET 0) AS q',
           [[array, domain_oid]], ('v', 'typ'), (1007, 26))


def check_array_order_and_distinct(checks, table):
    values = {
        1: '{10}', 2: '{2}', 3: '{2}', 4: '{2,NULL}', 5: None,
        6: '[0:0]={2}', 7: '{{2}}', 8: '{}', 9: '{NULL}',
        10: '[1:1][0:1]={{2,NULL}}', 11: '{{2},{NULL}}',
        12: '[1:1][0:1]={{2,NULL}}', 13: '{-3}', 14: None, 15: '[0:1]={2,NULL}',
    }
    # Equal arrays form peers. Their IDs give a deterministic second key.
    ascending = (
        (8,), (13,), (6,), (2, 3), (7,), (15,), (4,),
        (10, 12), (11,), (1,), (9,), (5, 14),
    )
    plain = f'(SELECT id,a FROM {table}) AS q'
    short = f'(SELECT id,a FROM {table} WHERE id<=3) AS q'
    q = checks.query
    q('order-create-table', f'CREATE TABLE {table}(id integer PRIMARY KEY,a integer[])',
      tag='CREATE TABLE', require_success=True)
    entries = ','.join(f"({identifier}," + ('NULL' if value is None else f"'{value}'") + ')'
                       for identifier, value in values.items())
    q('order-seed', f'INSERT INTO {table} VALUES {entries}',
      tag=f'INSERT 0 {len(values)}', require_success=True)

    def rows(groups):
        return [[str(identifier), values[identifier]] for group in groups for identifier in group]

    def selected(label, sql, expected, names=None, oids=None, layout=None):
        q(label, sql, tag=f'SELECT {len(expected)}', rows=expected,
          names=names or ['id', 'a'], oids=oids or [23, 1007],
          wire_layout=layout or [[4, -1], [-1, -1]])

    def arrays(label, sql, expected, name='a'):
        selected(label, sql, [[value] for value in expected], [name], [1007], [[-1, -1]])

    selected('array-order-numeric', f'SELECT q.id,q.a FROM {short} ORDER BY q.a,q.id',
             rows(((2, 3), (1,))))
    selected('array-order-shape', f'SELECT q.id,q.a FROM {plain} ORDER BY q.a NULLS LAST,q.id',
             rows(ascending))
    selected('array-order-reverse-and-nulls',
             f'SELECT q.id,q.a FROM {plain} ORDER BY q.a DESC NULLS FIRST,q.id',
             rows(reversed(ascending)))
    selected('array-order-with-limit', f'SELECT q.id,q.a FROM {short} ORDER BY q.a,q.id LIMIT 1',
             rows(((2,),)))
    arrays('array-order-ordinal', f'SELECT q.a FROM {short} ORDER BY 1',
           ['{2}', '{2}', '{10}'])
    arrays('array-order-alias', f'SELECT q.a AS key FROM {short} ORDER BY key',
           ['{2}', '{2}', '{10}'], 'key')
    selected('array-order-with-scalar-output', f'SELECT q.id FROM {short} ORDER BY q.a,q.id',
             [['2'], ['3'], ['1']], ['id'], [23], [[4, -1]])
    arrays('array-order-nested',
           f'SELECT nested.a FROM (SELECT q.a FROM {plain}) AS nested ORDER BY nested.a',
           [values[identifier] for group in ascending for identifier in group])
    selected('array-order-not-materialized',
             f'WITH q AS NOT MATERIALIZED (SELECT id,a FROM {table}) '
             'SELECT q.id,q.a FROM q ORDER BY q.a NULLS LAST,q.id', rows(ascending))
    arrays('array-distinct-order', f'SELECT DISTINCT q.a FROM {plain} ORDER BY q.a NULLS LAST',
           [values[group[0]] for group in ascending])
    arrays('array-distinct-without-order',
           f'SELECT DISTINCT q.a FROM (SELECT id,a FROM {table} WHERE id IN (2,3)) AS q',
           ['{2}'])
    arrays('array-order-from-values',
           'SELECT q.a FROM (VALUES(ARRAY[10]),(ARRAY[2]),(ARRAY[NULL]::integer[])) AS q(a) ORDER BY q.a',
           ['{2}', '{10}', '{NULL}'])
    arrays('array-order-from-union',
           'SELECT q.a FROM (SELECT ARRAY[10] AS a UNION ALL SELECT ARRAY[2] '
           'UNION ALL SELECT ARRAY[2]) AS q ORDER BY q.a',
           ['{2}', '{2}', '{10}'])
    arrays('array-order-with-ties', f'SELECT q.a FROM {short} ORDER BY q.a FETCH FIRST 1 ROW WITH TIES',
           ['{2}', '{2}'])


def check_array_value_comparisons(checks):
    q = checks.query

    def source(element, values):
        literals = ['NULL' if value is None else f"'{value}'" for value in values]
        rows = ','.join(f'({i},{literal}::{element}[])' for i, literal in enumerate(literals, 1))
        return f'(VALUES {rows}) AS q(id,a)'

    def ordered(label, relation, ids):
        q(label, f'SELECT q.id FROM {relation} ORDER BY q.a,q.id',
          tag=f'SELECT {len(ids)}', rows=[[str(i)] for i in ids], names=['id'],
          oids=[23], wire_layout=[[4, -1]])

    def distinct_count(label, relation, count, keys='q.a'):
        q(label, f'SELECT count(*) AS n FROM (SELECT DISTINCT {keys} FROM {relation}) AS d',
          tag='SELECT 1', rows=[[str(count)]], names=['n'], oids=[20], wire_layout=[[8, -1]])

    float8 = source('double precision', ('{NaN}', '{Infinity}', '{-0}', '{0}',
                                         '{-Infinity}', '{NaN}', '{NULL}', None))
    ordered('float8-array-specials-order', float8, [5, 3, 4, 2, 1, 6, 7, 8])
    distinct_count('float8-array-specials-distinct', float8, 6)
    q('float8-array-signed-zero-peers',
      f'SELECT 1 AS marker FROM {float8} WHERE q.id BETWEEN 3 AND 5 '
      'ORDER BY q.a DESC FETCH FIRST 1 ROW WITH TIES',
      tag='SELECT 2', rows=[['1'], ['1']], names=['marker'], oids=[23], wire_layout=[[4, -1]])

    float4 = source('real', ('{16777217}', '{16777216}', '{16777218}'))
    ordered('float4-array-rounded-order', float4, [1, 2, 3])
    distinct_count('float4-array-rounded-distinct', float4, 2)

    numeric = source('numeric', ('{10}', '{2.00}', '{2.0}', '{-0.000000000000000001}',
                                 '{2.000000000000000001}', None))
    ordered('numeric-array-exact-order', numeric, [4, 2, 3, 5, 1, 6])
    distinct_count('numeric-array-scale-distinct', numeric, 5)

    strings = ('{2}', '{10}', '{a}', '{"a "}', '{NULL}')
    text = source('text', strings)
    ordered('text-array-order', text, [2, 1, 3, 4, 5])
    distinct_count('text-array-trailing-space-distinct', text, 5)
    bpchar = source('pg_catalog.bpchar', strings)
    ordered('bpchar-array-order', bpchar, [2, 1, 3, 4, 5])
    distinct_count('bpchar-array-padding-distinct', bpchar, 4)

    distinct_count('multiple-array-key-positions',
                   "(VALUES (1,'{0.0}'::numeric[],'{2}'::integer[]),"
                   "(1,'{0.00}'::numeric[],'{2}'::integer[]),"
                   "(2,'{0.00}'::numeric[],'{2}'::integer[]),"
                   "(1,NULL::numeric[],'{2}'::integer[]),"
                   "(1,NULL::numeric[],'{2}'::integer[]),"
                   "(1,'{0.00}'::numeric[],NULL::integer[])) AS q(id,a,b)",
                   4, 'q.id,q.a,q.b')


def check_temporal_array_order(checks, table):
    q = checks.query
    dates = [
        '{2024-10-02}', '{2024-02-03}', '{2024-02-03}', '[0:0]={2024-02-03}',
        '{}', '{NULL}', None, '{2024-02-03,NULL}', '{2023-12-31}',
        '{2024-02-03,2024-02-03}', None, '{2025-01-01}',
    ]
    times = [
        '{12:00:00.000002}', '{12:00:00.100000}', '{12:00:00.1}', '[0:0]={12:00:00.1}',
        '{}', '{NULL}', None, '{24:00:00}', '{00:00:00.000001}', '{12:00:00}',
        None, '{23:59:59.999999}',
    ]
    q('temporal-array-create', f'CREATE TABLE {table}(id integer PRIMARY KEY,d date[],t time[])',
      tag='CREATE TABLE', require_success=True)

    def literal(value):
        return 'NULL' if value is None else f"'{value}'"

    values = ','.join(f'({i},{literal(date)},{literal(time)})'
                      for i, (date, time) in enumerate(zip(dates, times), 1))
    q('temporal-array-seed', f'INSERT INTO {table} VALUES {values}',
      tag=f'INSERT 0 {len(dates)}', require_success=True)

    expected_times = times.copy()
    expected_times[1] = '{12:00:00.1}'
    for kind, column, oid, expected, ascending in (
        ('date', 'd', 1182, dates, (5, 9, 4, 2, 3, 10, 8, 1, 12, 6, 7, 11)),
        ('time', 't', 1183, expected_times, (5, 9, 10, 1, 4, 2, 3, 12, 8, 6, 7, 11)),
    ):
        source = f'(SELECT id,{column} AS a FROM {table} OFFSET 0) AS q'
        ordered = [expected[i - 1] for i in ascending]
        q(f'{kind}-array-physical-order', f'SELECT q.id,q.a FROM {source} ORDER BY q.a,q.id',
          tag=f'SELECT {len(ascending)}', rows=[[str(i), a] for i, a in zip(ascending, ordered)],
          names=['id', 'a'], oids=[23, oid], wire_layout=[[4, -1], [-1, -1]])
        unique = list(dict.fromkeys(ordered))
        q(f'{kind}-array-physical-distinct', f'SELECT DISTINCT q.a FROM {source} ORDER BY q.a',
          tag=f'SELECT {len(unique)}', rows=[[a] for a in unique], names=['a'], oids=[oid],
          wire_layout=[[-1, -1]])

    q('time-array-equivalent-precision-peers',
      f'SELECT q.a FROM (SELECT id,t AS a FROM {table} OFFSET 0) AS q WHERE q.id IN (2,3,10) '
      'ORDER BY q.a DESC FETCH FIRST 1 ROW WITH TIES',
      tag='SELECT 2', rows=[['{12:00:00.1}'], ['{12:00:00.1}']], names=['a'], oids=[1183],
      wire_layout=[[-1, -1]])


def check_timetz_and_jsonb_array_order(checks, timetz_table, jsonb_table):
    q = checks.query

    def source(table, element, cells):
        q(f'{element}-array-create', f'CREATE TABLE {table}(id integer PRIMARY KEY,a {element}[])',
          tag='CREATE TABLE', require_success=True)
        values = ','.join(f'({i},{value})' for i, value in enumerate(cells, 1))
        q(f'{element}-array-seed', f'INSERT INTO {table} VALUES {values}',
          tag=f'INSERT 0 {len(cells)}', require_success=True)
        return f'(SELECT id,a FROM {table} OFFSET 0) AS q'

    def integers(label, sql, numbers, name='id'):
        q(label, sql, tag=f'SELECT {len(numbers)}', rows=[[str(i)] for i in numbers],
          names=[name], oids=[23], wire_layout=[[4, -1]])

    def distinct_count(label, relation, count):
        q(label, f'SELECT count(*) AS n FROM (SELECT DISTINCT q.a FROM {relation}) AS d',
          tag='SELECT 1', rows=[[str(count)]], names=['n'], oids=[20], wire_layout=[[8, -1]])

    times = (
        '{10:00:00-01}', '{12:00:00+01}', '{14:00:00+03}', '{11:00:00+00}',
        '{12:00:00.000001+01}', '{00:30:00+02}', '{22:30:00-02}', '{NULL}',
        None, '[-1:-1]={12:00:00+01}', '{}', '{12:00:00+01:00}',
        '{24:00:00+00}', '{00:00:00+00}', None, '{11:00:17+00:00:17}',
    )
    timetz = source(timetz_table, 'timetz', ['NULL' if a is None else f"'{a}'" for a in times])
    integers('timetz-array-order', f'SELECT q.id FROM {timetz} ORDER BY q.a,q.id',
             [11, 6, 14, 3, 10, 2, 12, 16, 4, 1, 5, 13, 7, 8, 9, 15])
    integers('timetz-array-descending-null-placement',
             f'SELECT q.id FROM {timetz} ORDER BY q.a DESC NULLS FIRST,q.id',
             [9, 15, 8, 7, 13, 5, 1, 4, 16, 2, 12, 10, 3, 14, 6, 11])
    distinct_count('timetz-array-distinct-offset-and-bounds', timetz, 14)
    integers('timetz-array-equivalent-offset-peers',
             f'SELECT 1 AS marker FROM {timetz} WHERE q.id IN (1,2,4,12) '
             'ORDER BY q.a FETCH FIRST 1 ROW WITH TIES', [1, 1], 'marker')
    integers('timetz-array-different-offset-peers',
             f'SELECT q.id FROM {timetz} WHERE q.id IN (2,4,12) '
             'ORDER BY q.a DESC FETCH FIRST 1 ROW WITH TIES', [4])

    json_elements = (
        '{}', 'null', '[]', '"zz"', 'true', '9007199254740993', '9007199254740992',
        '[0,0]', '[99]', None, None, '{"b":1,"d":1}', '{"aa":1,"c":1}',
        '{"b":1,"d":1,"b":1.0}', '{"aa":1,"é":1}',
    )
    json_cells = [f"ARRAY['{value}'::jsonb]" if value is not None else 'ARRAY[NULL::jsonb]'
                  for value in json_elements]
    json_cells[10] = 'NULL'
    json_cells += ["'[0:0]={3}'::jsonb[]", "'{3}'::jsonb[]", "'{3.00}'::jsonb[]"]
    jsonb = source(jsonb_table, 'jsonb', json_cells)
    q('jsonb-array-projection', f'SELECT q.a FROM {jsonb} WHERE q.id IN (3,10,11) ORDER BY q.id',
      tag='SELECT 3', rows=[['{[]}'], ['{NULL}'], [None]], names=['a'], oids=[3807],
      wire_layout=[[-1, -1]])
    integers('jsonb-array-order', f'SELECT q.id FROM {jsonb} ORDER BY q.a,q.id',
             [3, 2, 4, 16, 17, 18, 7, 6, 5, 9, 8, 1, 15, 12, 14, 13, 10, 11])
    distinct_count('jsonb-array-distinct-numeric-scale-and-bounds', jsonb, 16)
    integers('jsonb-array-equivalent-numeric-scale-peers',
             f'SELECT 1 AS marker FROM {jsonb} WHERE q.id IN (16,17,18) '
             'ORDER BY q.a DESC FETCH FIRST 1 ROW WITH TIES', [1, 1], 'marker')


def main():
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    reference = sys.argv[1:] == ['--reference18']
    assert reference or not sys.argv[1:], 'expected --reference18 or no arguments'
    client = runner.load_protocol_client()
    evidence_root = os.environ.get('DBMS_TEST_EVIDENCE_DIR')
    evidence = Path(evidence_root) if evidence_root else None
    if evidence is not None:
        evidence.mkdir(parents=True, exist_ok=False)
    prefix = 'derived_array_' + uuid.uuid4().hex[:12]
    names = {name: f'{prefix}_{name}' for name in (
        'source', 'plain', 'ints', 'refs', 'sequence', 'domain', 'other', 'reference', 'checked', 'width',
        'array_order', 'temporal_order', 'timetz_order', 'jsonb_order',
    )}
    checks = Checks(client, evidence)
    with tempfile.TemporaryDirectory(prefix='dbms-derived-arrays-') as directory:
        server = None if reference else ProjectServer(client, Path(directory))

        def reconnect():
            if server is not None:
                server.stop()
                checks.sock = server.start()
            else:
                if checks.sock is not None:
                    checks.sock.close()
                    checks.sock = None
                checks.sock = reference_connection(client)

        complete = stopped = False
        try:
            reconnect()
            check_workload(checks, names, reconnect)
            check_array_order_and_distinct(checks, names['array_order'])
            check_array_value_comparisons(checks)
            check_temporal_array_order(checks, names['temporal_order'])
            check_timetz_and_jsonb_array_order(checks, names['timetz_order'], names['jsonb_order'])
            failures = [record['label'] for record in checks.records if not record['pass']]
            assert not failures, failures
            complete = True
        finally:
            try:
                if server is not None:
                    server.stop()
                elif checks.sock is not None:
                    try:
                        client.simple_query(checks.sock, 'ROLLBACK')
                        for name in ('source', 'plain', 'ints', 'refs', 'array_order', 'temporal_order',
                                     'timetz_order', 'jsonb_order'):
                            client.simple_query(checks.sock, f'DROP TABLE IF EXISTS {names[name]}')
                        for name in ('domain', 'other', 'reference', 'checked', 'width'):
                            client.simple_query(checks.sock, f'DROP DOMAIN IF EXISTS {names[name]}')
                        client.simple_query(checks.sock, f"DROP SEQUENCE IF EXISTS {names['sequence']}")
                    finally:
                        checks.sock.close()
                stopped = True
            finally:
                if evidence is not None:
                    program = None if reference else Path(runner.DBMS_MAIN).resolve()

                    def sha(path):
                        return hashlib.sha256(path.read_bytes()).hexdigest()

                    report = {
                        'complete': complete and stopped, 'prefix': prefix,
                        'reference': 'PostgreSQL 18.6' if reference else None,
                        'program': str(program) if program else None,
                        'program_sha256': sha(program) if program else None,
                        'test_sha256': sha(Path(__file__)),
                        'checks': len(checks.records),
                        'passes': sum(record['pass'] for record in checks.records),
                        'stops': server.stops if server is not None else [],
                        'records': checks.records,
                    }
                    (evidence / 'summary.json').write_text(json.dumps(report, indent=2) + '\n',
                                                           encoding='utf-8')
    print('DERIVED_ARRAY_DOMAIN_PROTOCOL_PASS', len(checks.records))


if __name__ == '__main__':
    main()
