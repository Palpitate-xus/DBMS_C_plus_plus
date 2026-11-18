#!/usr/bin/env python3
"""Check array-domain join values, descriptors, and assignment checks."""

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


def workload(checks, prefix, objects):
    q = checks.query
    domain_oids = {}
    table_oids = {}

    def name(symbol):
        return f'{prefix}_{symbol}'

    def create(label, kind, symbol, declaration=''):
        identifier = name(symbol)
        objects.append((kind, identifier))
        q(label, f'CREATE {kind} {identifier}{declaration}',
          tag=f'CREATE {kind}', require_success=True)

    def identity(label, catalog, column, symbol):
        return q(label, f"SELECT oid FROM {catalog} WHERE {column}='{name(symbol)}'",
                 tag='SELECT 1', names=['oid'], oids=[26], capture_oid=True)

    def origin(table, column=2):
        return [table_oids[table], column] if table is not None else [0, 0]

    def select(label, sql, rows, names, oids, layout, origins=None):
        q(label, sql, tag=f'SELECT {len(rows)}', rows=rows, names=names, oids=oids,
          wire_layout=layout)
        if origins is not None:
            record = checks.records[-1]
            record['expected']['origins'] = origins
            if [field[1:3] for field in record['fields']] != origins:
                record['pass'] = False
                print('ARRAY_DOMAIN_JOIN_ORIGIN_DIFFERENCE', json.dumps(record), flush=True)

    def number(label, sql, value):
        select(label, sql, [[str(value)]], ['calls'], [20], [[8, -1]])

    def joined(left, right, kind):
        return f'{name(left)} AS l {kind} JOIN {name(right)} AS r USING(k)'

    create('tap-sequence', 'SEQUENCE', 'u_tap')
    sequence = name('u_tap')
    domains = {
        'u_d': f"integer[] CHECK(nextval('{sequence}')>0)",
        'u_e': f"integer[] CHECK(nextval('{sequence}')>0)",
        'u_w3': 'varchar(3)[]', 'u_w3b': 'varchar(3)[]', 'u_w5': 'varchar(5)[]',
    }
    for symbol, declaration in domains.items():
        create(f'domain-{symbol}', 'DOMAIN', symbol, f' AS {declaration}')
        domain_oids[symbol] = identity(f'type-oid-{symbol}', 'pg_type', 'typname', symbol)
    assert len(set(domain_oids.values())) == len(domains)

    tables = {
        'u_ld': ('u_d', 'int-left', -1), 'u_rd': ('u_d', 'int-right', -1),
        'u_re': ('u_e', 'int-right', -1), 'u_rb': ('integer[]', 'int-right', -1),
        'u_l3d': ('u_w3', 'text-left', 7), 'u_r3d': ('u_w3', 'text-right', 7),
        'u_r3b': ('u_w3b', 'text-right', 7), 'u_r5d': ('u_w5', 'text-right', 9),
        'u_l3': ('varchar(3)[]', 'text-left', 7), 'u_r3': ('varchar(3)[]', 'text-right', 7),
        'u_r5': ('varchar(5)[]', 'text-right', 9), 'u_sink': ('u_d', None, -1),
    }
    seeds = {
        'int-left': "(1,'{2}'),(2,'{10}')", 'int-right': "(10,'{2}'),(30,'{3}')",
        'text-left': "(1,'{ab}'),(2,'{abc}')", 'text-right': "(10,'{ab}'),(30,'{xy}')",
    }
    for table, (declaration, seed, _) in tables.items():
        create(f'table-{table}', 'TABLE', table,
               f'(id integer PRIMARY KEY,k {name(declaration) if declaration in domains else declaration})')
        if seed is not None:
            q(f'seed-{table}', f'INSERT INTO {name(table)} VALUES {seeds[seed]}',
              tag='INSERT 0 2', require_success=True)
        table_oids[table] = int(identity(f'relation-oid-{table}', 'pg_class', 'relname', table))
    assert len(set(table_oids.values())) == len(tables)

    kinds = ('INNER', 'LEFT', 'RIGHT', 'FULL')
    # Each origin tuple follows the four join kinds. None denotes a computed field.
    pairs = (
        ('same-integer-domain', 'u_ld', 'u_rd', 1007, -1, ('u_ld', 'u_ld', 'u_rd', None)),
        ('mixed-integer-domains', 'u_ld', 'u_re', 1007, -1, (None,)*4),
        ('integer-domain-and-base', 'u_ld', 'u_rb', 1007, -1, ('u_rb', None, 'u_rb', None)),
        ('integer-base-and-domain', 'u_rb', 'u_ld', 1007, -1, ('u_rb', 'u_rb', None, None)),
        ('same-width-domain', 'u_l3d', 'u_r3d', 1015, 7, ('u_l3d', 'u_l3d', 'u_r3d', None)),
        ('mixed-domains-same-width', 'u_l3d', 'u_r3b', 1015, -1, (None,)*4),
        ('mixed-domains-different-width', 'u_l3d', 'u_r5d', 1015, -1, (None,)*4),
        ('plain-arrays-same-width', 'u_l3', 'u_r3', 1015, 7, ('u_l3', 'u_l3', 'u_r3', None)),
        ('plain-arrays-different-width', 'u_l3', 'u_r5', 1015, -1, (None,)*4),
    )
    order = ' ORDER BY l.id NULLS LAST,r.id NULLS LAST'
    columns = ('k,l.k AS lk,r.k AS rk,pg_typeof(k)::oid AS kt,'
               'pg_typeof(l.k)::oid AS lt,pg_typeof(r.k)::oid AS rt')
    headers = ['k', 'lk', 'rk', 'kt', 'lt', 'rt']
    for label, left, right, base_oid, typmod, sources in pairs:
        left_type, _, left_mod = tables[left]
        right_type, _, right_mod = tables[right]
        left_oid = domain_oids.get(left_type, str(base_oid))
        right_oid = domain_oids.get(right_type, str(base_oid))
        merged_oid = left_oid if left_oid == right_oid else str(base_oid)
        sql_types = [merged_oid, left_oid, right_oid]
        layout = [[-1, typmod], [-1, left_mod], [-1, right_mod]] + [[4, -1]]*3
        shared, left_only, right_only = ('{2}', '{10}', '{3}') if base_oid == 1007 else (
            '{ab}', '{abc}', '{xy}')
        if left == 'u_rb':
            left_only, right_only = right_only, left_only
        for kind, source in zip(kinds, sources):
            rows = [[shared, shared, shared, *sql_types]]
            if kind in ('LEFT', 'FULL'):
                rows.append([left_only, left_only, None, *sql_types])
            if kind in ('RIGHT', 'FULL'):
                rows.append([right_only, None, right_only, *sql_types])
            select(f'{label}-{kind.lower()}', f'SELECT {columns} FROM {joined(left, right, kind)}{order}',
                   rows, headers, [base_oid]*3+[26]*3, layout,
                   [origin(source), origin(left), origin(right)] + [origin(None)]*3)

    select('nested-same-domain', 'SELECT q.k,pg_typeof(q.k)::oid AS kt FROM '
           f'(SELECT k FROM {name("u_ld")} AS l JOIN {name("u_rd")} AS r USING(k) OFFSET 0) AS q',
           [['{2}', domain_oids['u_d']]], ['k', 'kt'], [1007, 26], [[-1, -1], [4, -1]],
           [origin('u_ld'), origin(None)])

    number('tap-reset', f"SELECT setval('{sequence}',100,true) AS calls", 100)
    writes = (
        ('same-merged-inner', 'INNER', 'u_rd', 'k', 'l.id=1', '{2}', 0),
        ('same-merged-left-unmatched', 'LEFT', 'u_rd', 'k', 'l.id=2', '{10}', 0),
        ('same-merged-right-unmatched', 'RIGHT', 'u_rd', 'k', 'r.id=30', '{3}', 0),
        ('same-merged-full-unmatched', 'FULL', 'u_rd', 'k', 'r.id=30', '{3}', 0),
        ('same-qualified-null', 'LEFT', 'u_rd', 'r.k', 'l.id=2', None, 0),
        ('mixed-merged-inner', 'INNER', 'u_re', 'k', 'l.id=1', '{2}', 1),
        ('mixed-qualified-same', 'INNER', 'u_re', 'l.k', 'l.id=1', '{2}', 0),
        ('mixed-qualified-other', 'INNER', 'u_re', 'r.k', 'l.id=1', '{2}', 1),
        ('mixed-merged-full-unmatched', 'FULL', 'u_re', 'k', 'l.id=2', '{10}', 1),
        ('mixed-plain-qualified-null', 'LEFT', 'u_rb', 'r.k', 'l.id=2', None, 1),
    )
    calls = 100
    sink = name('u_sink')
    data = []
    for identifier, (label, kind, right, expression, predicate, value, taps) in enumerate(writes, 1):
        q(f'insert-{label}', f'INSERT INTO {sink} SELECT {identifier},{expression} '
          f'FROM {joined("u_ld", right, kind)} WHERE {predicate}', tag='INSERT 0 1')
        calls += taps
        number(f'tap-{label}', f"SELECT currval('{sequence}') AS calls", calls)
        data.append([str(identifier), value])
    q('insert-empty', f'INSERT INTO {sink} SELECT 99,k FROM {joined("u_ld", "u_re", "FULL")} LIMIT 0',
      tag='INSERT 0 0')
    number('tap-empty', f"SELECT currval('{sequence}') AS calls", calls)
    select('stored-results', f'SELECT id,k FROM {sink} ORDER BY id', data,
           ['id', 'k'], [23, 1007], [[4, -1], [-1, -1]], [origin('u_sink', 1), origin('u_sink')])

    full = joined('u_l3d', 'u_r3d', 'FULL')
    width_oid = domain_oids['u_w3']
    select('qualified-wildcard', f'SELECT l.*,r.*,k,pg_typeof(k)::oid AS kt FROM {full}{order}',
           [['1', '{ab}', '10', '{ab}', '{ab}', width_oid],
            ['2', '{abc}', None, None, '{abc}', width_oid],
            [None, None, '30', '{xy}', '{xy}', width_oid]],
           ['id', 'k', 'id', 'k', 'k', 'kt'], [23, 1015, 23, 1015, 1015, 26],
           [[4, -1], [-1, 7], [4, -1], [-1, 7], [-1, 7], [4, -1]],
           [origin('u_l3d', 1), origin('u_l3d'), origin('u_r3d', 1), origin('u_r3d'),
            origin(None), origin(None)])
    select('empty-join-description', f'SELECT {columns} FROM {full} LIMIT 0', [], headers,
           [1015]*3+[26]*3, [[-1, 7]]*3+[[4, -1]]*3,
           [origin(None), origin('u_l3d'), origin('u_r3d')] + [origin(None)]*3)

    create('table-bigint-array', 'TABLE', 'u_big', '(id integer PRIMARY KEY,k bigint[])')
    q('seed-bigint-array', f"INSERT INTO {name('u_big')} VALUES (20,'{{2}}'),(40,'{{4}}')",
      tag='INSERT 0 2', require_success=True)
    table_oids['u_big'] = int(identity('relation-oid-u_big', 'pg_class', 'relname', 'u_big'))
    for kind, limit in (('INNER', ''), ('LEFT', ' LIMIT 0'), ('RIGHT', ''), ('FULL', ' LIMIT 0')):
        q(f'different-array-types-{kind.lower()}',
          f'SELECT k FROM {joined("u_rb", "u_big", kind)}{limit}', tag=None, state='42883')
    converted = f'(SELECT id,k::bigint[] AS k FROM {name("u_rb")} OFFSET 0) AS l'
    select('explicit-array-key-conversion',
           f'SELECT k,l.k AS lk,r.k AS rk,pg_typeof(k)::oid AS kt FROM {converted} '
           f'JOIN {name("u_big")} AS r USING(k) WHERE l.id=10',
           [['{2}', '{2}', '{2}', '1016']], ['k', 'lk', 'rk', 'kt'], [1016, 1016, 1016, 26],
           [[-1, -1]]*3+[[4, -1]], [origin(None), origin(None), origin('u_big'), origin(None)])

    for symbol, id_column, entries in (
        ('u_numeric_left', 'lid', "(1,'{2.0}'),(2,'{10.0}')"),
        ('u_numeric_right', 'rid', "(11,'{2.00}'),(12,'{3.00}')"),
    ):
        create(f'table-{symbol}', 'TABLE', symbol, f'({id_column} integer PRIMARY KEY,k numeric[])')
        q(f'seed-{symbol}', f'INSERT INTO {name(symbol)} VALUES {entries}',
          tag='INSERT 0 2', require_success=True)
        table_oids[symbol] = int(identity(f'relation-oid-{symbol}', 'pg_class', 'relname', symbol))

    left, right = 'u_numeric_left', 'u_numeric_right'
    unmatched_left = ['{10.0}', '{10.0}', None]
    unmatched_right = ['{3.00}', None, '{3.00}']
    numeric = (
        ('INNER', left, [['{2.0}', '{2.0}', '{2.00}']]),
        ('LEFT', left, [['{2.0}', '{2.0}', '{2.00}'], unmatched_left]),
        ('RIGHT', right, [['{2.00}', '{2.0}', '{2.00}'], unmatched_right]),
        ('FULL', None, [['{2.0}', '{2.0}', '{2.00}'], unmatched_left, unmatched_right]),
    )
    numeric_order = ' ORDER BY l.lid NULLS LAST,r.rid NULLS LAST'
    for kind, source, rows in numeric:
        select(f'numeric-array-representation-{kind.lower()}',
               f'SELECT k,l.k AS lk,r.k AS rk FROM {joined(left, right, kind)}{numeric_order}',
               rows, ['k', 'lk', 'rk'], [1231]*3, [[-1, -1]]*3,
               [origin(source), origin(left), origin(right)])

    select('numeric-array-natural-full', 'SELECT k,l.k AS lk,r.k AS rk FROM '
           f'{name(left)} AS l NATURAL FULL JOIN {name(right)} AS r{numeric_order}',
           numeric[-1][2], ['k', 'lk', 'rk'], [1231]*3, [[-1, -1]]*3,
           [origin(None), origin(left), origin(right)])


def array_pair_workload(checks, prefix, objects):
    q = checks.query

    def name(symbol):
        return f'{prefix}_{symbol}'

    def create(label, kind, symbol, declaration=''):
        identifier = name(symbol)
        objects.append((kind, identifier))
        q(label, f'CREATE {kind} {identifier}{declaration}',
          tag=f'CREATE {kind}', require_success=True)

    def identity(label, catalog, column, symbol):
        return q(label, f"SELECT oid FROM {catalog} WHERE {column}='{name(symbol)}'", tag='SELECT 1',
                 names=['oid'], oids=[26], capture_oid=True, wire_layout=[[4, -1]])

    def select(label, sql, rows, names, oids, origins=None, state=None):
        layout = [[8, -1] if oid == 20 else [4, -1] if oid in (23, 26) else [-1, -1]
                  for oid in oids]
        q(label, sql, tag=None if state else f'SELECT {len(rows)}', rows=rows, names=names, oids=oids,
          wire_layout=layout, state=state)
        if origins is not None:
            record = checks.records[-1]
            record['expected']['origins'] = origins
            if [field[1:3] for field in record['fields']] != origins:
                record['pass'] = False
                print('ARRAY_DOMAIN_JOIN_ORIGIN_DIFFERENCE', json.dumps(record), flush=True)

    def literal(value):
        if value is None:
            return 'NULL'
        return f"'{value}'" if isinstance(value, str) else str(value)

    def seed(label, symbol, cells):
        values = ','.join('(' + ','.join(map(literal, row)) + ')' for row in cells)
        q(label, f'INSERT INTO {name(symbol)} VALUES {values}', tag=f'INSERT 0 {len(cells)}',
          require_success=True)

    def number(label, sql, value):
        select(label, sql, [[str(value)]], ['calls'], [20])

    def relation_oid(symbol):
        return int(identity(f'relation-oid-{symbol}', 'pg_class', 'relname', symbol))

    matches = ((1, 10), (1, 11), (2, 10), (2, 11), (4, 13), (5, 14), (6, 15),
               (7, 16), (8, 17), (10, 20))
    left_only, right_only = ((3, None), (9, None)), ((None, 12), (None, 18), (None, 19))
    second_key = ((1, 11), (2, 10), (5, 14), (6, 15), (7, 16), (8, 17), (10, 20))
    for element, letter, oid, point, near, edge in (
        ('smallint', 's', 1005, -32768, -32767, 32767),
        ('integer', 'i', 1007, -2147483648, -2147483647, 2147483647),
        ('bigint', 'b', 1016, 9007199254740992, 9007199254740993, -9223372036854775808),
    ):
        left, right = f'p_{letter}_left', f'p_{letter}_right'
        key = f'{{{point},NULL}}'
        offset = f'[0:1]={key}'
        rank_two = '{' + key + '}'
        boundary = f'{{{edge}}}'
        long_key = '{' + ','.join([str(point)]*32 + [str(edge)]) + '}'
        left_keys = (key, key, None, offset, rank_two, boundary, '{}', '{NULL}',
                     f'{{{near},NULL}}', long_key)
        right_keys = (key, key, None, offset, rank_two, boundary, '{}', '{NULL}', '{7}',
                      '{{' + str(point) + '},{NULL}}', long_key)
        left_second = {2: ('{2}', 2), 4: (None, None)}
        right_second = {10: ('{2}', 2), 13: (None, None)}
        left_data = [(i, k, *left_second.get(i, ('{1}', 1))) for i, k in enumerate(left_keys, 1)]
        right_data = [(i, k, *right_second.get(i, ('{1}', 1))) for i, k in enumerate(right_keys, 10)]
        for table, identifier, data in ((left, 'lid', left_data), (right, 'rid', right_data)):
            create(f'table-{table}', 'TABLE', table,
                   f'({identifier} integer PRIMARY KEY,k {element}[],m {element}[],s integer)')
            seed(f'seed-{table}', table, data)
        left_oid, right_oid = relation_oid(left), relation_oid(right)
        relation = f'{name(left)} AS l {{}} JOIN {name(right)} AS r USING'
        for kind, unmatched, key_origin in (
            ('INNER', (), [left_oid, 2]), ('LEFT', left_only, [left_oid, 2]),
            ('RIGHT', right_only, [right_oid, 2]), ('FULL', left_only+right_only, [0,0]),
        ):
            pairs = sorted(matches+unmatched, key=lambda pair: tuple((n is None, n) for n in pair))
            rows = [[None if lid is None else str(lid), None if rid is None else str(rid),
                     right_keys[rid-10] if lid is None else left_keys[lid-1]] for lid, rid in pairs]
            select(f'pair-{letter}-{kind.lower()}', f'SELECT l.lid,r.rid,k FROM {relation.format(kind)}(k) '
                   'ORDER BY l.lid NULLS LAST,r.rid NULLS LAST', rows, ['lid','rid','k'], [23,23,oid],
                   [[left_oid,1],[right_oid,1],key_origin])
        for other in ('m','s'):
            select(f'pair-{letter}-multiple-{other}', f'SELECT l.lid,r.rid FROM {relation.format("INNER")}(k,{other}) '
                   'ORDER BY l.lid,r.rid', [[str(l),str(r)] for l,r in second_key], ['lid','rid'], [23,23],
                   [[left_oid,1],[right_oid,1]])

    int_left, int_right = name('p_i_left'), name('p_i_right')
    create('pair-copy-table', 'TABLE', 'p_copy', '(id integer PRIMARY KEY,k integer[])')
    target = name('p_copy')
    q('pair-copy-duplicates', f'INSERT INTO {target} SELECT r.rid,k FROM {int_left} AS l '
      f'JOIN {int_right} AS r USING(k) WHERE l.lid=1', tag='INSERT 0 2')
    copy_oid = relation_oid('p_copy')
    select('pair-duplicates-persisted', f'SELECT id,k FROM {target} ORDER BY id',
           [['10','{-2147483648,NULL}'],['11','{-2147483648,NULL}']], ['id','k'], [23,1007],
           [[copy_oid,1],[copy_oid,2]])

    create('pair-demand-sequence', 'SEQUENCE', 'p_calls')
    create('pair-demand-source', 'TABLE', 'p_demand', '(rid integer PRIMARY KEY,raw text)')
    seed('pair-demand-seed', 'p_demand', [(1,'{2,NULL}'), (2,'late-invalid-array')])
    create('pair-demand-left', 'TABLE', 'p_one', '(k integer[])')
    seed('pair-demand-left-seed', 'p_one', [('{2,NULL}',)])
    create('pair-demand-empty', 'TABLE', 'p_empty', '(k integer[])')
    sequence = name('p_calls')
    child = f"SELECT rid,raw::integer[] AS k,nextval('{sequence}') AS tap FROM {name('p_demand')}"
    union = f'({child} WHERE rid=1 UNION ALL {child} WHERE rid=2) AS r'
    for kind, lhs in (('INNER','p_one'), ('RIGHT','p_empty')):
        label = f'pair-demand-{kind.lower()}'
        sql = f'SELECT r.rid,r.tap FROM {name(lhs)} AS l {kind} JOIN {union} USING(k)'
        number(f'{label}-reset', f"SELECT setval('{sequence}',100,true) AS calls", 100)
        number_sql = f"SELECT currval('{sequence}') AS calls"
        select(f'{label}-limit-zero', sql+' LIMIT 0', [], ['rid','tap'], [23,20], [[0,0],[0,0]])
        number(f'{label}-no-source', number_sql, 100)
        select(f'{label}-limit-one', sql+' LIMIT 1', [['1','101']], ['rid','tap'], [23,20], [[0,0],[0,0]])
        number(f'{label}-one-source', number_sql, 101)
        select(f'{label}-late-error', sql+' LIMIT 1 OFFSET 1', [], ['rid','tap'], [23,20],
               [[0,0],[0,0]], state='22P02')
        number(f'{label}-source-before-error', number_sql, 102)

    create('pair-correlated-table', 'TABLE', 'p_outer', '(id integer PRIMARY KEY,lid integer)')
    seed('pair-correlated-seed', 'p_outer', [(1,1),(2,4),(3,3),(4,2),(5,90)])
    correlated = (f'SELECT o.id+0 AS id,(SELECT r.rid FROM {int_left} AS l JOIN {int_right} AS r USING(k) '
                  f'WHERE l.lid=o.lid ORDER BY r.rid OFFSET 1 LIMIT 1) AS picked FROM {name("p_outer")} AS o ORDER BY o.id')
    for repeat in ('first','again'):
        select(f'pair-correlated-{repeat}', correlated,
               [['1','11'],['2',None],['3',None],['4','11'],['5',None]], ['id','picked'], [23,23], [[0,0],[0,0]])

    create('nonnull-domain-sequence', 'SEQUENCE', 'nn_calls')
    sequence = name('nn_calls')
    create('nonnull-domain', 'DOMAIN', 'nn_domain', f" AS integer[] NOT NULL CHECK(nextval('{sequence}')>0)")
    create('nonnull-other-domain', 'DOMAIN', 'nn_other', ' AS integer[]')
    domain_oid = identity('type-oid-nn_domain', 'pg_type', 'typname', 'nn_domain')
    other_oid = identity('type-oid-nn_other', 'pg_type', 'typname', 'nn_other')
    assert domain_oid != other_oid
    for symbol, domain, column_constraint in (
        ('nn_left','nn_domain',''), ('nn_right','nn_domain',''), ('nn_mixed','nn_other',''),
        ('nn_sink','nn_domain',''), ('nn_column','nn_domain',' NOT NULL'),
    ):
        create(f'table-{symbol}', 'TABLE', symbol, f'(id integer PRIMARY KEY,k {name(domain)}{column_constraint})')
    seed('nonnull-left-seed', 'nn_left', [(1,'{2}')])
    seed('nonnull-right-seed', 'nn_right', [(11,'{1}')])
    seed('nonnull-mixed-seed', 'nn_mixed', [(21,'{1}')])
    left = name('nn_left')
    same = f'{left} AS l LEFT JOIN {name("nn_right")} AS r USING(k)'
    unrelated = f'{left} AS l LEFT JOIN {name("nn_mixed")} AS r USING(k)'
    mixed = f'{int_left} AS l LEFT JOIN {name("nn_right")} AS r USING(k) WHERE l.lid=3'
    for label, source, expression, declared in (
        ('same',same,'r.k',domain_oid), ('unrelated',unrelated,'r.k',other_oid), ('mixed',mixed,'k','1007'),
    ):
        select(f'nonnull-{label}-null-type', f'SELECT {expression} AS v,pg_typeof({expression})::oid AS kt FROM {source}',
               [[None,declared]], ['v','kt'], [1007,26])
    number('nonnull-reset', f"SELECT setval('{sequence}',100,true) AS calls", 100)
    sink = name('nn_sink')
    q('nonnull-same-null-insert', f'INSERT INTO {sink} SELECT 1,r.k FROM {same}', tag='INSERT 0 1')
    for label, sql in (
        ('column', f'INSERT INTO {name("nn_column")} SELECT 9,r.k FROM {same}'),
        ('unrelated', f'INSERT INTO {sink} SELECT 9,r.k FROM {unrelated}'),
        ('mixed', f'INSERT INTO {sink} SELECT 9,k FROM {mixed}'),
        ('base-cast', f'INSERT INTO {sink} SELECT 9,r.k::integer[] FROM {same}'),
        ('fresh', f'INSERT INTO {sink} VALUES (9,NULL)'),
    ):
        q(f'nonnull-{label}-null-rejected', sql, tag=None, state='23502')
    sink_oid = relation_oid('nn_sink')
    select('nonnull-stored-null', f'SELECT id,k,pg_typeof(k)::oid AS kt FROM {sink} ORDER BY id',
           [['1',None,domain_oid]], ['id','k','kt'], [23,1007,26], [[sink_oid,1],[sink_oid,2],[0,0]])
    number('nonnull-domain-no-recheck', f"SELECT currval('{sequence}') AS calls", 100)


def array_lookup_workload(checks, prefix, objects):
    q = checks.query
    relations = {}

    for side, identifier in (('left', 'lid'), ('right', 'rid'), ('empty_right', 'rid')):
        symbol = f'lookup_{side}'
        table = f'{prefix}_{symbol}'
        objects.append(('TABLE', table))
        q(f'table-{symbol}', f'CREATE TABLE {table}({identifier} integer PRIMARY KEY,k bigint[],m integer[])',
          tag='CREATE TABLE', require_success=True)
        oid = q(f'relation-oid-{symbol}', f"SELECT oid FROM pg_class WHERE relname='{table}'",
                tag='SELECT 1', names=['oid'], oids=[26], capture_oid=True, wire_layout=[[4, -1]])
        relations[side] = table, int(oid)

    # These distinct bigint[] values have the same integer-array fingerprint.
    equal_key = '{11,22}'
    other_key = '{12,6137473909959017}'
    missing_key = '{13,7094049026230848}'
    left_rows = {1: (missing_key, '{1}'), 2: (equal_key, '{1}'), 3: (equal_key, '{2}'),
                 4: (other_key, '{1}'), 5: (None, '{1}')}
    right_rows = {10: (other_key, '{1}'), 11: (equal_key, '{1}'), 12: ('{99,100}', '{1}'),
                  13: (other_key, '{2}'), 14: (equal_key, '{2}'), 15: (equal_key, '{1}'), 16: (None, '{1}')}
    for side, rows in (('left', left_rows), ('right', right_rows)):
        table, _ = relations[side]
        entries = []
        for identifier, (key, second) in rows.items():
            literal = 'NULL' if key is None else f"'{key}'"
            entries.append(f"({identifier},{literal},'{second}')")
        values = ','.join(entries)
        q(f'lookup-{side}-data', f'INSERT INTO {table} VALUES {values}', tag=f'INSERT 0 {len(rows)}',
          require_success=True)

    def selected(label, kind, pairs, key_list='k', right='right', tail=''):
        left_table, left_oid = relations['left']
        right_table, right_oid = relations[right]
        sql = (f'SELECT l.lid,r.rid,k FROM {left_table} AS l {kind} JOIN {right_table} AS r USING({key_list}) '
               f'ORDER BY l.lid NULLS LAST,r.rid DESC NULLS LAST{tail}')
        rows = [[None if lid is None else str(lid), None if rid is None else str(rid),
                 right_rows[rid][0] if lid is None else left_rows[lid][0]] for lid,rid in pairs]
        q(label, sql, tag=f'SELECT {len(rows)}', rows=rows, names=['lid','rid','k'], oids=[23,23,1016],
          wire_layout=[[4, -1], [4, -1], [-1, -1]])
        origins = [[left_oid,1], [right_oid,1], [0,0] if kind=='FULL' else [left_oid,2]]
        record = checks.records[-1]
        record['expected']['origins'] = origins
        if [field[1:3] for field in record['fields']] != origins:
            record['pass'] = False
            print('ARRAY_DOMAIN_JOIN_ORIGIN_DIFFERENCE', json.dumps(record), flush=True)

    matches = [(2,15),(2,14),(2,11),(3,15),(3,14),(3,11),(4,13),(4,10)]
    full = [(1,None), *matches, (5,None), (None,16), (None,12)]
    selected('lookup-collision-inner','INNER',matches)
    selected('lookup-collision-full','FULL',full)
    selected('lookup-collision-multiple-keys','INNER',[(2,15),(2,11),(3,14),(4,10)],key_list='k,m')
    selected('lookup-order-subset','INNER',matches[3:7],tail=' OFFSET 3 LIMIT 4')
    q('lookup-low-work-mem', "SET work_mem='64kB'", tag='SET')
    selected('lookup-low-memory-full','FULL',full)
    q('lookup-reset-work-mem', 'RESET work_mem', tag='RESET')
    selected('lookup-empty-right-inner','INNER',[],right='empty_right')
    unmatched = [(identifier,None) for identifier in left_rows]
    selected('lookup-empty-right-left','LEFT',unmatched,right='empty_right')
    selected('lookup-empty-right-full','FULL',unmatched,right='empty_right')


def nested_insert_workload(checks, prefix, objects):
    q = checks.query

    def name(symbol):
        return f'{prefix}_{symbol}'

    def create(kind, symbol, definition=''):
        identifier = name(symbol)
        objects.append((kind, identifier))
        q(f'nested-create-{symbol}', f'CREATE {kind} {identifier}{definition}',
          tag=f'CREATE {kind}', require_success=True)

    def value(label, expression, n):
        q(label, f'SELECT {expression} AS calls', tag='SELECT 1', names=['calls'],
          rows=[[str(n)]], oids=[20], wire_layout=[[8, -1]])

    create('SEQUENCE', 'nest_domain_tap')
    create('SEQUENCE', 'nest_on_tap')
    domain_tap = name('nest_domain_tap')
    on_tap = name('nest_on_tap')
    left, same, other, sink, inputs = [name(f'nest_{symbol}') for symbol in
                                       ('left', 'same', 'other', 'sink', 'inputs')]
    for symbol in ('nest_d', 'nest_e'):
        create('DOMAIN', symbol, f" AS integer[] CHECK(nextval('{domain_tap}')>0)")
    for symbol, id_name, domain in (
        ('nest_left', 'lid', 'nest_d'), ('nest_same', 'rid', 'nest_d'),
        ('nest_other', 'rid', 'nest_e'), ('nest_sink', 'id', 'nest_d')
    ):
        create('TABLE', symbol, f'({id_name} integer PRIMARY KEY,k {name(domain)})')
    create('TABLE', 'nest_inputs', '(rid integer PRIMARY KEY, raw text)')
    for symbol, rows, count in (
        ('nest_left', "(1,'{2}'),(2,'{10}')", 2),
        ('nest_same', "(10,'{2}'),(30,'[0:1]={3,NULL}')", 2),
        ('nest_other', "(10,'{2}'),(30,'[0:1]={3,NULL}')", 2),
        ('nest_inputs', "(10,'invalid'),(20,'invalid'),(30,'invalid'),(40,'invalid')", 4),
    ):
        q(f'nested-seed-{symbol}', f'INSERT INTO {name(symbol)} VALUES {rows}',
          tag=f'INSERT 0 {count}', require_success=True)

    def current_domain(label, calls):
        value(label, f"currval('{domain_tap}')", calls)

    value('nested-domain-reset', f"setval('{domain_tap}',1000,true)", 1000)
    for label, right, wrappers, identifier, calls in (
        ('same-full', same, False, 1, 1000),
        ('mixed-full', other, False, 2, 1001),
        ('same-full-two-wrappers', same, True, 3, 1001),
        ('mixed-full-two-wrappers', other, True, 4, 1002),
    ):
        join = f'SELECT k FROM {left} AS l FULL JOIN {right} AS r USING(k) WHERE r.rid=30'
        source = f'(SELECT v.k FROM ({join} OFFSET 0) AS v) AS s' if wrappers else f'({join}) AS s'
        q(f'nested-insert-{label}', f'INSERT INTO {sink} SELECT {identifier},s.k FROM {source}',
          tag='INSERT 0 1')
        current_domain(f'nested-domain-{label}', calls)

    value('nested-on-reset', f"setval('{on_tap}',2000,true)", 2000)
    right_input = f'(SELECT rid,raw FROM {inputs} ORDER BY rid OFFSET 0) AS r'
    predicate = f"(CASE WHEN r.rid>0 THEN nextval('{on_tap}')>0 ELSE false END)"
    source = f'FROM {left} AS l LEFT JOIN {right_input} ON {predicate} WHERE l.lid=1'

    def insert(label, identifier, source, tail, count=None, state=None):
        q(label, f'INSERT INTO {sink} SELECT {identifier},l.k {source}{tail}',
          tag=None if state else f'INSERT 0 {count}', state=state)

    insert('nested-on-zero', 5, source, ' LIMIT 0', 0)
    value('nested-on-zero-calls', f"currval('{on_tap}')", 2000)
    insert('nested-on-one', 5, source, ' LIMIT 1', 1)
    value('nested-on-one-calls', f"currval('{on_tap}')", 2001)
    insert('nested-on-offset', 6, source, ' OFFSET 2 LIMIT 1', 1)
    value('nested-on-offset-calls', f"currval('{on_tap}')", 2004)
    # Every pair can supply the same output. The ON expression accepts the first
    # attempt and fails on the second independently of the join's choice of rows.
    for count, label, identifier, start, calls, state in (
        (1, 'one', 7, 3000, 3001, None), (2, 'two', 8, 4000, 4002, '22P02'),
    ):
        value(f'nested-on-late-error-{label}-reset', f"setval('{on_tap}',{start},true)", start)
        predicate = f"(CASE WHEN nextval('{on_tap}')={start + 1} THEN true ELSE r.raw::integer[]=l.k END)"
        source = f'FROM {left} AS l LEFT JOIN {right_input} ON {predicate} WHERE l.lid=1'
        insert(f'nested-on-late-error-{label}', identifier, source, f' LIMIT {count}', count, state)
        value(f'nested-on-late-error-{label}-calls', f"currval('{on_tap}')", calls)
    current_domain('nested-domain-final', 1002)
    expected = [[str(i), '[0:1]={3,NULL}' if i < 5 else '{2}'] for i in range(1, 8)]
    q('nested-insert-stored', f'SELECT id,k FROM {sink} ORDER BY id', tag='SELECT 7',
      rows=expected, names=['id', 'k'], oids=[23, 1007], wire_layout=[[4, -1], [-1, -1]])

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
    prefix = 'using_array_' + uuid.uuid4().hex[:12]
    checks = Checks(client, evidence)
    objects = []
    with tempfile.TemporaryDirectory(prefix='dbms-using-array-domain-') as directory:
        server = None if reference else ProjectServer(client, Path(directory))
        complete = stopped = False
        try:
            checks.sock = reference_connection(client) if reference else server.start()
            workload(checks, prefix, objects)
            array_pair_workload(checks, prefix, objects)
            array_lookup_workload(checks, prefix, objects)
            nested_insert_workload(checks, prefix, objects)
            failures = [record['label'] for record in checks.records if not record['pass']]
            assert not failures, failures
            complete = True
        finally:
            try:
                if server is not None:
                    server.stop()
                elif checks.sock is not None:
                    try:
                        for kind, identifier in reversed(objects):
                            client.simple_query(checks.sock, f'DROP {kind} IF EXISTS {identifier}')
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
                        'test_sha256': sha(Path(__file__)), 'checks': len(checks.records),
                        'passes': sum(record['pass'] for record in checks.records),
                        'stops': server.stops if server is not None else [], 'records': checks.records,
                    }
                    (evidence/'summary.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print('ARRAY_DOMAIN_JOIN_USING_PROTOCOL_PASS', len(checks.records))


if __name__ == '__main__':
    main()
