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
