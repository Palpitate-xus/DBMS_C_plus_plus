#!/usr/bin/env python3
"""Ordinary REGCLASS output uses real local relation identities and names."""
from compat.regtype_projection_protocol import run


def steps(prefix):
    referent = prefix + '_object'
    schema = prefix + '_objects'
    custom = schema + '."Mixed Table"'
    domain = prefix + '_class_ref'
    table, saved, defaults = (prefix + suffix for suffix in ('_classes', '_saved_classes', '_default_classes'))
    out = []

    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    def array(values, lower=1):
        def token(value):
            if value is None:
                return 'NULL'
            if not value or value.upper() == 'NULL' or any(c.isspace() or c in '{}",\\' for c in value):
                return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
            return value
        body = '{' + ','.join(token(value) for value in values) + '}'
        return ('[%d:%d]=' % (lower, lower + len(values) - 1) if values and lower != 1 else '') + body

    def add(label, sql, rows=None, oids=None, ready='I', setup=False,
            headers=None, unordered=False, capture=None):
        out.append(dict(label=label, sql=sql, rows=rows, oids=oids, state=None,
                        ready=ready, setup=setup, headers=headers, unordered=unordered, capture=capture))

    add('referent-table', 'CREATE TABLE ' + referent + '(id integer)', setup=True)
    add('referent-schema', 'CREATE SCHEMA ' + schema, setup=True)
    add('quoted-referent', 'CREATE TABLE ' + custom + '(id integer)', setup=True)
    add('domain', 'CREATE DOMAIN ' + domain + ' AS regclass', setup=True)
    add('table', 'CREATE TABLE ' + table + '(id integer PRIMARY KEY,c regclass,ca regclass[],d ' + domain + ',t text)', setup=True)
    # Each identity capture executes actual SQL input/casting. The expected
    # display names below remain independent of these locally allocated OIDs.
    add('capture-public-oid', 'SELECT ' + literal(referent) + '::regclass::oid',
        oids=[26], capture='public', setup=True)
    add('capture-custom-oid', 'SELECT ' + literal(custom) + '::regclass::oid',
        oids=[26], capture='custom', setup=True)
    add('data', 'INSERT INTO ' + table + ' VALUES'
        '(1,' + literal(referent) + ',' + literal(array([referent,None,'-'],0)) + ',' + literal(custom) + ",'initial'),"
        '(2,' + literal(custom) + ',ARRAY[' + literal(custom) + "::regclass,NULL::regclass],'-',''),"
        "(3,'-',NULL,NULL,'NULL'),"
        '(4,NULL,' + literal(array([referent])) + ',' + literal(referent) + ',NULL),'
        "(5,'4294967295','{4294967295,NULL}','4294967295','unknown')", setup=True)
    values = [
        ['1',referent,array([referent,None,'-'],0),custom,'initial'],
        ['2',custom,array([custom,None]),'-',''],
        ['3','-',None,None,'NULL'],
        ['4',None,array([referent]),referent,None],
        ['5','4294967295','{4294967295,NULL}','4294967295','unknown']]
    scalar = [[row[1]] for row in values]
    arrays = [[row[2]] for row in values]
    domains = [[row[3]] for row in values]
    source, ordered = ' FROM ' + table, ' FROM ' + table + ' ORDER BY id'
    add('direct-scalar', 'SELECT c' + ordered, scalar, [2205], headers=['c'])
    add('direct-array', 'SELECT ca' + ordered, arrays, [2210], headers=['ca'])
    add('direct-domain', 'SELECT d' + ordered, domains, [2205], headers=['d'])
    add('full-wildcard', 'SELECT *' + ordered, values, [23,2205,2210,2205,25], headers=['id','c','ca','d','t'])
    add('explicit-all', 'SELECT id,c,ca,d,t' + ordered, values, [23,2205,2210,2205,25])
    add('reordered-subset', 'SELECT d,c,id,ca' + ordered,
        [[r[3],r[1],r[0],r[2]] for r in values], [2205,2205,23,2210])
    add('qualified-scalar', 'SELECT x.c FROM ' + table + ' AS x ORDER BY x.id', scalar, [2205], headers=['c'])
    add('aliased-scalar', 'SELECT c AS chosen' + ordered, scalar, [2205], headers=['chosen'])
    add('repeated-projection', 'SELECT c AS first_reference,c AS second_reference,ca AS first_array,ca AS second_array,d' + ordered,
        [[r[1],r[1],r[2],r[2],r[3]] for r in values], [2205,2205,2210,2210,2205],
        headers=['first_reference','second_reference','first_array','second_array','d'])
    add('cast-control', 'SELECT c::text AS c,ca::text AS ca,d::text AS d' + ordered,
        [[r[1],r[2],r[3]] for r in values], [25,25,25], headers=['c','ca','d'])
    add('source-object-oid', 'SELECT id,c::oid AS object_oid' + ordered,
        lambda symbols: [['1',symbols['public']],['2',symbols['custom']],['3','0'],['4',None],['5','4294967295']],
        [23,26], headers=['id','object_oid'])
    add('direct-zero', 'SELECT c' + source + ' WHERE id=3', [['-']], [2205])
    add('direct-null', 'SELECT c' + source + ' WHERE id=4', [[None]], [2205])
    add('direct-unknown', 'SELECT c' + source + ' WHERE id=5', [['4294967295']], [2205])
    add('text-is-not-reference', 'SELECT t' + ordered, [[r[4]] for r in values], [25], headers=['t'])
    add('empty-projection', 'SELECT c,ca,d' + source + ' WHERE id<0', [], [2205,2210,2205])
    add('unordered-scalar', 'SELECT id,c' + source, [[r[0],r[1]] for r in values], [23,2205], unordered=True)
    add('limited-subset', 'SELECT c' + ordered + ' LIMIT 2 OFFSET 1', [[custom],['-']], [2205])
    add('reference-filter', 'SELECT c' + source + ' WHERE c=' + literal(referent) + '::regclass', [[referent]], [2205])
    add('computed-reference-array', 'SELECT ARRAY[c,NULL::regclass] AS refs,coalesce(c,' + literal(referent) + '::regclass) AS chosen' + ordered,
        [[array([r[1],None]),r[1] if r[1] is not None else referent] for r in values], [2210,2205], headers=['refs','chosen'])
    add('same-type-casts', 'SELECT c::regclass AS c,ca::regclass[] AS ca,d::regclass AS d' + ordered,
        [[r[1],r[2],r[3]] for r in values], [2205,2210,2205])
    add('cte-direct', 'WITH x AS (SELECT id,c,ca,d FROM ' + table + ') SELECT c,ca,d FROM x ORDER BY id',
        [[r[1],r[2],r[3]] for r in values], [2205,2210,2205])
    add('cte-computed-array', 'WITH x AS (SELECT id,coalesce(c,' + literal(referent) + '::regclass) AS chosen,'
        'ARRAY[c,NULL::regclass] AS refs FROM ' + table + ') SELECT chosen,refs FROM x ORDER BY id',
        [[r[1] if r[1] is not None else referent,array([r[1],None])] for r in values], [2205,2210])
    add('cte-cast-control', 'WITH x AS (SELECT id,c::text AS c,ca::text AS ca,d::text AS d FROM ' + table +
        ') SELECT c,ca,d FROM x ORDER BY id', [[r[1],r[2],r[3]] for r in values], [25,25,25])
    add('joined-references', 'SELECT a.id,a.c,b.ca,a.d,b.d FROM ' + table + ' a JOIN ' + table +
        ' b ON a.id=b.id ORDER BY a.id', [[r[0],r[1],r[2],r[3],r[3]] for r in values], [23,2205,2210,2205,2205])
    add('outer-null-reference', 'SELECT a.id,a.c,b.ca,b.d FROM ' + table + ' a LEFT JOIN ' + table +
        ' b ON b.id=0 ORDER BY a.id', [[r[0],r[1],None,None] for r in values], [23,2205,2210,2205])
    add('join-cast-control', 'WITH x AS (SELECT id,c::text AS c,ca::text AS ca,d::text AS d FROM ' + table +
        ') SELECT a.id,a.c,b.ca,a.d,b.d FROM x a JOIN x b ON a.id=b.id ORDER BY a.id',
        [[r[0],r[1],r[2],r[3],r[3]] for r in values], [23,25,25,25,25])
    add('three-table-reference', 'SELECT a.id,a.c,b.ca,c.d FROM ' + table + ' a JOIN ' + table +
        ' b ON a.id=b.id JOIN ' + table + ' c ON b.id=c.id ORDER BY a.id',
        [[r[0],r[1],r[2],r[3]] for r in values], [23,2205,2210,2205])
    add('using-reference', 'SELECT id,a.c,b.ca,b.d FROM ' + table + ' a JOIN ' + table +
        ' b USING(id) ORDER BY id', [[r[0],r[1],r[2],r[3]] for r in values], [23,2205,2210,2205])
    add('visible-path', 'SET search_path=' + schema + ',pg_catalog,public')
    visible = [[r[0], '"Mixed Table"' if r[1] == custom else r[1],
                array(['"Mixed Table"',None]) if r[0] == '2' else r[2],
                '"Mixed Table"' if r[3] == custom else r[3], r[4]] for r in values]
    add('visible-reference-output', 'SELECT id,c,ca,d,t FROM public.' + table + ' ORDER BY id',
        visible, [23,2205,2210,2205,25])
    add('visible-cast-control', 'SELECT c::text AS c,ca::text AS ca,d::text AS d FROM public.' + table + ' ORDER BY id',
        [[r[1],r[2],r[3]] for r in visible], [25,25,25])
    add('restore-path', 'SET search_path=public,pg_catalog')
    add('begin', 'BEGIN', ready='T')
    add('outer-update', 'UPDATE ' + table + ' SET c=' + literal(custom) + ",ca='{}',d='-',t='outer' WHERE id=1", ready='T')
    add('outer-control', 'SELECT c::text AS c,ca::text AS ca,d::text AS d,t' + source + ' WHERE id=1',
        [[custom,'{}','-','outer']], [25,25,25,25], ready='T')
    add('savepoint', 'SAVEPOINT class_projection', ready='T')
    add('inner-update', 'UPDATE ' + table + " SET c='-',ca=NULL,d=" + literal(referent) + ",t='inner' WHERE id=1", ready='T')
    add('pending-reference', 'SELECT c,ca,d,t' + source + ' WHERE id=1',
        [['-',None,referent,'inner']], [2205,2210,2205,25], ready='T')
    add('rollback-savepoint', 'ROLLBACK TO SAVEPOINT class_projection', ready='T')
    add('outer-restored', 'SELECT c,ca,d,t' + source + ' WHERE id=1',
        [[custom,'{}','-','outer']], [2205,2210,2205,25], ready='T')
    add('rollback', 'ROLLBACK')
    add('base-restored', 'SELECT id,c,ca,d,t' + ordered, values, [23,2205,2210,2205,25])
    add('saved-table', 'CREATE TABLE ' + saved + '(id integer PRIMARY KEY,c regclass,ca regclass[],d ' + domain + ')', setup=True)
    add('insert-select', 'INSERT INTO ' + saved + ' SELECT id,c,ca,d' + source, setup=True)
    saved_rows = [r[:4] for r in values]
    add('saved-values', 'SELECT id,c,ca,d FROM ' + saved + ' ORDER BY id', saved_rows, [23,2205,2210,2205])
    add('default-table', 'CREATE TABLE ' + defaults + '(id integer,c regclass DEFAULT ' + literal(referent) +
        ',ca regclass[] DEFAULT ARRAY[' + literal(referent) + '::regclass,NULL::regclass])', setup=True)
    add('default-insert', 'INSERT INTO ' + defaults + '(id) VALUES(1)', setup=True)
    add('default-values', 'SELECT id,c,ca FROM ' + defaults, [['1',referent,array([referent,None])]], [23,2205,2210])
    out.append({'restart': True})
    add('cold-all-values', 'SELECT id,c,ca,d,t' + ordered, values, [23,2205,2210,2205,25])
    add('cold-source-object-oid', 'SELECT id,c::oid AS object_oid' + ordered,
        lambda symbols: [['1',symbols['public']],['2',symbols['custom']],['3','0'],['4',None],['5','4294967295']], [23,26])
    add('cold-cast-control', 'SELECT c::text AS c,ca::text AS ca,d::text AS d' + ordered,
        [[r[1],r[2],r[3]] for r in values], [25,25,25])
    add('cold-saved-values', 'SELECT id,c,ca,d FROM ' + saved + ' ORDER BY id', saved_rows, [23,2205,2210,2205])
    add('cold-default-values', 'SELECT id,c,ca FROM ' + defaults, [['1',referent,array([referent,None])]], [23,2205,2210])
    return out, [referent,custom,table,saved,defaults], ['DROP DOMAIN IF EXISTS ' + domain,
                                                      'DROP SCHEMA IF EXISTS ' + schema + ' CASCADE']


def main():
    run(steps, entry_file=__file__, cohort='regclass-direct-projection', expected_checks=62)


if __name__ == '__main__':
    main()
