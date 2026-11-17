#!/usr/bin/env python3
"""Strict read-only output and identity through parentheses, tables and views."""
from compat.regtype_projection_protocol import run


def steps(prefix, *, include_view_write=False):
    table, saved, view, quoted, computed, domain = (
        prefix + suffix for suffix in ('_rows', '_saved', '_view', '_quoted', '_computed', '_ref'))
    commands = []

    def add(label, sql, rows=None, oids=None, setup=False, headers=None, query=False):
        command = dict(label=label, sql=sql, rows=rows, oids=oids, state=None,
                       ready='I', setup=setup, headers=headers, unordered=False, capture=None)
        if query:
            command['statement_kind'] = 'SELECT'
        commands.append(command)

    values = [['1','integer','[0:2]={integer,NULL,-}','text'],
              ['2','text','{}','-'], ['3','-',None,None],
              ['4',None,'{text}','integer'],
              ['5','4294967295','{4294967295,NULL}','4294967295']]
    reference_oids = ['23','25','0',None,'4294967295']
    domain_oids = ['25','0',None,'23','4294967295']
    types = [23,2206,2211,2206]
    casts = [[reference_oids[i],row[2],domain_oids[i]] for i,row in enumerate(values)]
    chosen = [[row[0],row[1] if row[1] is not None else 'integer',row[2],row[3]] for row in values]
    chosen_oids = [[reference_oids[i] if reference_oids[i] is not None else '23',row[2]]
                   for i,row in enumerate(values)]
    add('domain', 'CREATE DOMAIN ' + domain + ' AS regtype', setup=True)
    add('table', 'CREATE TABLE ' + table + '(id integer PRIMARY KEY,r regtype,ra regtype[],d ' + domain + ')', setup=True)
    add('data', 'INSERT INTO ' + table + " VALUES"
        "(1,'integer','[0:2]={integer,NULL,-}','text'),"
        "(2,'text','{}','-'),(3,'-',NULL,NULL),"
        "(4,NULL,'{text}','integer'),"
        "(5,'4294967295','{4294967295,NULL}','4294967295')", setup=True)
    add('view', 'CREATE VIEW ' + view + ' AS SELECT id,r,ra,d FROM ' + table, setup=True)
    add('quoted-view', 'CREATE VIEW ' + quoted + '("Identifier","R","Array","D") AS SELECT id,r,ra,d FROM ' + table, setup=True)
    add('computed-view', 'CREATE VIEW ' + computed +
        " AS SELECT id,coalesce(r,'integer'::regtype) AS chosen,ra,d FROM " + table, setup=True)

    body = 'SELECT id,r,ra,d FROM ' + table + ' ORDER BY id'
    add('parenthesized-direct', '(' + body + ')', values, types,
        headers=['id','r','ra','d'], query=True)
    add('double-parenthesized-direct', '((' + body + '))', values, types, query=True)
    add('parenthesized-cast-alias', '(SELECT r::oid AS r,ra::text AS ra,d::text AS d FROM ' + table + ' ORDER BY id)',
        [[reference_oids[i],row[2],row[3]] for i,row in enumerate(values)], [26,25,25],
        headers=['r','ra','d'], query=True)
    add('parenthesized-whole-null', '(SELECT r FROM ' + table + ' WHERE id=4)', [[None]], [2206], query=True)
    add('parenthesized-array-whole-null', '(SELECT ra FROM ' + table + ' WHERE id=3)', [[None]], [2211], query=True)
    add('parenthesized-empty', '(SELECT r,ra,d FROM ' + table + ' WHERE id<0)', [], [2206,2211,2206], query=True)
    derived = '(SELECT id,r,ra,d FROM ' + table + ') x'
    add('derived-reference', 'SELECT x.id,x.r,x.ra,x.d FROM ' + derived + ' ORDER BY x.id', values, types)
    add('derived-cast-alias', 'SELECT x.r::oid AS r,x.ra::text AS ra,x.d::oid AS d FROM ' + derived + ' ORDER BY x.id',
        casts, [26,25,26], headers=['r','ra','d'])
    add('derived-predicate', 'SELECT x.id,x.r,x.ra FROM ' + derived + " WHERE x.r='integer'::regtype ORDER BY x.id",
        [['1','integer',values[0][2]]], [23,2206,2211])

    add('view-direct', 'SELECT id,r,ra,d FROM ' + view + ' ORDER BY id', values, types)
    add('view-wildcard', 'SELECT * FROM ' + view + ' ORDER BY id', values, types)
    add('view-cast-alias', 'SELECT r::oid AS r,ra::text AS ra,d::oid AS d FROM ' + view + ' ORDER BY id',
        casts, [26,25,26], headers=['r','ra','d'])
    add('view-mixed-roles', 'SELECT r,r::oid AS r_oid,ra,ra::text AS ra_text,d,d::oid AS d_oid FROM ' + view + ' ORDER BY id',
        [[row[1],reference_oids[i],row[2],row[2],row[3],domain_oids[i]] for i,row in enumerate(values)],
        [2206,26,2211,25,2206,26])
    add('view-zero-identity', 'SELECT id,r,ra,d FROM ' + view + ' WHERE r::oid=0 ORDER BY id', [values[2]], types)
    add('view-reference-order', 'SELECT id,r FROM ' + view + ' ORDER BY r',
        [[values[i][0],values[i][1]] for i in (2,0,1,4,3)], [23,2206])
    add('view-limit-offset', 'SELECT id,r,ra,d FROM ' + view + ' ORDER BY id LIMIT 2 OFFSET 2', values[2:4], types)
    add('view-whole-null', 'SELECT r FROM ' + view + ' WHERE id=4', [[None]], [2206])
    add('view-empty', 'SELECT r,ra,d FROM ' + view + ' WHERE id<0', [], [2206,2211,2206])
    add('parenthesized-view', '(SELECT id,r,ra,d FROM ' + view + ' ORDER BY id)', values, types, query=True)
    add('double-parenthesized-view', '((SELECT r,ra,d FROM ' + view + ' ORDER BY id))',
        [row[1:] for row in values], [2206,2211,2206], query=True)
    add('quoted-view-direct', 'SELECT "Identifier","R","Array","D" FROM ' + quoted + ' ORDER BY "Identifier"',
        values, types, headers=['Identifier','R','Array','D'])
    add('quoted-view-cast-alias', 'SELECT "R"::oid AS "R","Array"::text AS "Array","D"::oid AS "D" FROM ' + quoted +
        ' ORDER BY "Identifier"', casts, [26,25,26], headers=['R','Array','D'])
    add('computed-view-display', 'SELECT id,chosen,ra,d FROM ' + computed + ' ORDER BY id', chosen, types)
    add('computed-view-identity', 'SELECT chosen::oid AS chosen,ra FROM ' + computed + ' ORDER BY id',
        chosen_oids, [26,2211])
    if include_view_write:
        # This complete group depends on writing view results into storage.
        add('saved-table', 'CREATE TABLE ' + saved + '(id integer PRIMARY KEY,r regtype,ra regtype[],d ' + domain + ')', setup=True)
        add('insert-select', 'INSERT INTO ' + saved + ' SELECT id,r,ra,d FROM ' + view, setup=True)
        add('stored-view-values', 'SELECT id,r,ra,d FROM ' + saved + ' ORDER BY id', values, types)
        add('stored-view-identities', 'SELECT r::oid AS r,ra::text AS ra,d::oid AS d FROM ' + saved + ' ORDER BY id',
            casts, [26,25,26])
    commands.append({'restart': True})
    add('cold-view', 'SELECT id,r,ra,d FROM ' + view + ' ORDER BY id', values, types)
    add('cold-quoted-view', 'SELECT "Identifier","R","Array","D" FROM ' + quoted + ' ORDER BY "Identifier"', values, types)
    add('cold-computed-view', 'SELECT chosen::oid AS chosen,ra FROM ' + computed + ' ORDER BY id', chosen_oids, [26,2211])
    add('cold-parenthesized-view', '(SELECT id,r,ra,d FROM ' + view + ' ORDER BY id)', values, types, query=True)
    add('cold-derived-view', 'SELECT x.id,x.r,x.ra,x.d FROM (SELECT id,r,ra,d FROM ' + view + ') x ORDER BY x.id', values, types)
    if include_view_write:
        add('cold-stored-view', 'SELECT id,r,ra,d FROM ' + saved + ' ORDER BY id', values, types)
    cleanup = ['DROP VIEW IF EXISTS ' + computed, 'DROP VIEW IF EXISTS ' + quoted,
               'DROP VIEW IF EXISTS ' + view]
    if include_view_write:
        cleanup.append('DROP TABLE IF EXISTS ' + saved)
    cleanup.extend(['DROP TABLE IF EXISTS ' + table, 'DROP DOMAIN IF EXISTS ' + domain])
    return commands, [], cleanup


def main():
    run(steps, entry_file=__file__, cohort='regtype_materialization_read35', expected_checks=35)


if __name__ == '__main__':
    main()
