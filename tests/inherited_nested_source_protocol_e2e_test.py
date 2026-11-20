#!/usr/bin/env python3
"""Nested and joined sources retain inherited tables and each ONLY boundary.

The optional --reference18 mode requires an explicitly identified private
PostgreSQL 18.6 endpoint. Project mode owns its data and performs a cold
restart. SimpleQuery results include names, type OIDs, tags and transaction
state as well as rows.
"""
from compat.regtype_projection_protocol import run

def steps(prefix):
    parent, first, second, diamond, reordered, lookup = (
        prefix + suffix for suffix in ('_root', '_first', '_second', '_diamond', '_reordered', '_lookup'))
    commands = []

    def add(label, sql, rows=None, oids=None, headers=None, setup=False, kind=None, state=None):
        item = dict(label=label, sql=sql, rows=rows, oids=oids, state=state,
                    ready='I', setup=setup, headers=headers, unordered=False, capture=None)
        if kind:
            item['statement_kind'] = kind
        commands.append(item)

    add('parent', 'CREATE TABLE ' + parent + '(id integer,t text)', setup=True)
    add('first', 'CREATE TABLE ' + first + '(extra_a text) INHERITS (' + parent + ')', setup=True)
    add('second', 'CREATE TABLE ' + second + '(extra_b text) INHERITS (' + parent + ')', setup=True)
    add('diamond', 'CREATE TABLE ' + diamond + '(extra_d text) INHERITS (' + first + ',' + second + ')', setup=True)
    add('reordered', 'CREATE TABLE ' + reordered + '(t text,id integer,extra_r text)', setup=True)
    add('attach', 'ALTER TABLE ' + reordered + ' INHERIT ' + parent, setup=True, kind='ALTER TABLE')
    add('lookup', 'CREATE TABLE ' + lookup + '(id integer,label text)', setup=True)
    for label, table, columns, value in (
        ('parent', parent, 'id,t', "1,'parent'"),
        ('first', first, 'id,t,extra_a', "2,NULL,'a'"),
        ('second', second, 'id,t,extra_b', "3,'child','b'"),
        ('diamond', diamond, 'id,t,extra_a,extra_b,extra_d', "4,'diamond','a','b','d'"),
        ('reordered', reordered, 't,id,extra_r', "'reordered',5,'r'"),
    ):
        add('insert-' + label, 'INSERT INTO ' + table + '(' + columns + ') VALUES (' + value + ')', setup=True)
    for identifier in range(1,7):
        add('lookup-' + str(identifier), 'INSERT INTO ' + lookup + " VALUES (" + str(identifier) + ",'L" + str(identifier) + "')", setup=True)

    values = [['1','parent'], ['2',None], ['3','child'], ['4','diamond'], ['5','reordered']]
    upper = [['1','PARENT'], ['2',None], ['3','CHILD'], ['4','DIAMOND'], ['5','REORDERED']]
    joined = [row + ['L' + row[0]] for row in values]
    outer = values + [['6',None]]
    outer_only = [['1','parent'], ['2',None], ['3',None], ['4',None], ['5',None], ['6',None]]
    upper_outer = upper + [['6',None]]
    only = [values[0]]
    oids, names = [23,25], ['id','t']
    add('flat-control', 'SELECT id,t FROM ' + parent + ' ORDER BY id', values, oids, names)
    add('derived-plain', 'SELECT d.id,d.t FROM (SELECT id,t FROM ' + parent + ') AS d ORDER BY d.id', values, oids, names)
    add('derived-scalar', 'SELECT d.id,upper(d.t) AS chosen FROM (SELECT id,t FROM ' + parent + ') AS d ORDER BY d.id', upper, oids, ['id','chosen'])
    add('derived-inner-scalar', 'SELECT d.id,d.chosen FROM (SELECT id,upper(t) AS chosen FROM ' + parent + ') AS d ORDER BY d.id', upper, oids, ['id','chosen'])
    add('derived-only-bare', 'SELECT d.id,d.t FROM (SELECT id,t FROM ONLY ' + parent + ') AS d ORDER BY d.id', only, oids, names)
    add('derived-only-parens', 'SELECT d.id,d.t FROM (SELECT id,t FROM ONLY (' + parent + ')) AS d ORDER BY d.id', only, oids, names)
    add('cte-plain', 'WITH d AS (SELECT id,t FROM ' + parent + ') SELECT d.id,d.t FROM d ORDER BY d.id', values, oids, names)
    add('cte-scalar', 'WITH d AS (SELECT id,upper(t) AS chosen FROM ' + parent + ') SELECT d.id,d.chosen FROM d ORDER BY d.id', upper, oids, ['id','chosen'])
    add('cte-only', 'WITH d AS (SELECT id,t FROM ONLY ' + parent + ') SELECT d.id,d.t FROM d ORDER BY d.id', only, oids, names)
    add('inner-join', 'SELECT p.id,p.t,l.label FROM ' + parent + ' AS p JOIN ' + lookup + ' AS l ON p.id=l.id ORDER BY p.id', joined, [23,25,25], ['id','t','label'])
    add('inner-join-only', 'SELECT p.id,p.t,l.label FROM ONLY ' + parent + ' AS p JOIN ' + lookup + ' AS l ON p.id=l.id ORDER BY p.id', joined[:1], [23,25,25], ['id','t','label'])
    add('left-join', 'SELECT l.id,p.t FROM ' + lookup + ' AS l LEFT JOIN ' + parent + ' AS p ON l.id=p.id ORDER BY l.id', outer, oids, names)
    add('right-join', 'SELECT l.id,p.t FROM ' + parent + ' AS p RIGHT JOIN ' + lookup + ' AS l ON l.id=p.id ORDER BY l.id', outer, oids, names)
    add('full-join', 'SELECT l.id,p.t FROM ' + lookup + ' AS l FULL JOIN ' + parent + ' AS p ON l.id=p.id ORDER BY l.id', outer, oids, names)
    add('full-equality-conjunct', 'SELECT p.id,l.id FROM ' + parent + ' p FULL JOIN ' + lookup +
        ' l ON p.id=l.id AND p.id<5 ORDER BY p.id,l.id',
        [['1','1'], ['2','2'], ['3','3'], ['4','4'], ['5',None], [None,'5'], [None,'6']],
        [23,23], ['id','id'])
    add('full-using', 'SELECT id,p.t,l.label FROM ' + parent + ' p FULL JOIN ' + lookup +
        ' l USING (id) ORDER BY id', [row + ['L' + row[0]] for row in outer],
        [23,25,25], ['id','t','label'])
    add('full-inequality', 'SELECT p.id,l.id FROM ' + parent + ' p FULL JOIN ' + lookup +
        ' l ON p.id<l.id ORDER BY p.id,l.id', state='0A000')
    add('full-derived-inequality', 'SELECT d.id,l.id FROM (SELECT id FROM ' + parent + ') d FULL JOIN ' +
        lookup + ' l ON d.id<l.id ORDER BY d.id,l.id', state='0A000')
    add('full-derived-only-inequality', 'SELECT d.id,l.id FROM (SELECT id FROM ONLY ' + parent + ') d FULL JOIN ' +
        lookup + ' l ON d.id<l.id ORDER BY d.id,l.id', state='0A000')
    add('full-derived-equality', 'SELECT d.id,l.id FROM (SELECT id FROM ' + parent + ') d FULL JOIN ' +
        lookup + ' l ON d.id=l.id ORDER BY d.id,l.id',
        [['1','1'], ['2','2'], ['3','3'], ['4','4'], ['5','5'], [None,'6']], [23,23], ['id','id'])
    add('full-constant-true', 'SELECT p.id,l.id FROM ' + parent + ' p FULL JOIN ' + lookup +
        ' l ON 1=1 ORDER BY p.id,l.id LIMIT 4', [['1','1'], ['1','2'], ['1','3'], ['1','4']],
        [23,23], ['id','id'])
    add('full-constant-false', 'SELECT p.id,l.id FROM ' + parent + ' p FULL JOIN ' + lookup +
        ' l ON p.id<l.id AND false ORDER BY p.id,l.id LIMIT 4',
        [['1',None], ['2',None], ['3',None], ['4',None]], [23,23], ['id','id'])
    add('inner-scalar', 'SELECT p.id,upper(p.t),l.label FROM ' + parent + ' AS p JOIN ' + lookup + ' AS l ON p.id=l.id ORDER BY p.id',
        [row + ['L' + row[0]] for row in upper], [23,25,25], ['id','upper','label'])
    add('left-only', 'SELECT l.id,p.t FROM ' + lookup + ' AS l LEFT JOIN ONLY ' + parent + ' AS p ON l.id=p.id ORDER BY l.id', outer_only, oids, names)
    add('correlated-inner', 'SELECT l.id,(SELECT p.t FROM ' + parent + ' AS p WHERE p.id=l.id) AS chosen FROM ' + lookup + ' AS l ORDER BY l.id', outer, oids, ['id','chosen'])
    add('correlated-inner-only', 'SELECT l.id,(SELECT p.t FROM ONLY ' + parent + ' AS p WHERE p.id=l.id) AS chosen FROM ' + lookup + ' AS l ORDER BY l.id', outer_only, oids, ['id','chosen'])
    add('correlated-inner-only-parens', 'SELECT l.id,(SELECT p.t FROM ONLY (' + parent + ') AS p WHERE p.id=l.id) AS chosen FROM ' + lookup + ' AS l ORDER BY l.id', outer_only, oids, ['id','chosen'])
    add('correlated-inner-scalar', 'SELECT l.id,(SELECT upper(p.t) FROM ' + parent + ' AS p WHERE p.id=l.id) AS chosen FROM ' + lookup + ' AS l ORDER BY l.id', upper_outer, oids, ['id','chosen'])
    add('self-only-right', 'SELECT l.id,r.t FROM ' + parent + ' AS l JOIN ONLY ' + parent + ' AS r ON l.id=r.id ORDER BY l.id', only, oids, names)
    add('self-only-left', 'SELECT l.id,r.t FROM ONLY ' + parent + ' AS l JOIN ' + parent + ' AS r ON l.id=r.id ORDER BY l.id', only, oids, names)
    add('using-inner', 'SELECT p.id,p.t,l.label FROM ' + parent + ' AS p JOIN ' + lookup + ' AS l USING (id) ORDER BY p.id', joined, [23,25,25], ['id','t','label'])
    add('natural-inner', 'SELECT p.id,p.t,l.label FROM ' + parent + ' AS p NATURAL JOIN ' + lookup + ' AS l ORDER BY p.id', joined, [23,25,25], ['id','t','label'])
    add('using-left', 'SELECT l.id,p.t FROM ' + lookup + ' AS l LEFT JOIN ' + parent + ' AS p USING (id) ORDER BY l.id', outer, oids, names)
    add('derived-zero-limit', 'SELECT d.id,d.t FROM (SELECT id,t FROM ' + parent + ') AS d ORDER BY d.id LIMIT 0', [], oids, names)
    add('correlated-zero-limit', 'SELECT l.id,(SELECT p.t FROM ' + parent + ' AS p WHERE p.id=l.id) AS chosen FROM ' + lookup + ' AS l ORDER BY l.id LIMIT 0', [], oids, ['id','chosen'])
    # Preserve duplicate rows: inheritance-source traversal deduplicates table
    # identities, not values or tuple occurrences.
    add('duplicate-first', 'INSERT INTO ' + first + "(id,t) VALUES (7,'dup')", setup=True)
    add('duplicate-second', 'INSERT INTO ' + first + "(id,t) VALUES (7,'dup')", setup=True)
    add('duplicate-derived', 'SELECT d.id,d.t FROM (SELECT id,t FROM ' + parent + ') AS d WHERE d.id=7 ORDER BY d.id', [['7','dup'],['7','dup']], oids, names)
    add('duplicate-join', 'SELECT p.id,l.label FROM ' + parent + ' AS p JOIN ' + lookup + " AS l ON l.id=1 WHERE p.id=7", [['7','L1'],['7','L1']], [23,25], ['id','label'])
    commands.append({'restart': True})
    add('cold-derived', 'SELECT d.id,d.t FROM (SELECT id,t FROM ' + parent + ') AS d ORDER BY d.id', values + [['7','dup'],['7','dup']], oids, names)
    add('cold-only-derived', 'SELECT d.id,d.t FROM (SELECT id,t FROM ONLY ' + parent + ') AS d ORDER BY d.id', only, oids, names)
    add('cold-left', 'SELECT l.id,p.t FROM ' + lookup + ' AS l LEFT JOIN ' + parent + ' AS p ON l.id=p.id ORDER BY l.id', outer, oids, names)
    add('cold-correlated', 'SELECT l.id,(SELECT p.t FROM ' + parent + ' AS p WHERE p.id=l.id) AS chosen FROM ' + lookup + ' AS l ORDER BY l.id', outer, oids, ['id','chosen'])
    return commands, [parent, first, second, diamond, reordered, lookup], []



def main():
    run(steps, entry_file=__file__, cohort='inherited_nested_source', expected_checks=61)


if __name__ == '__main__':
    main()
