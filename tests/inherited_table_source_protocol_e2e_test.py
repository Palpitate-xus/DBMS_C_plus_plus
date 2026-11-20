#!/usr/bin/env python3
"""Inherited scalar projections use the complete, deduplicated table source.

The optional --reference18 mode requires an explicitly identified private
PostgreSQL 18.6 endpoint. Project mode uses owned private data and a cold
restart. Each SimpleQuery validates rows, names, type OIDs, tag and state.
"""
from compat.regtype_projection_protocol import run


def steps(prefix):
    parent, first, second, grandchild, diamond, reordered = (
        prefix + suffix for suffix in ('_root', '_first', '_second', '_grand', '_diamond', '_reordered'))
    sequence = prefix + '_sequence'
    wide_parent, wide_child = prefix + '_wide', prefix + '_wide_child'
    commands = []

    def add(label, sql, rows=None, oids=None, headers=None, setup=False, kind=None):
        item = dict(label=label, sql=sql, rows=rows, oids=oids, state=None,
                    ready='I', setup=setup, headers=headers, unordered=False, capture=None)
        if kind:
            item['statement_kind'] = kind
        commands.append(item)

    add('parent', 'CREATE TABLE ' + parent + '(id integer,t text)', setup=True)
    add('first', 'CREATE TABLE ' + first + '(extra_a text) INHERITS (' + parent + ')', setup=True)
    add('second', 'CREATE TABLE ' + second + '(extra_b text) INHERITS (' + parent + ')', setup=True)
    add('grandchild', 'CREATE TABLE ' + grandchild + '(extra_g text) INHERITS (' + first + ')', setup=True)
    # The same descendant occurs on two inheritance paths; its row occurs once.
    add('diamond', 'CREATE TABLE ' + diamond + '(extra_d text) INHERITS (' + first + ',' + second + ')', setup=True)
    # ALTER INHERIT preserves the child's pre-existing t,id physical order.
    add('reordered', 'CREATE TABLE ' + reordered + '(t text,id integer,extra_r text)', setup=True)
    add('attach-reordered', 'ALTER TABLE ' + reordered + ' INHERIT ' + parent, setup=True, kind='ALTER TABLE')
    for label, table, values in (
        ('parent', parent, "(1,'parent')"),
        ('first-null', first, '(2,NULL)'),
        ('first-empty', first, "(3,'')"),
        ('second', second, "(4,'MiXeD')"),
        ('grandchild', grandchild, "(5,'a,b')"),
        ('diamond', diamond, "(6,'delta')"),
        ('reordered', reordered, "(7,'return')"),
    ):
        add('insert-' + label, 'INSERT INTO ' + table + '(id,t) VALUES' + values, setup=True)

    values = [['1', 'parent'], ['2', None], ['3', ''], ['4', 'MiXeD'],
              ['5', 'a,b'], ['6', 'delta'], ['7', 'return']]
    ids = [row[0] for row in values]
    upper = [['PARENT'], [None], [''], ['MIXED'], ['A,B'], ['DELTA'], ['RETURN']]
    replaced = [['pZrent'], [None], [''], ['MiXeD'], ['Z,b'], ['deltZ'], ['return']]
    lengths = [['6'], [None], ['0'], ['5'], ['3'], ['5'], ['6']]
    names = ['id', 't']
    text_type, int_type = [25], [23]
    source = ' FROM ' + parent
    ordered = source + ' ORDER BY id'
    add('parent-schema-and-diamond', 'SELECT *' + ordered, values, [23, 25], names)
    add('plain-order', 'SELECT id,t' + ordered, values, [23, 25], names)
    add('plain-order-desc', 'SELECT id,t' + source + ' ORDER BY id DESC', list(reversed(values)), [23, 25], names)
    add('plain-filter-page', 'SELECT id,t' + source + ' WHERE id>=2 ORDER BY id LIMIT 3 OFFSET 1', values[2:5], [23, 25], names)
    add('plain-aliased', 'SELECT p.id,p.t FROM ' + parent + ' AS p ORDER BY p.id', values, [23, 25], names)
    add('qualified-star', 'SELECT p.* FROM ' + parent + ' AS p ORDER BY p.id', values, [23, 25], names)
    add('upper', 'SELECT upper(t)' + ordered, upper, text_type, ['upper'])
    add('upper-concat', "SELECT upper('pre:'||t)" + ordered,
        [['PRE:PARENT'], [None], ['PRE:'], ['PRE:MIXED'], ['PRE:A,B'], ['PRE:DELTA'], ['PRE:RETURN']],
        text_type, ['upper'])
    add('replace', "SELECT replace(t,'a','Z')" + ordered, replaced, text_type, ['replace'])
    add('length', 'SELECT length(t)' + ordered, lengths, int_type, ['length'])
    add('mixed-projection', 'SELECT id,upper(t),length(t)' + ordered,
        [[row[0], upper[i][0], lengths[i][0]] for i, row in enumerate(values)],
        [23, 25, 23], ['id', 'upper', 'length'])
    add('scalar-filter-page', 'SELECT upper(t)' + source + ' WHERE id>=2 ORDER BY id LIMIT 3 OFFSET 1', upper[2:5], text_type, ['upper'])
    add('scalar-hidden-order', 'SELECT upper(t)' + source + ' ORDER BY id DESC LIMIT 3 OFFSET 1', list(reversed(upper))[1:4], text_type, ['upper'])
    add('scalar-alias', 'SELECT upper(p.t) AS chosen FROM ' + parent + ' AS p ORDER BY p.id', upper, text_type, ['chosen'])
    add('scalar-alias-replace', "SELECT replace(p.t,'a','Z') AS chosen FROM " + parent + ' p ORDER BY p.id', replaced, text_type, ['chosen'])
    add('first-and-descendants', 'SELECT id,t FROM ' + first + ' ORDER BY id', [values[i-1] for i in (2,3,5,6)], [23,25], names)
    add('grand-direct', 'SELECT upper(t) FROM ' + grandchild + ' ORDER BY id', [upper[4]], text_type, ['upper'])
    add('diamond-direct', 'SELECT upper(t) FROM ' + diamond + ' ORDER BY id', [upper[5]], text_type, ['upper'])
    add('reordered-direct', 'SELECT id,t FROM ' + reordered + ' ORDER BY id', [values[6]], [23,25], names)
    add('only-bare', 'SELECT upper(t) FROM ONLY ' + parent + ' ORDER BY id', [upper[0]], text_type, ['upper'])
    add('only-parenthesized', 'SELECT upper(t) FROM ONLY (' + parent + ') ORDER BY id', [upper[0]], text_type, ['upper'])
    add('only-alias', 'SELECT upper(p.t) FROM ONLY ' + parent + ' AS p ORDER BY p.id', [upper[0]], text_type, ['upper'])
    add('only-parenthesized-alias', 'SELECT upper(p.t) FROM ONLY (' + parent + ') AS p ORDER BY p.id', [upper[0]], text_type, ['upper'])
    add('only-child', 'SELECT id,t FROM ONLY ' + first + ' ORDER BY id', values[1:3], [23,25], names)
    add('empty-result', 'SELECT upper(t)' + source + ' WHERE id<0 ORDER BY id', [], text_type, ['upper'])
    add('zero-limit', 'SELECT upper(t)' + source + ' ORDER BY id LIMIT 0', [], text_type, ['upper'])
    # The sort key and large payload are deliberately absent from projection.
    # A separate hierarchy leaves the parent's SELECT * contract unchanged.
    add('wide-parent', 'CREATE TABLE ' + wide_parent + '(id integer,t text,sortkey integer,unused text)', setup=True)
    add('wide-child', 'CREATE TABLE ' + wide_child + '(extra text) INHERITS (' + wide_parent + ')', setup=True)
    payload = "'" + ('x' * 16384) + "'"
    add('wide-insert-parent', 'INSERT INTO ' + wide_parent + " VALUES(1,'a',20," + payload + ')', setup=True)
    add('wide-insert-child', 'INSERT INTO ' + wide_child + "(id,t,sortkey,unused) VALUES(2,'b',10," + payload + ')', setup=True)
    add('wide-hidden-order', 'SELECT id FROM ' + wide_parent + ' ORDER BY sortkey', [['2'],['1']], [23], ['id'])
    add('wide-scalar-hidden-order', 'SELECT upper(t) FROM ' + wide_parent + ' ORDER BY sortkey', [['B'],['A']], [25], ['upper'])
    add('wide-count-star', 'SELECT count(*) FROM ' + wide_parent, [['2']], [20], ['count'])
    add('wide-aggregate-filter', "SELECT count(*) FILTER (WHERE t='a') FROM " + wide_parent, [['1']], [20], ['count'])
    add('wide-sum', 'SELECT sum(id) FROM ' + wide_parent, [['3']], [20], ['sum'])
    add('wide-aggregate-filter-child', "SELECT count(*) FILTER (WHERE t='b') FROM " + wide_parent, [['1']], [20], ['count'])
    # p.t is needed by the subquery but is not an outer output column.
    add('wide-correlated-hidden-column', 'SELECT p.id,(SELECT upper(p.t)) AS chosen FROM ' + wide_parent + ' AS p ORDER BY p.id',
        [['1','A'], ['2','B']], [23,25], ['id','chosen'])
    add('wide-detach', 'ALTER TABLE ' + wide_child + ' NO INHERIT ' + wide_parent, setup=True, kind='ALTER TABLE')
    add('wide-detached-count', 'SELECT count(*) FROM ' + wide_parent, [['1']], [20], ['count'])
    add('wide-detached-rows', 'SELECT id FROM ' + wide_parent + ' ORDER BY id', [['1']], [23], ['id'])
    add('wide-reattach', 'ALTER TABLE ' + wide_child + ' INHERIT ' + wide_parent, setup=True, kind='ALTER TABLE')
    add('wide-reattached-count', 'SELECT count(*) FROM ' + wide_parent, [['2']], [20], ['count'])
    add('wide-reattached-sum', 'SELECT sum(id) FROM ' + wide_parent, [['3']], [20], ['sum'])
    add('wide-reattached-scalar', 'SELECT upper(t) FROM ' + wide_parent + ' ORDER BY id', [['A'],['B']], [25], ['upper'])
    add('sequence', 'CREATE SEQUENCE ' + sequence, setup=True)
    add('zero-demand', 'SELECT id' + source + " WHERE nextval('" + sequence + "')>0 ORDER BY id LIMIT 0", [], int_type, ['id'])
    add('zero-demand-check', "SELECT nextval('" + sequence + "')", [['1']], [20], ['nextval'])
    add('volatile-global-page', 'SELECT id,nextval(\'' + sequence + '\')' + source + ' ORDER BY id LIMIT 2 OFFSET 1',
        [['2','3'], ['3','4']], [23,20], ['id','nextval'])
    add('volatile-count', "SELECT nextval('" + sequence + "')", [['5']], [20], ['nextval'])
    commands.append({'restart': True})
    add('cold-parent', 'SELECT id,t' + ordered, values, [23,25], names)
    add('cold-upper', 'SELECT upper(t)' + ordered, upper, text_type, ['upper'])
    add('cold-only', 'SELECT upper(t) FROM ONLY (' + parent + ') ORDER BY id', [upper[0]], text_type, ['upper'])
    add('cold-page', 'SELECT id,t' + source + ' ORDER BY id LIMIT 3 OFFSET 2', values[2:5], [23,25], names)
    add('cold-zero-demand', 'SELECT id' + source + " WHERE nextval('" + sequence + "')>0 ORDER BY id LIMIT 0", [], int_type, ['id'])
    add('cold-sequence', "SELECT nextval('" + sequence + "')", [['6']], [20], ['nextval'])
    return commands, [parent, first, second, grandchild, diamond, reordered, wide_parent, wide_child], ['DROP SEQUENCE IF EXISTS ' + sequence]


def main():
    run(steps, entry_file=__file__, cohort='inherited_table_source', expected_checks=69)


if __name__ == '__main__':
    main()
