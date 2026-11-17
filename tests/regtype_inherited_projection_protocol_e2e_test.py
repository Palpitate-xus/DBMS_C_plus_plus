#!/usr/bin/env python3
"""Strict ordinary inherited REGTYPE projections and global physical ordering.

Uses only SimpleQuery, real parent/child tables and physical ORDER BY keys.
Domain-over-array, inherited CAST behavior and retained plans are outside this test.
"""
from compat.regtype_projection_protocol import run, steps_for


def steps(prefix):
    # Reuse the source factory's actual reference/domain declaration. Add a
    # nullable physical ordering field and permit NULL/duplicate values.
    source, _, _ = steps_for(prefix, include_inheritance=False)
    template = {command['label']: command for command in source if not command.get('restart')}
    parent, first, second, domain = (prefix + suffix for suffix in ('_rows', '_first', '_second', '_ref'))
    commands = [dict(template['domain'])]
    parent_command = dict(template['table'])
    parent_command['sql'] = parent_command['sql'].replace(
        '(id integer PRIMARY KEY,t text,', '(id integer,k integer,t text,')
    commands.append(parent_command)

    def add(label, sql, rows=None, oids=None, setup=False, headers=None, ready='I'):
        commands.append(dict(label=label, sql=sql, rows=rows, oids=oids, state=None,
                             ready=ready, setup=setup, headers=headers, unordered=False, capture=None))

    def literal(value):
        return 'NULL' if value is None else "'" + value.replace("'", "''") + "'"

    # Each record is [id, nullable ordering key, ordinary text, reference,
    # reference array, domain over scalar reference]. Names here are expected
    # PG output; the SQL inserts use the real type input functions.
    values = [
        ['1','2','parent one','-','[0:2]={integer,NULL,-}','text'],
        ['2','1','child two','integer','{}','-'],
        ['3',None,'child three','text',None,None],
        ['4','1','','integer','{text,NULL}','integer'],
        ['5','2','NULL',None,'{-,4294967295}','4294967295'],
        ['6',None,None,'4294967295','[0:1]={text,NULL}','text'],
        ['7','0','parent seven','text','{}','-'],
        ['8','1','child eight','-','{integer}','-'],
        ['9','0','child nine','integer',None,'integer'],
    ]
    by_id = {int(row[0]): row for row in values}
    types = [23,23,25,2206,2211,2206]
    add('first-child', 'CREATE TABLE ' + first + '(unused_first text) INHERITS (' + parent + ')', setup=True)
    add('second-child', 'CREATE TABLE ' + second + '(unused_second integer) INHERITS (' + parent + ')', setup=True)

    def insert(table, identifier):
        row = by_id[identifier]
        fields = [row[0], 'NULL' if row[1] is None else row[1]] + [literal(value) for value in row[2:]]
        add('insert-' + str(identifier), 'INSERT INTO ' + table +
            '(id,k,t,r,ra,d) VALUES(' + ','.join(fields) + ')', setup=True)

    for identifier in (1,4,7):
        insert(parent, identifier)
    for identifier in (2,5,8):
        insert(first, identifier)
    add('empty-child-hidden-key', 'SELECT r FROM ' + parent + ' ORDER BY id',
        [[by_id[i][3]] for i in (1,2,4,5,7,8)], [2206])
    for identifier in (3,6,9):
        insert(second, identifier)

    def projected(identifiers, positions):
        return [[by_id[identifier][position] for position in positions] for identifier in identifiers]

    all_ids = list(range(1,10))
    descending = list(reversed(all_ids))
    k_asc_id_desc = [9,7,8,4,2,5,1,6,3]
    k_desc_id_asc = [3,6,1,5,2,4,8,7,9]
    reference_asc_id_desc = [8,1,9,4,2,7,3,6,5]
    domain_desc_id_asc = [5,1,6,4,9,2,7,8,3]
    ordinary = 'SELECT id,k,t,r,ra,d FROM ' + parent
    add('global-schema-order', ordinary + ' ORDER BY id', values, types,
        headers=['id','k','t','r','ra','d'])
    add('parent-wildcard', 'SELECT * FROM ' + parent + ' ORDER BY id', values, types,
        headers=['id','k','t','r','ra','d'])
    add('global-descending', ordinary + ' ORDER BY id DESC', projected(descending, range(6)), types)
    add('hidden-id', 'SELECT r FROM ' + parent + ' ORDER BY id', projected(all_ids, [3]), [2206])
    add('hidden-id-descending', 'SELECT ra,d FROM ' + parent + ' ORDER BY id DESC',
        projected(descending, [4,5]), [2211,2206])
    add('nullable-hidden-keys', 'SELECT r FROM ' + parent + ' ORDER BY k ASC NULLS LAST,id DESC',
        projected(k_asc_id_desc, [3]), [2206])
    add('nullable-hidden-keys-descending', 'SELECT ra,d FROM ' + parent + ' ORDER BY k DESC NULLS FIRST,id ASC',
        projected(k_desc_id_asc, [4,5]), [2211,2206])
    add('reference-key-ordinal', 'SELECT r,id FROM ' + parent + ' ORDER BY 1 ASC NULLS LAST,2 DESC',
        projected(reference_asc_id_desc, [3,0]), [2206,23])
    add('domain-key-hidden-id', 'SELECT d FROM ' + parent + ' ORDER BY d DESC NULLS LAST,id ASC',
        projected(domain_desc_id_asc, [5]), [2206])
    add('alias-key', 'SELECT r AS chosen,id AS logical_id FROM ' + parent + ' ORDER BY logical_id DESC',
        projected(descending, [3,0]), [2206,23], headers=['chosen','logical_id'])
    add('alias-and-ordinal-keys', 'SELECT r AS chosen,k AS priority,id AS logical_id FROM ' + parent +
        ' ORDER BY 2 ASC NULLS LAST,logical_id DESC', projected(k_asc_id_desc, [3,1,0]),
        [2206,23,23], headers=['chosen','priority','logical_id'])
    add('reordered-columns', 'SELECT d,r,id,ra FROM ' + parent + ' ORDER BY id',
        projected(all_ids, [5,3,0,4]), [2206,2206,23,2211])
    add('repeated-columns', 'SELECT r AS first_ref,r AS second_ref,ra AS first_array,ra AS second_array,d AS chosen FROM ' +
        parent + ' ORDER BY id', projected(all_ids, [3,3,4,4,5]), [2206,2206,2211,2211,2206],
        headers=['first_ref','second_ref','first_array','second_array','chosen'])
    add('overlapping-or', ordinary + ' WHERE id<=4 OR id>=4 ORDER BY id', values, types)
    add('or-with-empty-branch', 'SELECT r,ra,d FROM ' + parent +
        ' WHERE id<0 OR id<=4 OR id>=7 OR id>=8 ORDER BY id',
        projected([1,2,3,4,7,8,9], [3,4,5]), [2206,2211,2206])
    add('or-preserves-equal-values', 'SELECT r FROM ' + parent +
        ' WHERE id<=4 OR id>=4 ORDER BY id', projected(all_ids, [3]), [2206])
    add('global-limit-offset', 'SELECT r,ra,d FROM ' + parent + ' ORDER BY id LIMIT 4 OFFSET 2',
        projected([3,4,5,6], [3,4,5]), [2206,2211,2206])
    add('global-multiple-key-limit', 'SELECT r FROM ' + parent +
        ' ORDER BY k ASC NULLS LAST,id DESC LIMIT 4 OFFSET 2', projected([8,4,2,5], [3]), [2206])
    add('or-limit-offset', 'SELECT d,r FROM ' + parent +
        ' WHERE id<0 OR id<=4 OR id>=7 ORDER BY id DESC LIMIT 4 OFFSET 1',
        projected([8,7,4,3], [5,3]), [2206,2206])
    add('only-parent', 'SELECT r,ra,d FROM ONLY ' + parent + ' ORDER BY id',
        projected([1,4,7], [3,4,5]), [2206,2211,2206])
    add('only-parent-page', 'SELECT r FROM ONLY ' + parent + ' ORDER BY id DESC LIMIT 1 OFFSET 1',
        projected([4], [3]), [2206])
    add('direct-first-child', 'SELECT id,r,ra,d FROM ' + first + ' ORDER BY id',
        projected([2,5,8], [0,3,4,5]), [23,2206,2211,2206])
    add('direct-second-child', 'SELECT id,r,ra,d FROM ' + second + ' ORDER BY id',
        projected([3,6,9], [0,3,4,5]), [23,2206,2211,2206])
    add('whole-null', 'SELECT r FROM ' + parent + ' WHERE id=5 ORDER BY k,id', [[None]], [2206])
    add('zero-identity', 'SELECT r FROM ' + parent + ' WHERE id=1 OR id=8 ORDER BY id', [['-'],['-']], [2206])
    add('maximum-identity', 'SELECT r FROM ' + parent + ' WHERE id=6 ORDER BY id', [['4294967295']], [2206])
    add('empty-result', 'SELECT r,ra,d FROM ' + parent + ' WHERE id<0 ORDER BY k,id', [], [2206,2211,2206])
    add('empty-or-result', 'SELECT r FROM ' + parent + ' WHERE id<0 OR id>=10 ORDER BY id', [], [2206])
    add('zero-limit', 'SELECT r,ra,d FROM ' + parent + ' ORDER BY k,id LIMIT 0', [], [2206,2211,2206])
    add('exhausted-offset', 'SELECT r FROM ' + parent + ' ORDER BY id OFFSET 9', [], [2206])
    add('begin', 'BEGIN', ready='T')
    add('pending-update', 'UPDATE ' + parent + " SET r='bigint' WHERE id=4", ready='T')
    add('pending-inherited-projection', 'SELECT r FROM ' + parent + ' WHERE id=4 ORDER BY id', [['bigint']], [2206], ready='T')
    add('rollback', 'ROLLBACK')
    add('rollback-restored', 'SELECT r FROM ' + parent + ' ORDER BY id', projected(all_ids, [3]), [2206])
    commands.append({'restart': True})
    add('cold-global-order', ordinary + ' ORDER BY id', values, types)
    add('cold-hidden-keys', 'SELECT r,ra,d FROM ' + parent + ' ORDER BY k DESC NULLS FIRST,id',
        projected(k_desc_id_asc, [3,4,5]), [2206,2211,2206])
    add('cold-ordinal', 'SELECT r,id FROM ' + parent + ' ORDER BY 1 ASC NULLS LAST,2 DESC',
        projected(reference_asc_id_desc, [3,0]), [2206,23])
    add('cold-or-page', 'SELECT r FROM ' + parent +
        ' WHERE id<0 OR id<=4 OR id>=7 OR id>=8 ORDER BY id DESC LIMIT 4 OFFSET 1',
        projected([8,7,4,3], [3]), [2206])
    add('cold-repeated-columns', 'SELECT d,ra,r,d FROM ' + parent + ' ORDER BY id',
        projected(all_ids, [5,4,3,5]), [2206,2211,2206,2206])
    add('cold-only-parent', 'SELECT id,r FROM ONLY ' + parent + ' ORDER BY id',
        projected([1,4,7], [0,3]), [23,2206])
    add('cold-empty', 'SELECT r,ra FROM ' + parent + ' WHERE id<0 ORDER BY k,id', [], [2206,2211])
    add('cold-zero-limit', 'SELECT r FROM ' + parent + ' ORDER BY id LIMIT 0', [], [2206])
    sequence = prefix + '_reads'
    add('zero-demand-sequence', 'CREATE SEQUENCE ' + sequence, setup=True)
    add('zero-demand-volatile-where', 'SELECT r FROM ' + parent +
        " WHERE nextval('" + sequence + "')>0 ORDER BY id LIMIT 0", [], [2206])
    add('zero-demand-first-nextval', "SELECT nextval('" + sequence + "')", [['1']], [20])
    return commands, [parent,first,second], ['DROP SEQUENCE IF EXISTS ' + sequence,
                                          'DROP DOMAIN IF EXISTS ' + domain]


def main():
    run(steps, entry_file=__file__, cohort='regtype_inherited_projection', expected_checks=60)


if __name__ == '__main__':
    main()
