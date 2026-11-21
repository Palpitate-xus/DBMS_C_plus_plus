#!/usr/bin/env python3
"""Ordinary wide tables, nullable row boundaries and the 1600-column limit.

Project mode owns its private data directory and performs a normal restart.
The optional --reference18 mode requires an explicitly identified private
PostgreSQL 18.6 endpoint. The 1600-column int4 rows fit in one 8 KiB page;
this test does not claim arbitrary oversized rows fit in that page.
"""
import sys

from compat.regtype_projection_protocol import run


def steps(prefix):
    legacy, thirty_one, sixty_five, alter64, max_table, retry, duplicate = (
        prefix + suffix for suffix in ('_legacy', '_31', '_65', '_alter64', '_1600', '_retry', '_duplicate'))
    index = prefix + '_late_idx'
    fk_parent, fk_child = prefix + '_fk_parent', prefix + '_fk_child'
    commands = []

    def add(label, sql, rows=None, oids=None, headers=None, state=None,
            setup=False, kind=None):
        item = dict(label=label, sql=sql, rows=rows, oids=oids, state=state,
                    ready='I', setup=setup, headers=headers, unordered=False, capture=None)
        if kind:
            item['statement_kind'] = kind
        commands.append(item)

    def column(n):
        return 'c%04d' % n

    def declaration(n):
        return ','.join(column(i) + ' integer' for i in range(1, n + 1))

    # Existing narrow schema and its rows survive subsequent ALTERs/restart.
    add('legacy-create', 'CREATE TABLE ' + legacy + '(' + declaration(29) + ')', setup=True)
    add('legacy-insert', 'INSERT INTO ' + legacy + '(c0001,c0029) VALUES (1,29)', setup=True)
    add('legacy-add-30', 'ALTER TABLE ' + legacy + ' ADD COLUMN c0030 integer', setup=True, kind='ALTER TABLE')
    add('legacy-old-null', 'SELECT c0001,c0029,c0030 FROM ' + legacy,
        [['1','29',None]], [23,23,23], ['c0001','c0029','c0030'])

    add('create-31', 'CREATE TABLE ' + thirty_one + '(' + declaration(31) + ')', setup=True)
    add('insert-31', 'INSERT INTO ' + thirty_one + '(c0001,c0030,c0031) VALUES (1,NULL,31)', setup=True)
    add('select-31', 'SELECT c0001,c0030,c0031 FROM ' + thirty_one,
        [['1',None,'31']], [23,23,23], ['c0001','c0030','c0031'])

    add('create-65', 'CREATE TABLE ' + sixty_five + '(' + declaration(65) + ')', setup=True)
    row_a = [None if i % 2 == 0 else str(i) for i in range(1,66)]
    row_b = [str(i) if i % 2 == 0 else None for i in range(1,66)]
    row_a[0], row_b[0] = '1', '2'
    literals = lambda row: ','.join('NULL' if value is None else value for value in row)
    add('insert-65-a', 'INSERT INTO ' + sixty_five + ' VALUES (' + literals(row_a) + ')', setup=True)
    add('insert-65-b', 'INSERT INTO ' + sixty_five + ' VALUES (' + literals(row_b) + ')', setup=True)
    add('star-65', 'SELECT * FROM ' + sixty_five + ' ORDER BY c0001', [row_a,row_b], [23]*65,
        [column(i) for i in range(1,66)])
    add('bitmap-65', 'SELECT c0001,c0063,c0064,c0065 FROM ' + sixty_five + ' ORDER BY c0001',
        [['1','63',None,'65'], ['2',None,'64',None]], [23]*4,
        ['c0001','c0063','c0064','c0065'])
    add('row-65', 'SELECT ROW(c0063,c0064,c0065) FROM ' + sixty_five + ' ORDER BY c0001',
        [['(63,,65)'], ['(,64,)']], [2249], ['row'])
    add('index-65', 'CREATE INDEX ' + index + ' ON ' + sixty_five + '(c0065)', setup=True)
    add('index-select-65', 'SELECT c0001,c0065 FROM ' + sixty_five + ' WHERE c0065=65',
        [['1','65']], [23,23], ['c0001','c0065'])

    # Add a nullable 65th attribute to an existing physical 64-column row.
    add('alter-create-64', 'CREATE TABLE ' + alter64 + '(' + declaration(64) + ')', setup=True)
    add('alter-insert-old', 'INSERT INTO ' + alter64 + '(c0001,c0063,c0064) VALUES (1,63,64)', setup=True)
    add('alter-add-65', 'ALTER TABLE ' + alter64 + ' ADD COLUMN c0065 integer', setup=True, kind='ALTER TABLE')
    add('alter-old-null', 'SELECT c0001,c0063,c0064,c0065 FROM ' + alter64,
        [['1','63','64',None]], [23]*4, ['c0001','c0063','c0064','c0065'])
    add('alter-update-65', 'UPDATE ' + alter64 + ' SET c0065=65 WHERE c0001=1', setup=True)
    add('alter-updated', 'SELECT c0001,c0063,c0064,c0065 FROM ' + alter64,
        [['1','63','64','65']], [23]*4, ['c0001','c0063','c0064','c0065'])
    add('duplicate-add', 'ALTER TABLE ' + alter64 + ' ADD COLUMN c0065 integer', state='42701', kind='ALTER TABLE')
    add('missing-late-column', 'SELECT c0066 FROM ' + alter64, state='42703')

    add('create-1600', 'CREATE TABLE ' + max_table + '(' + declaration(1600) + ')', setup=True)
    alternating = [None if i % 2 == 0 else str(i) for i in range(1,1601)]
    full = [str(i) for i in range(1,1601)]
    alternating[0], full[0] = '1', '2'
    add('insert-1600-null', 'INSERT INTO ' + max_table + ' VALUES (' + literals(alternating) + ')', setup=True)
    add('insert-1600-full', 'INSERT INTO ' + max_table + ' VALUES (' + literals(full) + ')', setup=True)
    add('star-1600', 'SELECT * FROM ' + max_table + ' ORDER BY c0001', [alternating,full], [23]*1600,
        [column(i) for i in range(1,1601)])
    add('boundary-1600', 'SELECT c0001,c0063,c0064,c0065,c1599,c1600 FROM ' + max_table + ' ORDER BY c0001',
        [['1','63',None,'65','1599',None], ['2','63','64','65','1599','1600']], [23]*6,
        ['c0001','c0063','c0064','c0065','c1599','c1600'])
    add('row-1600', 'SELECT ROW(c0063,c0064,c0065) FROM ' + max_table + ' ORDER BY c0001',
        [['(63,,65)'], ['(63,64,65)']], [2249], ['row'])
    add('too-many', 'CREATE TABLE ' + retry + '(' + declaration(1601) + ')', state='54011')
    add('reuse-after-reject', 'CREATE TABLE ' + retry + '(id integer)', setup=True)
    add('reuse-insert', 'INSERT INTO ' + retry + ' VALUES (7)', setup=True)
    add('reuse-select', 'SELECT id FROM ' + retry, [['7']], [23], ['id'])
    add('duplicate-definition', 'CREATE TABLE ' + duplicate + '(a integer,a integer)', state='42701')
    # This engine still deliberately limits foreign keys per table to 30.
    # PostgreSQL accepts this different limit, so it is a project-only guard:
    # reject the 31st FK before publishing any table, never silently drop it.
    if '--reference18' not in sys.argv:
        add('fk-parent', 'CREATE TABLE ' + fk_parent + '(id integer PRIMARY KEY)', setup=True)
        constraints = ','.join('CONSTRAINT fk%02d FOREIGN KEY (x) REFERENCES '%i + fk_parent + '(id)'
                               for i in range(31))
        add('fk-too-many', 'CREATE TABLE ' + fk_child + '(x integer,' + constraints + ')', state='54000')
        add('fk-not-published', 'SELECT * FROM ' + fk_child, state='42P01')
        add('fk-reuse', 'CREATE TABLE ' + fk_child + '(x integer)', setup=True)
        add('fk-reuse-insert', 'INSERT INTO ' + fk_child + ' VALUES (9)', setup=True)
        add('fk-reuse-select', 'SELECT x FROM ' + fk_child, [['9']], [23], ['x'])
    commands.append({'restart': True})
    add('cold-legacy', 'SELECT c0001,c0029,c0030 FROM ' + legacy,
        [['1','29',None]], [23,23,23], ['c0001','c0029','c0030'])
    add('cold-31', 'SELECT c0001,c0030,c0031 FROM ' + thirty_one,
        [['1',None,'31']], [23,23,23], ['c0001','c0030','c0031'])
    add('cold-65', 'SELECT c0001,c0063,c0064,c0065 FROM ' + sixty_five + ' ORDER BY c0001',
        [['1','63',None,'65'], ['2',None,'64',None]], [23]*4, ['c0001','c0063','c0064','c0065'])
    add('cold-index-65', 'SELECT c0001,c0065 FROM ' + sixty_five + ' WHERE c0065=65',
        [['1','65']], [23,23], ['c0001','c0065'])
    add('cold-alter-65', 'SELECT c0001,c0063,c0064,c0065 FROM ' + alter64,
        [['1','63','64','65']], [23]*4, ['c0001','c0063','c0064','c0065'])
    add('cold-1600', 'SELECT c0001,c0063,c0064,c0065,c1599,c1600 FROM ' + max_table + ' ORDER BY c0001',
        [['1','63',None,'65','1599',None], ['2','63','64','65','1599','1600']], [23]*6,
        ['c0001','c0063','c0064','c0065','c1599','c1600'])
    add('cold-reuse', 'SELECT id FROM ' + retry, [['7']], [23], ['id'])
    return commands, [legacy, thirty_one, sixty_five, alter64, max_table, retry, duplicate, fk_parent, fk_child], []


def main():
    run(steps, entry_file=__file__, cohort='wide_table',
        expected_checks=41 if '--reference18' in sys.argv else 47)


if __name__ == '__main__':
    main()
