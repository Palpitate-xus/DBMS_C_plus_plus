#!/usr/bin/env python3
"""Dollar signs within identifiers must not create prepared parameter slots."""
from compat.numeric_type_review import Review


def main():
    review = Review('PREPARED_IDENTIFIER_WIRE')
    aliases = ('value$1', 'value$0', 'value$12', 'value$01', 'value$1$2',
               'value$$', 'value$tag$', 'value$_tag$', '_value$2', 'a9$2',
               'value$184467440737095516160', 'value$', 'MiXeD$3', 'value$$$0')
    try:
        for index, alias in enumerate(aliases):
            name = 'identifier_review_' + str(index)
            review.query('SELECT 7 AS ' + alias, rows=[['7']], oids=[23])
            review.query('PREPARE ' + name + ' AS SELECT 7 AS ' + alias)
            review.query('EXECUTE ' + name, rows=[['7']], oids=[23])
            review.query('EXECUTE ' + name + '(1)', rows=[['7']], oids=[23])
            review.query('DEALLOCATE ' + name)
            review.query('PREPARE ' + name + '(integer) AS SELECT $1::integer AS ' + alias)
            review.query('EXECUTE ' + name + '(23)', rows=[['23']], oids=[23])
            review.query('DEALLOCATE ' + name)
            review.query('PREPARE ' + name + '(integer) AS SELECT 7 AS ' + alias + ', $1::integer AS actual')
            review.query('EXECUTE ' + name + '(31)', rows=[['7', '31']], oids=[23, 23])
            review.query('DEALLOCATE ' + name)
            review.describe('SELECT 7 AS ' + alias, oids=[23])
            review.parameter('SELECT $1::integer AS ' + alias, ['19'], [23], [['19']], [23])
            review.query('SELECT 7 AS ' + alias + ' LIMIT 0', rows=[], oids=[23])
            review.query('SELECT 7 AS "' + alias + '"', rows=[['7']], oids=[23])
            review.query('SELECT 7 AS ' + alias + ' /* $0 /* $99 */ */ -- $88\n', rows=[['7']], oids=[23])
        review.query('SELECT $0', state='42P02')
        review.query('SELECT 11 AS alive', rows=[['11']], oids=[23])
        review.complete(226)
    finally:
        review.close()


if __name__ == '__main__':
    main()
