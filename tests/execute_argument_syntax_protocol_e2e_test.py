#!/usr/bin/env python3
"""Dollar signs within identifiers must not create prepared parameter slots."""
from compat.numeric_type_review import Review


VALID = ['1', '1, 2', '1/0', 'missing_argument_column', 'no_such_function(99)', '(SELECT 1)', '(SELECT missing_argument_column)', '$0', '$1', '$2', '$000', '$2147483647', 'ARRAY[1,2]', 'ARRAY[]', 'ROW()', 'ROW(1,2)', '(1,2)', 'f()', 'f(1,2)', 'missing_schema.fn(missing_column)', 'CASE WHEN true THEN 1 ELSE 2 END', 'CASE 1 WHEN 1 THEN missing_column END', 'NULL', 'true', 'UNKNOWN', '1+2*3', 'CAST(NULL AS integer)', 'NULL::missing_type', "missing_type 'bad'", 'CURRENT_TIMESTAMP(999)', 'MIN(1)', 'count(*)', 'sum(1) OVER ()', 'lead(1) OVER (ORDER BY 1)', '1 IN (1,2)', '1 = ANY(ARRAY[1,2])', 'EXISTS (SELECT 1)', 'EXISTS (SELECT 1 FROM missing_table)', 'ARRAY(SELECT 1)', '1e9999', "'x,y'", '$$x,y($0)$$', '$_tag$x,y($0)$_tag$', "E'it\\'s a,b'", '/* comment */1', '1 /* tail */', '1 -- line\n, 2', '1 /* outer /* inner */ rest */ , 2', '"quoted,column"', 'value$0', '(SELECT)', 'DEFAULT', 'DEFAULT+1', 'f(DEFAULT)', 'ROW(DEFAULT)', 'ARRAY[DEFAULT]', '(SELECT FROM missing_table)', '(SELECT WHERE true)', '(SELECT ALL)', 'f(VARIADIC ARRAY[1,2])', 'f(a=>1)', 'f(a:=1)']
INVALID = ['', ',', '1,', ',1', '1,,2', '+', '1+', '1 AND', 'NOT', '(1', '1)', 'ARRAY[1,]', 'ARRAY[1', 'ARRAY[,1]', '[1]', 'f(,1)', 'f(1,)', 'f(1,,2)', '(SELECT 1,)', '(SELECT 1+)', '1 AS x', '1 x', '1 FROM t', '1 ORDER BY 1', 'SELECT 1', '1; SELECT 2', 'CASE WHEN true THEN 1', 'CASE WHEN true 1 END', 'CASE ELSE 1 END', 'CAST(1 AS)', '1::', "'unterminated", '/* comment */', 'f(1', '(1,)', '*', 'ROW(1,)', '(SELECT DISTINCT)', '(SELECT DISTINCT ON (1))']

def check_execute_arguments(review):
    def command(sql, tag):
        result=review.runner.decode_wire_result(review.client.simple_query(review.sock,sql),include_types=True)
        review.record(sql, result[0]==[] and result[1] is None and result[4]==tag, result, ([],None,tag))
    review.query('PREPARE syntax_probe AS SELECT 7 AS plain')
    review.query('EXECUTE syntax_probe', rows=[['7']], oids=[23])
    for expression in VALID:
        review.query('EXECUTE syntax_probe('+expression+')', rows=[['7']], oids=[23])
    for expression in INVALID:
        review.query('EXECUTE syntax_probe('+expression+')', state='42601')
        review.query('SELECT 11 AS alive', rows=[['11']], oids=[23])
    command('CREATE SEQUENCE r293_ignored_sequence START WITH 100', 'CREATE SEQUENCE')
    review.query("EXECUTE syntax_probe(nextval('r293_ignored_sequence'))", rows=[['7']], oids=[23])
    review.query("SELECT nextval('r293_ignored_sequence') AS v", rows=[['100']], oids=[20])
    review.query("EXECUTE syntax_probe((SELECT nextval('r293_ignored_sequence')))", rows=[['7']], oids=[23])
    review.query("SELECT nextval('r293_ignored_sequence') AS v", rows=[['101']], oids=[20])
    command('DROP SEQUENCE r293_ignored_sequence', 'DROP SEQUENCE')
    command('CREATE TABLE r293_write_target (id integer)', 'CREATE TABLE')
    review.query('PREPARE r293_insert AS INSERT INTO r293_write_target VALUES (9)')
    command('EXECUTE r293_insert(1/0)', 'INSERT 0 1')
    review.query('SELECT id FROM r293_write_target', rows=[['9']], oids=[23])
    review.query('PREPARE r293_update AS UPDATE r293_write_target SET id=10 RETURNING id')
    review.query('EXECUTE r293_update(no_such_function())', rows=[['10']], oids=[23], tag='UPDATE 1')
    review.query('SELECT id FROM r293_write_target', rows=[['10']], oids=[23])
    review.query('PREPARE r293_delete AS DELETE FROM r293_write_target RETURNING id')
    review.query('EXECUTE r293_delete($0)', rows=[['10']], oids=[23], tag='DELETE 1')
    review.query('SELECT id FROM r293_write_target', rows=[], oids=[23])
    for name in ('r293_insert','r293_update','r293_delete'):
        review.query('DEALLOCATE '+name)
    command('DROP TABLE r293_write_target', 'DROP TABLE')
    review.query('PREPARE r293_text(text) AS SELECT $1 AS value')
    review.query('EXECUTE r293_text($tag$a,b$tag$)', rows=[['a,b']], oids=[25])
    review.query('EXECUTE r293_text($$a,b($1)$$)', rows=[['a,b($1)']], oids=[25])
    review.query("EXECUTE r293_text(E'it\\'s,a')", rows=[["it's,a"]], oids=[25])
    review.query("EXECUTE r293_text('a''b,c')", rows=[["a'b,c"]], oids=[25])
    review.query('DEALLOCATE r293_text')
    review.query('PREPARE r293_array(integer[]) AS SELECT $1 AS value')
    review.query('EXECUTE r293_array(ARRAY[1,2])', rows=[['{1,2}']], oids=[1007])
    review.query('DEALLOCATE r293_array')
    for name,query,value in (('r293_unused','7','7'),('r293_actual','$1','5')):
        review.query('PREPARE '+name+'(integer) AS SELECT '+query+' AS plain')
        review.query('EXECUTE '+name, state='42601')
        review.query('EXECUTE '+name+'(5)', rows=[[value]], oids=[23])
        review.query('EXECUTE '+name+'(1,2)', state='42601')
        review.query('EXECUTE '+name+'(1/0)', state='22012')
        review.query('DEALLOCATE '+name)
    for sql,state in (('EXECUTE r293_missing()', '42601'),
                      ('EXECUTE r293_missing(1+)', '42601'),
                      ('EXECUTE r293_missing(,)', '42601'),
                      ('EXECUTE r293_missing(1)', '26000'),
                      ('EXECUTE r293_missing', '26000')):
        review.query(sql,state=state)
    review.query('SELECT 11 AS alive',rows=[['11']],oids=[23])
    review.query('PREPARE r293_case AS SELECT 7 AS plain')
    review.query('EXECUTE R293_CASE(1)',rows=[['7']],oids=[23])
    review.query('DEALLOCATE r293_case')
    review.query('DEALLOCATE syntax_probe')


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
        check_execute_arguments(review)
        review.complete(419)
    finally:
        review.close()


if __name__ == '__main__':
    main()
