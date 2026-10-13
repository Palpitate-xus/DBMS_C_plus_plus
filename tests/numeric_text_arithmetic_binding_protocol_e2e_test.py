#!/usr/bin/env python3
"""R37: typed strings do not acquire implicit numeric arithmetic operators."""
from compat.numeric_type_review import Review


def main():
    review = Review('NUMERIC_TEXT_ARITHMETIC')
    try:
        for operator in ('+', '-', '*', '/', '%', '^'):
            for left, right in (("'1e-40'::text", '1e-40'), ('1e-40', "'1e-40'::text"),
                                ('NULL::text', '1e-40'), ("'2'::varchar", '1::numeric'),
                                ('NULL::numeric', "'2'::text")):
                sql = 'SELECT '+left+' '+operator+' '+right+' AS n'
                review.query(sql, state='42883')
                review.describe(sql, state='42883')
        for expression, expected, oid in (
            ("'2'+1::numeric", '3', 1700), ("1::numeric+'2'", '3', 1700),
            ("'2'+1", '3', 23), ("'1e-40'+1e-40", '0.'+'0'*39+'2', 1700),
            ("'2'::text::numeric+1", '3', 1700), ('1::integer+2::numeric', '3', 1700),
            ('1::bigint+2::numeric', '3', 1700), ('1.25::real+2::numeric', '3.25', 701),
            ('NULL+1::numeric', None, 1700), ('NULL::numeric+1', None, 1700),
        ):
            review.query('SELECT '+expression+' AS n', [[expected]], [oid])
            review.describe('SELECT '+expression+' AS n', [oid])
        review.query('CREATE TEMP TABLE numeric_text_rows(t TEXT,v VARCHAR,n NUMERIC)')
        for sql in (
            'SELECT t+n FROM numeric_text_rows', 'SELECT n+t FROM numeric_text_rows',
            'SELECT v*n FROM numeric_text_rows WHERE false',
            "SELECT '2'::text+1::numeric LIMIT 0",
            "SELECT CASE WHEN false THEN '2'::text+1::numeric ELSE 0 END",
            "INSERT INTO numeric_text_rows(n) VALUES('2'::text+1::numeric)",
            "UPDATE numeric_text_rows SET n='2'::text+1::numeric WHERE false",
        ):
            review.query(sql, state='42883')
            review.describe(sql, state='42883')
        review.query("INSERT INTO numeric_text_rows VALUES(NULL,NULL,NULL),('2','2',1)")
        review.query('SELECT t+n FROM numeric_text_rows', state='42883')
        review.query('SELECT n+CAST(t AS numeric) AS n FROM numeric_text_rows ORDER BY n NULLS LAST', [['3'],[None]], [1700])
        for sql, input_oids in (('SELECT $1+$2', (25,1700)), ('SELECT $1+$2', (1700,25)),
                                ('SELECT $1*$2', (1043,1700))):
            review.describe(sql, state='42883', parameter_oids=input_oids)
        review.parameter('SELECT $1+$2', ('2', '1'), (1700,23), [['3']], [1700])
        review.parameter('SELECT $1+$2', (None, '1'), (1700,23), [[None]], [1700])
        review.query('CREATE TEMP SEQUENCE numeric_text_calls')
        sql = "SELECT '2'::text+nextval('numeric_text_calls')"
        review.describe(sql, state='42883')
        review.query(sql, state='42883')
        review.query("SELECT currval('numeric_text_calls')", state='55000')
        review.query('SELECT count(*) FROM numeric_text_rows', [['2']], [20])
        review.query('SELECT 42', [['42']], [23])
        review.complete(109)
    finally:
        review.close()


if __name__ == '__main__':
    main()
