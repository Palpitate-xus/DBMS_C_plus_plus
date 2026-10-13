#!/usr/bin/env python3
"""R38: legacy table projections retain scientific exponent signs."""
from decimal import Decimal
from compat.numeric_type_review import Review
from scientific_numeric_arithmetic_protocol_e2e_test import expected_arithmetic


def main():
    review = Review('SCIENTIFIC_NUMERIC_COLUMN')
    a = '2e-40'
    cases = []
    for operator, literal in (('+','1e-40'), ('-','1e-40'), ('*','1e+40'),
                              ('%','1.5e-40'), ('+','1E-40'), ('+','.5e-40'),
                              ('+','5.e-41'), ('+','1e+40'), ('+','1E+40')):
        cases.append(('a'+operator+literal, expected_arithmetic(a, literal, operator)))
    tiny3 = '0.'+'0'*39+'3'
    tiny1 = '0.'+'0'*39+'1'
    cases += [
        ('a/1e-40', '2.'+'0'*40), ('a + -1e-40', tiny1),
        ('a-(-1e-40)', tiny3), ('a+(1e-40)', tiny3),
        ('abs(a)+1e-40', tiny3), ('a::numeric+1e-40', tiny3),
        ('1e-40+a', tiny3), ('a+1e-40+1e-40', '0.'+'0'*39+'4'),
        ('a+1e-40*2e0', '0.'+'0'*39+'4'),
        ('rate-1e-40', tiny1), ('rate+1e-40', tiny3),
    ]
    try:
        review.query('CREATE TEMP TABLE scientific_column_rows(id INT PRIMARY KEY,a NUMERIC,rate NUMERIC)')
        for expression in ('a+1e-40', 'a-1e+40', 'rate-1e-40'):
            review.query('SELECT '+expression+' AS n FROM scientific_column_rows', [], [1700])
            review.describe('SELECT '+expression+' AS n FROM scientific_column_rows', [1700])
        review.query('INSERT INTO scientific_column_rows VALUES(1,2e-40,2e-40),(2,NULL,NULL)')
        for expression, expected in cases:
            sql = 'SELECT '+expression+' AS n FROM scientific_column_rows ORDER BY id'
            review.query(sql, [[expected],[None]], [1700])
            review.describe(sql, [1700])
        review.query('SELECT a+1e-40 AS n FROM scientific_column_rows WHERE false', [], [1700])
        review.query('SELECT a+1e-40 AS n FROM scientific_column_rows LIMIT 0', [], [1700])
        # Bare literals already retain exponent spelling on the wire (R32).
        # This lexical control checks exact value AND decimal scale, without
        # adding an output-canonicalization requirement to the projection fix.
        literal_sql = 'SELECT 1e-40 AS n FROM scientific_column_rows ORDER BY id'
        literal_result = review.runner.decode_wire_result(review.client.simple_query(review.sock, literal_sql), include_types=True)
        valid = literal_result[1] is None and literal_result[4] == 'SELECT 2' and literal_result[5] == [1700]
        try:
            valid = valid and [[Decimal(cell).as_tuple() for cell in row] for row in literal_result[0]] == [[Decimal(tiny1).as_tuple()]]*2
        except (TypeError, ValueError, ArithmeticError):
            valid = False
        review.record(literal_sql, valid, literal_result, 'two exact NUMERIC 1e-40 cells, scale 40')
        review.query('SELECT 2e-40+1e-40 AS n', [[tiny3]], [1700])
        review.parameter('SELECT a+$1 AS n FROM scientific_column_rows ORDER BY id', ('1e-40',), (1700,), [[tiny3],[None]], [1700])
        review.parameter('SELECT a+$1 AS n FROM scientific_column_rows ORDER BY id', (None,), (1700,), [[None],[None]], [1700])
        review.query('SELECT rate-1,rate+1 FROM scientific_column_rows WHERE id=1',
                     [[expected_arithmetic(a,'1','-'),expected_arithmetic(a,'1','+')]], [1700,1700])
        review.query("SELECT '1e-40'::text AS n FROM scientific_column_rows ORDER BY id", [['1e-40'],['1e-40']], [25])
        review.query('CREATE TEMP SEQUENCE scientific_column_calls')
        review.describe("SELECT a+1e-40+nextval('scientific_column_calls') AS n FROM scientific_column_rows", [1700])
        review.query("SELECT currval('scientific_column_calls')", state='55000')
        review.query('SELECT 42', [['42']], [23])
        review.complete(60)
    finally:
        review.close()


if __name__ == '__main__':
    main()
