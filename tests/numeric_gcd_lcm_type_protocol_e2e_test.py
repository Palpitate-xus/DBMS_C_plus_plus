#!/usr/bin/env python3
"""R36: gcd/lcm output identity follows NUMERIC/BIGINT/INTEGER inputs."""
from compat.numeric_type_review import Review


def main():
    review = Review('NUMERIC_GCD_LCM_TYPE')
    try:
        for function in ('gcd', 'lcm'):
            for left, right, gcd, lcm, oid in (
                ('6e-40', '4e-40', '0.'+'0'*39+'2', '0.'+'0'*38+'12', 1700),
                ('6E40', '4e+40', '2'+'0'*40, '12'+'0'*40, 1700),
                ('1.250e-2', '2.50e-3', '0.00250', '0.01250', 1700),
                ('0e-40', '4e-40', '0.'+'0'*39+'4', '0.'+'0'*40, 1700),
                ('NULL::numeric', '4', None, None, 1700),
                ('6', '4', '2', '12', 23),
                ('6::smallint', '4', '2', '12', 23),
                ('6::bigint', '4', '2', '12', 20),
                ('6', '4::bigint', '2', '12', 20),
                ('6::bigint', '4e0', '2', '12', 1700),
                ("'6e-40'", '4e-40', '0.'+'0'*39+'2', '0.'+'0'*38+'12', 1700),
            ):
                sql = 'SELECT '+function+'('+left+','+right+') AS n'
                review.query(sql, [[gcd if function == 'gcd' else lcm]], [oid])
                review.describe(sql, [oid])
            for inputs, oids, expected, oid in (
                (('6e-40', '4e-40'), (1700, 1700), '0.'+'0'*39+'2' if function == 'gcd' else '0.'+'0'*38+'12', 1700),
                ((None, '4e-40'), (1700, 1700), None, 1700),
                (('6', '4'), (20, 23), '2' if function == 'gcd' else '12', 20),
            ):
                review.parameter('SELECT '+function+'($1,$2) AS n', inputs, oids, [[expected]], [oid])
        review.query('CREATE TEMP TABLE numeric_gcd_rows(n NUMERIC,b BIGINT,i INT)')
        review.query('SELECT gcd(n,4e-40),lcm(n,4e-40),gcd(b,i),lcm(i,b) FROM numeric_gcd_rows', [], [1700,1700,20,20])
        review.describe('SELECT gcd(n,4e-40),lcm(n,4e-40),gcd(b,i),lcm(i,b) FROM numeric_gcd_rows', [1700,1700,20,20])
        review.query('INSERT INTO numeric_gcd_rows VALUES(6e-40,6,4),(NULL,NULL,NULL)')
        review.query('SELECT gcd(n,4e-40),lcm(n,4e-40),gcd(b,i),lcm(i,b) FROM numeric_gcd_rows ORDER BY b NULLS LAST',
                     [['0.'+'0'*39+'2','0.'+'0'*38+'12','2','12'],[None,None,None,None]], [1700,1700,20,20])
        review.query('SELECT gcd(n,4e-40),lcm(n,4e-40) FROM numeric_gcd_rows WHERE false', [], [1700,1700])
        review.query('SELECT gcd(6e-40,4e-40),lcm(6e-40,4e-40) LIMIT 0', [], [1700,1700])
        review.query('CREATE TEMP SEQUENCE numeric_gcd_calls')
        review.describe("SELECT gcd(nextval('numeric_gcd_calls'),4e-40),lcm(nextval('numeric_gcd_calls'),4::bigint)", [1700,20])
        review.query("SELECT currval('numeric_gcd_calls')", state='55000')
        review.query('SELECT 42', [['42']], [23])
        review.complete(61)
    finally:
        review.close()


if __name__ == '__main__':
    main()
