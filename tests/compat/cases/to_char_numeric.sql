-- to_char numeric patterns
SELECT to_char(1234.567, '9999.99')
SELECT to_char(42, '00099')
SELECT to_char(0.5, '9.99')
SELECT to_char(0.5, '999.99')
SELECT to_char(5, '999')
SELECT to_char(1234.567, '99999.999')
SELECT to_char(-3.14, 'FM99.99')
SELECT to_char(3.14, '99.99')
