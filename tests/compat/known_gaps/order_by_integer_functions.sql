-- Multi-argument integer functions must not reuse a missing input-column key.
CREATE TEMP TABLE diff_order_integer_function(v integer);
INSERT INTO diff_order_integer_function VALUES (9), (4);
SELECT v FROM diff_order_integer_function ORDER BY gcd(v, 6);
SELECT v FROM diff_order_integer_function ORDER BY lcm(v, 6);
SELECT v FROM diff_order_integer_function ORDER BY div(v, 6);
DROP TABLE diff_order_integer_function;
