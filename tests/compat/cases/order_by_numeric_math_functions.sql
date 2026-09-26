-- Monotone math functions still require evaluated numeric sort keys.
CREATE TEMP TABLE diff_order_math_function(v numeric);
INSERT INTO diff_order_math_function VALUES (2.2), (10.2);
SELECT v FROM diff_order_math_function ORDER BY sqrt(v);
SELECT v FROM diff_order_math_function ORDER BY ln(v);
SELECT v FROM diff_order_math_function ORDER BY log(v);
SELECT v FROM diff_order_math_function ORDER BY exp(v);
DROP TABLE diff_order_math_function;
