-- Numeric ORDER BY keys need expression results and numeric comparison.
CREATE TEMP TABLE diff_order_numeric_function(v numeric);
INSERT INTO diff_order_numeric_function VALUES (-1.4), (-1.6);
SELECT v FROM diff_order_numeric_function ORDER BY round(v);
SELECT v FROM diff_order_numeric_function ORDER BY abs(v);
SELECT v FROM diff_order_numeric_function ORDER BY abs(v) DESC;
DROP TABLE diff_order_numeric_function;
CREATE TEMP TABLE diff_order_power(v integer);
INSERT INTO diff_order_power VALUES (-2), (-1);
SELECT v FROM diff_order_power ORDER BY power(v, 2);
DROP TABLE diff_order_power;
