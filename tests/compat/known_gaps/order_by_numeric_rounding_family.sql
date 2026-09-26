-- Monotone numeric functions still need numeric, rather than textual, keys.
CREATE TEMP TABLE diff_order_rounding_family(v numeric);
INSERT INTO diff_order_rounding_family VALUES (2.2), (10.2);
SELECT v FROM diff_order_rounding_family ORDER BY floor(v);
SELECT v FROM diff_order_rounding_family ORDER BY ceil(v);
SELECT v FROM diff_order_rounding_family ORDER BY trunc(v);
SELECT v FROM diff_order_rounding_family ORDER BY sign(v);
SELECT v FROM diff_order_rounding_family ORDER BY mod(v, 3);
DROP TABLE diff_order_rounding_family;
