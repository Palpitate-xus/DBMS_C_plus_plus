-- Parsing text as numeric changes lexicographic order to numeric order.
CREATE TEMP TABLE diff_order_to_number(v text);
INSERT INTO diff_order_to_number VALUES ('10'), ('2');
SELECT v FROM diff_order_to_number ORDER BY to_number(v, '999');
DROP TABLE diff_order_to_number;
