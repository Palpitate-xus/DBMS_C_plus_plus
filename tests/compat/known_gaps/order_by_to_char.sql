-- Formatting changes integer order to text order (10 before 2).
CREATE TEMP TABLE diff_order_to_char(v integer);
INSERT INTO diff_order_to_char VALUES (2), (10);
SELECT v FROM diff_order_to_char ORDER BY to_char(v, 'FM99');
DROP TABLE diff_order_to_char;
