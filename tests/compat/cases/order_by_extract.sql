-- EXTRACT ordering follows the extracted field, not source timestamps.
CREATE TEMP TABLE diff_order_extract(v timestamp);
INSERT INTO diff_order_extract VALUES ('2020-12-01 00:00:00'), ('2021-01-01 00:00:00');
SELECT v FROM diff_order_extract ORDER BY extract(month from v);
DROP TABLE diff_order_extract;
