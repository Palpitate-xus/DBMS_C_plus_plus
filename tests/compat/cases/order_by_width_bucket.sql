-- ORDER BY must use the computed histogram bucket, not insertion order.
CREATE TEMP TABLE diff_order_width_bucket(v integer);
INSERT INTO diff_order_width_bucket VALUES (6), (1);
SELECT v FROM diff_order_width_bucket ORDER BY width_bucket(v, 0, 10, 2);
DROP TABLE diff_order_width_bucket;
