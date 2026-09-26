-- date_part sorting follows the extracted field, not source timestamps.
CREATE TEMP TABLE diff_order_date_part(v timestamp);
INSERT INTO diff_order_date_part VALUES ('2020-12-01 00:00:00'), ('2021-01-01 00:00:00');
SELECT v FROM diff_order_date_part ORDER BY date_part('month', v);
DROP TABLE diff_order_date_part;
